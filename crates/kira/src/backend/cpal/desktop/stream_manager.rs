mod send_on_drop;

use std::{
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	time::Duration,
};

use super::renderer_with_cpu_usage::RendererWithCpuUsage;
use cpal::{
	BufferSize, Device, Stream, StreamConfig,
	traits::{DeviceTrait, HostTrait, StreamTrait},
};
use rtrb::{Consumer, Producer, PushError, RingBuffer};
use send_on_drop::SendOnDrop;

use super::super::Error;

const CHECK_STREAM_INTERVAL: Duration = Duration::from_millis(500);

#[allow(clippy::large_enum_variant)]
enum State {
	Empty,
	Idle {
		renderer: RendererWithCpuUsage,
	},
	Recovering {
		renderer_consumer: Consumer<RendererWithCpuUsage>,
	},
	Running {
		stream: Stream,
		renderer_consumer: Consumer<RendererWithCpuUsage>,
	},
}

pub(super) struct StreamManagerController {
	should_drop: Arc<AtomicBool>,
	num_stream_errors_discarded: Arc<AtomicU64>,
	handled_stream_error_consumer: Mutex<Consumer<cpal::Error>>,
}

impl StreamManagerController {
	pub fn stop(&self) {
		self.should_drop.store(true, Ordering::SeqCst);
	}

	#[must_use]
	pub fn num_stream_errors_discarded(&self) -> u64 {
		self.num_stream_errors_discarded.load(Ordering::Acquire)
	}

	pub fn pop_handled_error(&mut self) -> Option<cpal::Error> {
		self.handled_stream_error_consumer
			.get_mut()
			.unwrap()
			.pop()
			.ok()
	}
}

/// Starts a cpal stream and restarts it if needed
/// in the case of device changes or disconnections.
pub(super) struct StreamManager {
	state: State,
	device_id: String,
	sample_rate: u32,
	custom_device: bool,
	buffer_size: BufferSize,
}

impl StreamManager {
	pub fn start(
		renderer: RendererWithCpuUsage,
		device: Device,
		mut config: StreamConfig,
		custom_device: bool,
		buffer_size: BufferSize,
	) -> Result<StreamManagerController, Error> {
		let should_drop = Arc::new(AtomicBool::new(false));
		let should_drop_clone = should_drop.clone();

		let num_stream_errors_discarded = Arc::new(AtomicU64::new(0));
		let num_stream_errors_discarded_clone = num_stream_errors_discarded.clone();

		let (mut handled_stream_error_producer, handled_stream_error_consumer) =
			RingBuffer::new(64);

		let (mut initial_result_producer, mut initial_result_consumer) = RingBuffer::new(1);

		std::thread::spawn(move || {
			let mut stream_manager = StreamManager {
				state: State::Idle { renderer },
				device_id: device_id(&device),
				sample_rate: config.sample_rate,
				custom_device,
				buffer_size,
			};
			let mut unhandled_stream_error_consumer = match stream_manager.start_stream(
				&device,
				&mut config,
				num_stream_errors_discarded.clone(),
			) {
				Ok(unhandled_stream_error_consumer) => {
					initial_result_producer.push(Ok(())).unwrap();
					unhandled_stream_error_consumer
				}
				Err(err) => {
					initial_result_producer.push(Err(err)).unwrap();
					return;
				}
			};
			loop {
				std::thread::sleep(CHECK_STREAM_INTERVAL);
				if should_drop.load(Ordering::SeqCst) {
					break;
				}
				stream_manager.check_stream(
					&mut unhandled_stream_error_consumer,
					&mut handled_stream_error_producer,
					&num_stream_errors_discarded,
				);
			}
		});

		loop {
			if let Ok(result) = initial_result_consumer.pop() {
				result?;
				break;
			}
			std::thread::sleep(Duration::from_micros(100));
		}

		Ok(StreamManagerController {
			should_drop: should_drop_clone,
			num_stream_errors_discarded: num_stream_errors_discarded_clone,
			handled_stream_error_consumer: Mutex::new(handled_stream_error_consumer),
		})
	}

	/// Restarts the stream if the audio device gets disconnected.
	fn check_stream(
		&mut self,
		unhandled_stream_error_consumer: &mut Consumer<cpal::Error>,
		handled_stream_error_producer: &mut Producer<cpal::Error>,
		num_stream_errors_discarded: &Arc<AtomicU64>,
	) {
		let mut report = |error| {
			if handled_stream_error_producer.push(error).is_err() {
				num_stream_errors_discarded.fetch_add(1, Ordering::AcqRel);
			}
		};
		let mut disconnected = false;
		while let Ok(error) = unhandled_stream_error_consumer.pop() {
			disconnected |= matches!(
				error.kind(),
				cpal::ErrorKind::DeviceNotAvailable | cpal::ErrorKind::StreamInvalidated
			);
			report(error);
		}
		if disconnected && matches!(self.state, State::Running { .. }) {
			self.stop_stream();
		}

		// Avoid querying a healthy custom device or interrupting playback on macOS (#38).
		if matches!(self.state, State::Running { .. })
			&& (self.custom_device || cfg!(target_os = "macos"))
		{
			return;
		}
		let Ok((device, mut config)) = default_device_and_config() else {
			return;
		};
		if matches!(self.state, State::Running { .. }) {
			if device_id(&device) == self.device_id && config.sample_rate == self.sample_rate {
				return;
			}
			self.stop_stream();
		}
		if let State::Recovering { renderer_consumer } = &mut self.state {
			let Ok(renderer) = renderer_consumer.pop() else {
				return;
			};
			self.state = State::Idle { renderer };
		}
		match self.start_stream(&device, &mut config, num_stream_errors_discarded.clone()) {
			Ok(consumer) => *unhandled_stream_error_consumer = consumer,
			Err(error) => report(error),
		}
	}

	fn start_stream(
		&mut self,
		device: &Device,
		config: &mut StreamConfig,
		num_stream_errors_discarded: Arc<AtomicU64>,
	) -> Result<Consumer<cpal::Error>, cpal::Error> {
		let mut renderer =
			if let State::Idle { renderer } = std::mem::replace(&mut self.state, State::Empty) {
				renderer
			} else {
				panic!("trying to start a stream when the stream manager is not idle");
			};
		config.buffer_size = self.buffer_size; // this won't change anything if the buffer size is BufferSize::Default
		let device_id = device_id(device);
		let sample_rate = config.sample_rate;
		if sample_rate != self.sample_rate {
			renderer.on_change_sample_rate(sample_rate);
		}
		self.device_id = device_id;
		self.sample_rate = sample_rate;
		let (mut renderer_wrapper, renderer_consumer) = SendOnDrop::new(renderer);
		let (mut unhandled_stream_error_producer, unhandled_stream_error_consumer) =
			RingBuffer::new(64);
		let channels = config.channels;
		let stream = device
			.build_output_stream(
				*config,
				move |data: &mut [f32], _| {
					#[cfg(feature = "assert_no_alloc")]
					assert_no_alloc::assert_no_alloc(|| {
						process_renderer(&mut renderer_wrapper, data, channels, sample_rate);
					});
					#[cfg(not(feature = "assert_no_alloc"))]
					process_renderer(&mut renderer_wrapper, data, channels, sample_rate);
				},
				move |error| match unhandled_stream_error_producer.push(error) {
					Ok(()) => {}
					Err(PushError::Full(_stream_error)) => {
						num_stream_errors_discarded.fetch_add(1, Ordering::AcqRel);
					}
				},
				None,
			)
			.and_then(|stream| {
				stream.play()?;
				Ok(stream)
			});
		let stream = match stream {
			Ok(stream) => stream,
			Err(error) => {
				// The failed stream owns the callback. Keep its return channel so the
				// next check can reuse the mixer once the callback has been dropped.
				self.state = State::Recovering { renderer_consumer };
				return Err(error);
			}
		};
		self.state = State::Running {
			stream,
			renderer_consumer,
		};
		Ok(unhandled_stream_error_consumer)
	}

	fn stop_stream(&mut self) {
		if let State::Running {
			renderer_consumer,
			stream,
			..
		} = std::mem::replace(&mut self.state, State::Empty)
		{
			drop(stream);
			self.state = State::Recovering { renderer_consumer };
		} else {
			panic!("Trying to stop the stream when it's not running")
		}
	}
}

fn default_device_and_config() -> Result<(Device, StreamConfig), Error> {
	let host = cpal::default_host();
	let device = host
		.default_output_device()
		.ok_or(Error::NoDefaultOutputDevice)?;
	let config = device.default_output_config()?.config();
	Ok((device, config))
}

fn device_id(device: &Device) -> String {
	match device.id() {
		Ok(id) => format!("{}", id),
		Err(_) => "device id unavailable".to_string(),
	}
}

fn process_renderer(
	renderer: &mut SendOnDrop<RendererWithCpuUsage>,
	data: &mut [f32],
	channels: u16,
	sample_rate: u32,
) {
	renderer.on_start_processing();
	renderer.process(data, channels, sample_rate);
}
