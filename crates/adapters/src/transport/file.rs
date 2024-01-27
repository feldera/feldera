use super::{
    InputConsumer, InputEndpoint, InputReader, InputTransport, OutputEndpoint, OutputTransport,
    Step,
};
use crate::{OutputEndpointConfig, PipelineState};
use anyhow::{bail, Error as AnyError, Result as AnyResult};
use crossbeam::sync::{Parker, Unparker};
use num_traits::FromPrimitive;
use pipeline_types::transport::file::{FileInputConfig, FileOutputConfig};
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    fs::File,
    io::{BufRead, BufReader, Write},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    thread::{sleep, spawn},
    time::Duration,
};

const SLEEP_MS: u64 = 200;

/// [`InputTransport`] implementation that reads data from a file.
///
/// The input transport factory gives this transport the name `file`.
pub struct FileInputTransport;

impl InputTransport for FileInputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("file")
    }

    /// Creates a new [`InputEndpoint`] for reading a file, interpreting
    /// `config` as a [`FileInputConfig`].
    ///
    /// See [`InputTransport::new_endpoint()`] for more information.
    fn new_endpoint(&self, config: &YamlValue) -> AnyResult<Box<dyn InputEndpoint>> {
        let ep = FileInputEndpoint {
            config: FileInputConfig::deserialize(config)?,
        };
        Ok(Box::new(ep))
    }
}

struct FileInputEndpoint {
    config: FileInputConfig,
}

impl InputEndpoint for FileInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        _start_step: Step,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(FileInputReader::new(&self.config, consumer)?))
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

struct FileInputReader {
    status: Arc<AtomicU32>,
    unparker: Option<Unparker>,
}

impl FileInputReader {
    fn new(config: &FileInputConfig, consumer: Box<dyn InputConsumer>) -> AnyResult<Self> {
        let file = File::open(&config.path).map_err(|e| {
            AnyError::msg(format!("Failed to open input file '{}': {e}", config.path))
        })?;
        let reader = match config.buffer_size_bytes {
            Some(buffer_size) if buffer_size > 0 => BufReader::with_capacity(buffer_size, file),
            _ => BufReader::new(file),
        };

        let parker = Parker::new();
        let unparker = Some(parker.unparker().clone());
        let status = Arc::new(AtomicU32::new(PipelineState::Paused as u32));
        let status_clone = status.clone();
        let follow = config.follow;
        let _worker =
            spawn(move || Self::worker_thread(reader, consumer, parker, status_clone, follow));

        Ok(Self { status, unparker })
    }

    fn unpark(&self) {
        if let Some(unparker) = &self.unparker {
            unparker.unpark();
        }
    }

    fn worker_thread(
        mut reader: BufReader<File>,
        mut consumer: Box<dyn InputConsumer>,
        parker: Parker,
        status: Arc<AtomicU32>,
        follow: bool,
    ) {
        loop {
            match PipelineState::from_u32(status.load(Ordering::Acquire)) {
                Some(PipelineState::Paused) => parker.park(),
                Some(PipelineState::Running) => {
                    let data = reader.fill_buf();
                    match data {
                        Err(e) => {
                            consumer.error(true, AnyError::from(e));
                            return;
                        }
                        Ok(data) if data.is_empty() => {
                            if !follow {
                                let _ = consumer.eoi();
                                return;
                            } else {
                                sleep(Duration::from_millis(SLEEP_MS));
                            }
                        }
                        Ok(data) => {
                            // println!("read {} bytes from file", data.len());

                            // Leave it to the controller to handle errors.  There is noone we can
                            // forward the error to upstream.
                            let _ = consumer.input_fragment(data);
                            let len = data.len();
                            reader.consume(len);
                        }
                    }
                }
                Some(PipelineState::Terminated) => return,
                _ => unreachable!(),
            }
        }
    }
}

impl InputReader for FileInputReader {
    fn pause(&self) -> AnyResult<()> {
        // Notify worker thread via the status flag.  The worker may
        // send another buffer downstream before the flag takes effect.
        self.status
            .store(PipelineState::Paused as u32, Ordering::Release);
        Ok(())
    }

    fn start(&self, _step: Step) -> AnyResult<()> {
        self.status
            .store(PipelineState::Running as u32, Ordering::Release);

        // Wake up the worker if it's paused.
        self.unpark();
        Ok(())
    }

    fn disconnect(&self) {
        self.status
            .store(PipelineState::Terminated as u32, Ordering::Release);

        // Wake up the worker if it's paused.
        self.unpark();
    }
}

impl Drop for FileInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

/// [`OutputTransport`] implementation that writes data to a file.
///
/// The output transport factory gives this transport the name `file`.
pub struct FileOutputTransport;

impl OutputTransport for FileOutputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("file")
    }

    /// Creates a new [`OutputEndpoint`] for writing a file, interpreting
    /// `config` as a [`FileOutputConfig`].
    ///
    /// See [`OutputTransport::new_endpoint()`] for more information.
    fn new_endpoint(&self, config: &OutputEndpointConfig) -> AnyResult<Box<dyn OutputEndpoint>> {
        let config = FileOutputConfig::deserialize(&config.connector_config.transport.config)?;
        let ep = FileOutputEndpoint::new(config)?;

        Ok(Box::new(ep))
    }
}

struct FileOutputEndpoint {
    file: File,
}

impl FileOutputEndpoint {
    fn new(config: FileOutputConfig) -> AnyResult<Self> {
        let file = File::create(&config.path).map_err(|e| {
            AnyError::msg(format!(
                "Failed to create output file '{}': {e}",
                config.path
            ))
        })?;
        Ok(Self { file })
    }
}

impl OutputEndpoint for FileOutputEndpoint {
    fn connect(
        &mut self,
        _async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
    ) -> AnyResult<()> {
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        self.file.write_all(buffer)?;
        Ok(())
    }

    fn push_key(&mut self, _key: &[u8], _val: &[u8]) -> AnyResult<()> {
        bail!(
            "File output transport does not support key-value pairs. \
This output endpoint was configured with a data format that produces outputs as key-value pairs; \
however the File transport does not support this representation."
        );
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::{
        deserialize_without_context,
        test::{mock_input_pipeline, wait, DEFAULT_TIMEOUT_MS},
    };
    use csv::WriterBuilder as CsvWriterBuilder;
    use serde::{Deserialize, Serialize};
    use std::{io::Write, thread::sleep, time::Duration};
    use tempfile::NamedTempFile;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
    struct TestStruct {
        s: String,
        b: bool,
        i: i64,
    }

    deserialize_without_context!(TestStruct);

    impl TestStruct {
        fn new(s: String, b: bool, i: i64) -> Self {
            Self { s, b, i }
        }
    }

    #[test]
    fn test_csv_file_nofollow() {
        let test_data = vec![
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];
        let temp_file = NamedTempFile::new().unwrap();

        // Create a transport endpoint attached to the file.
        // Use a very small buffer size for testing.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: file
    config:
        path: {:?}
        buffer_size_bytes: 5
format:
    name: csv
"#,
            temp_file.path().to_str().unwrap()
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file.as_file());
        for val in test_data.iter().cloned() {
            writer.serialize(val).unwrap();
        }
        writer.flush().unwrap();

        let (endpoint, consumer, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
        )
        .unwrap();

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(consumer.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        endpoint.start(0).unwrap();
        wait(
            || zset.state().flushed.len() == test_data.len(),
            DEFAULT_TIMEOUT_MS,
        );
        for (i, upd) in zset.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
    }

    #[test]
    fn test_csv_file_follow() {
        let test_data = vec![
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];
        let temp_file = NamedTempFile::new().unwrap();

        // Create a transport endpoint attached to the file.
        // Use a very small buffer size for testing.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: file
    config:
        path: {:?}
        buffer_size_bytes: 5
        follow: true
format:
    name: csv
"#,
            temp_file.path().to_str().unwrap()
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file.as_file());

        let (endpoint, consumer, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
        )
        .unwrap();

        for _ in 0..10 {
            for val in test_data.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();

            sleep(Duration::from_millis(10));

            // No outputs should be produced at this point.
            assert!(consumer.state().data.is_empty());
            assert!(!consumer.state().eoi);

            // Unpause the endpoint, wait for the data to appear at the output.
            endpoint.start(0).unwrap();
            wait(
                || zset.state().flushed.len() == test_data.len(),
                DEFAULT_TIMEOUT_MS,
            );
            for (i, upd) in zset.state().flushed.iter().enumerate() {
                assert_eq!(upd.unwrap_insert(), &test_data[i]);
            }
            endpoint.pause().unwrap();

            consumer.reset();
            zset.reset();
        }

        drop(writer);

        consumer.on_error(Some(Box::new(|_, _| {})));
        temp_file.as_file().write_all(b"xxx\n").unwrap();
        temp_file.as_file().flush().unwrap();

        endpoint.start(0).unwrap();
        wait(
            || {
                let state = consumer.state();
                // println!("result: {:?}", state.parser_result);
                state.parser_result.is_some() && !state.parser_result.as_ref().unwrap().1.is_empty()
            },
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();

        assert!(zset.state().buffered.is_empty());
        assert!(zset.state().flushed.is_empty());

        endpoint.disconnect();
    }
}
