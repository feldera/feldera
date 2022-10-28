use super::{InputConsumer, InputEndpoint, InputTransport};
use crate::PipelineState;
use anyhow::{Error as AnyError, Result as AnyResult};
use crossbeam::sync::{Parker, Unparker};
use num_traits::FromPrimitive;
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    fs::File,
    io::{BufRead, BufReader},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    thread::{sleep, spawn},
    time::Duration,
};

const SLEEP_MS: u64 = 200;

/// `InputTransport` implementation that reads data from file.
pub struct FileInputTransport;

impl InputTransport for FileInputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("file")
    }

    fn new_endpoint(
        &self,
        config: &YamlValue,
        consumer: Box<dyn InputConsumer>,
    ) -> AnyResult<Box<dyn InputEndpoint>> {
        let config = FileInputConfig::deserialize(config)?;
        let mut ep = FileInputEndpoint::new(config);
        ep.connect(consumer)?;
        Ok(Box::new(ep))
    }
}

#[derive(Deserialize)]
struct FileInputConfig {
    /// File path.
    path: String,

    /// Read buffer size.
    ///
    /// Default: when this parameter is not specified, a platform-specific
    /// default is used.
    buffer_size: Option<usize>,

    /// Enable file following.
    ///
    /// When `false`, the endpoint outputs an [`eof`](`InputConsumer::eof`)
    /// message and stops upon reaching the end of file.  When `true`, the
    /// endpoint will keep watching the file and outputting any new content
    /// appended to it.
    #[serde(default)]
    follow: bool,
}

struct FileInputEndpoint {
    config: FileInputConfig,
    status: Arc<AtomicU32>,
    unparker: Option<Unparker>,
}

impl FileInputEndpoint {
    fn new(config: FileInputConfig) -> Self {
        Self {
            config,
            status: Arc::new(AtomicU32::new(PipelineState::Paused as u32)),
            unparker: None,
        }
    }

    fn connect(&mut self, consumer: Box<dyn InputConsumer>) -> AnyResult<()> {
        let file = File::open(&self.config.path)?;
        let reader = match self.config.buffer_size {
            Some(buffer_size) if buffer_size > 0 => BufReader::with_capacity(buffer_size, file),
            _ => BufReader::new(file),
        };

        let parker = Parker::new();
        self.unparker = Some(parker.unparker().clone());
        let status = self.status.clone();
        let follow = self.config.follow;
        let _worker = spawn(move || Self::worker_thread(reader, consumer, parker, status, follow));
        Ok(())
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
                            consumer.error(AnyError::from(e));
                            return;
                        }
                        Ok(data) if data.is_empty() => {
                            if !follow {
                                consumer.eof();
                                return;
                            } else {
                                sleep(Duration::from_millis(SLEEP_MS));
                            }
                        }
                        Ok(data) => {
                            consumer.input(data);
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

impl InputEndpoint for FileInputEndpoint {
    fn pause(&self) -> AnyResult<()> {
        // Notify worker thread via the status flag.  The worker may
        // send another buffer downstream before the flag takes effect.
        self.status
            .store(PipelineState::Paused as u32, Ordering::Release);
        Ok(())
    }

    fn start(&self) -> AnyResult<()> {
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

impl Drop for FileInputEndpoint {
    fn drop(&mut self) {
        self.disconnect();
    }
}

#[cfg(test)]
mod test {
    use crate::test::{mock_input_pipeline, wait};
    use csv::WriterBuilder as CsvWriterBuilder;
    use serde::{Deserialize, Serialize};
    use serde_yaml;
    use std::{io::Write, thread::sleep, time::Duration};
    use tempfile::NamedTempFile;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
    struct TestStruct {
        s: String,
        b: bool,
        i: i64,
    }

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
transport:
    name: file
    config:
        path: {:?}
        buffer_size: 5
format:
    name: csv
    config:
        input_stream: test_input
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

        let (endpoint, consumer, zset) = mock_input_pipeline::<TestStruct>(
            "test_input",
            serde_yaml::from_str(&config_str).unwrap(),
        );

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(consumer.state().data.is_empty());
        assert!(!consumer.state().eof);

        // Unpause the endpoint, wait for the data to appear at the output.
        endpoint.start().unwrap();
        wait(|| zset.state().flushed.len() == test_data.len(), None);
        for (i, (val, polarity)) in zset.state().flushed.iter().enumerate() {
            assert_eq!(*polarity, true);
            assert_eq!(val, &test_data[i]);
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
transport:
    name: file
    config:
        path: {:?}
        buffer_size: 5
        follow: true
format:
    name: csv
    config:
        input_stream: test_input
"#,
            temp_file.path().to_str().unwrap()
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file.as_file());

        let (endpoint, consumer, zset) = mock_input_pipeline::<TestStruct>(
            "test_input",
            serde_yaml::from_str(&config_str).unwrap(),
        );

        for _ in 0..10 {
            for val in test_data.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();

            sleep(Duration::from_millis(10));

            // No outputs should be produced at this point.
            assert!(consumer.state().data.is_empty());
            assert!(!consumer.state().eof);

            // Unpause the endpoint, wait for the data to appear at the output.
            endpoint.start().unwrap();
            wait(|| zset.state().flushed.len() == test_data.len(), None);
            for (i, (val, polarity)) in zset.state().flushed.iter().enumerate() {
                assert_eq!(*polarity, true);
                assert_eq!(val, &test_data[i]);
            }
            endpoint.pause().unwrap();

            consumer.reset();
            zset.reset();
        }

        drop(writer);

        consumer.on_error(Some(Box::new(|_| {})));
        temp_file.as_file().write(b"xxx\n").unwrap();
        temp_file.as_file().flush().unwrap();

        endpoint.start().unwrap();
        wait(
            || {
                let state = consumer.state();
                // println!("result: {:?}", state.parser_result);
                state.parser_result.is_some() && state.parser_result.as_ref().unwrap().is_err()
            },
            None,
        );

        assert!(zset.state().buffered.is_empty());
        assert!(zset.state().flushed.is_empty());

        endpoint.disconnect();
    }
}
