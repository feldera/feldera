use super::{
    InputConsumer, InputEndpoint, InputReader, InputReaderCommand, OutputEndpoint,
    TransportInputEndpoint,
};
use crate::format::StreamSplitter;
use crate::{InputBuffer, Parser};
use anyhow::{bail, Error as AnyError, Result as AnyResult};
use feldera_adapterlib::transport::{parse_resume_info, Resume};
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use feldera_types::transport::file::{FileInputConfig, FileOutputConfig};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::hash::Hasher;
use std::io::{Seek, SeekFrom};
use std::ops::Range;
use std::path::PathBuf;
use std::thread::{self, Thread};
use std::{fs::File, io::Write, time::Duration};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info_span};
use url::Url;
use xxhash_rust::xxh3::Xxh3Default;

const SLEEP: Duration = Duration::from_millis(200);

#[cfg(test)]
use std::{collections::BTreeMap, sync::Mutex};

#[cfg(test)]
static BARRIERS: Mutex<BTreeMap<String, usize>> = Mutex::new(BTreeMap::new());

fn get_barrier(_name: &str) -> Option<usize> {
    #[cfg(test)]
    return BARRIERS.lock().unwrap().get(_name).copied();

    #[cfg(not(test))]
    return None;
}

#[cfg(test)]
pub fn set_barrier(name: &str, value: usize) {
    BARRIERS.lock().unwrap().insert(name.into(), value);
}

pub(crate) struct FileInputEndpoint {
    config: FileInputConfig,
}

impl FileInputEndpoint {
    pub(crate) fn new(config: FileInputConfig) -> Self {
        Self { config }
    }
}

impl InputEndpoint for FileInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        if get_barrier(&self.config.path).is_some() {
            Some(FtModel::AtLeastOnce)
        } else {
            Some(FtModel::ExactlyOnce)
        }
    }
}

impl TransportInputEndpoint for FileInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(FileInputReader::new(
            self,
            consumer,
            parser,
            resume_info,
        )?))
    }
}

pub struct FileInputReader {
    sender: UnboundedSender<InputReaderCommand>,
    thread: Thread,
}

fn parse_url(path: &str) -> Option<PathBuf> {
    let url = Url::parse(path).ok()?;
    if url.scheme() != "file" {
        return None;
    }
    url.to_file_path().ok()
}

impl FileInputReader {
    fn new(
        endpoint: &FileInputEndpoint,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Self> {
        let resume_info = if let Some(resume_info) = resume_info {
            Some(parse_resume_info::<Metadata>(&resume_info)?)
        } else {
            None
        };

        let config = &endpoint.config;
        let path = parse_url(&config.path).unwrap_or_else(|| PathBuf::from(&config.path));
        let file = File::open(&path).map_err(|e| {
            AnyError::msg(format!(
                "Failed to open input file '{}': {e}",
                path.display()
            ))
        })?;

        let (sender, receiver) = unbounded_channel();
        let join_handle = thread::Builder::new()
            .name("file-input".to_string())
            .spawn({
                let follow = config.follow;
                let path = config.path.clone();
                let buffer_size = match config.buffer_size_bytes {
                    Some(size) if size > 0 => size,
                    _ => 8192,
                };
                move || {
                    let _guard = info_span!("file_input", path).entered();
                    if let Err(error) = Self::worker_thread(
                        file,
                        path,
                        buffer_size,
                        consumer.as_ref(),
                        parser,
                        receiver,
                        follow,
                        resume_info,
                    ) {
                        consumer.error(true, error);
                    }
                }
            })
            .expect("failed to spawn file-input thread");

        Ok(Self {
            sender,
            thread: join_handle.thread().clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn worker_thread(
        mut file: File,
        path: String,
        buffer_size: usize,
        consumer: &dyn InputConsumer,
        mut parser: Box<dyn Parser>,
        mut receiver: UnboundedReceiver<InputReaderCommand>,
        follow: bool,
        resume_info: Option<Metadata>,
    ) -> AnyResult<()> {
        let mut splitter = StreamSplitter::new(parser.splitter());

        if let Some(resume_info) = resume_info {
            let offset = resume_info.offsets.end;
            file.seek(SeekFrom::Start(offset))?;
            splitter.seek(offset);
        }

        let mut queue = VecDeque::<(Range<u64>, Box<dyn InputBuffer>)>::new();
        let mut extending = false;
        let mut eof = false;
        let mut num_records = 0;
        loop {
            loop {
                let msg = receiver.try_recv();
                match msg {
                    Ok(InputReaderCommand::Extend) => {
                        extending = true;
                    }
                    Ok(InputReaderCommand::Pause) => {
                        extending = false;
                    }
                    Ok(InputReaderCommand::Queue { .. }) => {
                        let mut total = 0;
                        let mut hasher = consumer.hasher();
                        let limit = consumer.max_batch_size();
                        let mut range: Option<Range<u64>> = None;
                        while let Some((offsets, mut buffer)) = queue.pop_front() {
                            range = match range {
                                Some(range) => Some(range.start..offsets.end),
                                None => Some(offsets),
                            };
                            total += buffer.len();
                            if let Some(hasher) = hasher.as_mut() {
                                buffer.hash(hasher);
                            }
                            buffer.flush();
                            if total >= limit {
                                break;
                            }
                        }

                        let offsets = range.unwrap_or_else(|| {
                            let ofs = splitter.position();
                            ofs..ofs
                        });
                        let seek = serde_json::to_value(Metadata {
                            offsets: offsets.clone(),
                        })
                        .unwrap();

                        let resume = match get_barrier(&path) {
                            None => Resume::new_metadata_only(seek, hasher),
                            Some(barrier) => {
                                num_records += total;
                                if num_records < barrier {
                                    Resume::Barrier
                                } else {
                                    Resume::Seek { seek }
                                }
                            }
                        };

                        consumer.extended(total, Some(resume));
                    }
                    Ok(InputReaderCommand::Replay { metadata, .. }) => {
                        let Metadata { offsets } = serde_json_path_to_error::from_value(metadata)?;
                        file.seek(SeekFrom::Start(offsets.start))?;
                        splitter.seek(offsets.start);
                        let mut remainder = (offsets.end - offsets.start) as usize;
                        let mut num_records = 0;
                        let mut hasher = Xxh3Default::new();
                        while remainder > 0 {
                            let n = splitter.read(&mut file, buffer_size, remainder)?;
                            if n == 0 {
                                error!("file truncated since originally read");
                                break;
                            }
                            remainder -= n;
                            while let Some(chunk) = splitter.next(remainder == 0) {
                                let (mut buffer, errors) = parser.parse(chunk);
                                consumer.parse_errors(errors);
                                consumer.buffered(buffer.len(), chunk.len());
                                num_records += buffer.len();
                                buffer.hash(&mut hasher);
                                buffer.flush();
                            }
                        }
                        consumer.replayed(num_records, hasher.finish());
                    }
                    Ok(InputReaderCommand::Disconnect) => return Ok(()),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(()),
                }
            }

            if !extending || eof {
                thread::park();
                continue;
            }

            let n = splitter.read(&mut file, buffer_size, usize::MAX)?;
            if n == 0 {
                if follow {
                    thread::park_timeout(SLEEP);
                    continue;
                }
                eof = true;
            }
            loop {
                let start = splitter.position();
                let Some(chunk) = splitter.next(eof) else {
                    break;
                };
                let (buffer, errors) = parser.parse(chunk);
                let num_records = buffer.len();
                let num_bytes = chunk.len();
                consumer.parse_errors(errors);

                if let Some(buffer) = buffer {
                    let end = splitter.position();
                    queue.push_back((start..end, buffer));
                    consumer.buffered(num_records, num_bytes);
                }
            }
            if n == 0 && !follow {
                consumer.eoi();
            }
        }
    }
}

impl InputReader for FileInputReader {
    fn request(&self, command: super::InputReaderCommand) {
        let _ = self.sender.send(command);
        self.thread.unpark();
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl Drop for FileInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

pub(crate) struct FileOutputEndpoint {
    file: File,
}

impl FileOutputEndpoint {
    pub(crate) fn new(config: FileOutputConfig) -> AnyResult<Self> {
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
        self.file.sync_all()?;
        Ok(())
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
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
    use crate::test::{mock_input_pipeline, wait, DEFAULT_TIMEOUT_MS};
    use csv::WriterBuilder as CsvWriterBuilder;
    use feldera_types::deserialize_without_context;
    use feldera_types::program_schema::Relation;
    use serde::{Deserialize, Serialize};
    use std::{io::Write, thread::sleep, time::Duration};
    use tempfile::NamedTempFile;
    use url::Url;

    #[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
    pub struct TestStruct {
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
        let test_data = [
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
    name: file_input
    config:
        path: {:?}
        buffer_size_bytes: 5
format:
    name: csv
    config:
        delimiter: "|"
        headers: true
"#,
            temp_file.path().to_str().unwrap()
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(true)
            .delimiter(b'|')
            .from_writer(temp_file.as_file());
        for val in test_data.iter().cloned() {
            writer.serialize(val).unwrap();
        }
        writer.flush().unwrap();

        let (endpoint, consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        endpoint.extend();
        wait(
            || {
                endpoint.queue(false);
                zset.state().flushed.len() == test_data.len()
            },
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        for (i, upd) in zset.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
    }

    #[test]
    fn test_csv_file_follow() {
        let test_data = [
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];
        let temp_file = NamedTempFile::new().unwrap();
        let temp_file_url = Url::from_file_path(temp_file.path()).unwrap();

        // Create a transport endpoint attached to the file.
        // Use a very small buffer size for testing.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: file_input
    config:
        path: "{temp_file_url}"
        buffer_size_bytes: 5
        follow: true
format:
    name: csv
"#,
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file.as_file());

        let (endpoint, consumer, parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

        endpoint.extend();

        for _ in 0..10 {
            for val in test_data.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();

            sleep(Duration::from_millis(10));

            // No outputs should be produced at this point.
            assert!(!consumer.state().eoi);

            // Unpause the endpoint, wait for the data to appear at the output.
            wait(
                || {
                    endpoint.queue(false);
                    zset.state().flushed.len() == test_data.len()
                },
                DEFAULT_TIMEOUT_MS,
            )
            .unwrap();
            for (i, upd) in zset.state().flushed.iter().enumerate() {
                assert_eq!(upd.unwrap_insert(), &test_data[i]);
            }

            consumer.reset();
            zset.reset();
        }

        drop(writer);

        consumer.on_error(Some(Box::new(|_, _| {})));
        parser.on_error(Some(Box::new(|_, _| {})));
        temp_file.as_file().write_all(b"xxx\n").unwrap();
        temp_file.as_file().flush().unwrap();

        wait(
            || {
                endpoint.queue(false);
                let state = parser.state();
                // println!("result: {:?}", state.parser_result);
                state.parser_result.is_some() && !state.parser_result.as_ref().unwrap().is_empty()
            },
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();

        assert!(zset.state().flushed.is_empty());

        endpoint.disconnect();
    }
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    offsets: Range<u64>,
}
