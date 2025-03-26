use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Error as IoError, ErrorKind, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::mpsc::{channel, Receiver, Sender},
    thread::{self, JoinHandle},
};

use feldera_adapterlib::{errors::journal::StepError, transport::Step};
use feldera_types::config::InputEndpointConfig;
use rmpv::Value as RmpValue;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::util::write_file_atomically;

struct BackgroundSync {
    join_handle: Option<JoinHandle<()>>,
    request_sender: Option<Sender<()>>,
    reply_receiver: Receiver<Result<(), IoError>>,
    n_incomplete_requests: usize,
}

impl BackgroundSync {
    pub fn new(file: &File) -> Self {
        let file = file.try_clone().unwrap();
        let (request_sender, request_receiver) = channel();
        let (reply_sender, reply_receiver) = channel();
        let join_handle = thread::Builder::new()
            .name("dbsp-step-sync".into())
            .spawn({
                move || {
                    for () in request_receiver {
                        let _ = reply_sender.send(file.sync_data());
                    }
                }
            })
            .unwrap();
        Self {
            join_handle: Some(join_handle),
            request_sender: Some(request_sender),
            reply_receiver,
            n_incomplete_requests: 0,
        }
    }

    pub fn sync(&mut self) {
        let _ = self.request_sender.as_mut().unwrap().send(());
        self.n_incomplete_requests += 1;
    }

    pub fn wait(&mut self) -> Result<(), IoError> {
        while self.n_incomplete_requests > 0 {
            self.reply_receiver.recv().unwrap()?;
            self.n_incomplete_requests -= 1;
        }
        Ok(())
    }
}

impl Drop for BackgroundSync {
    fn drop(&mut self) {
        let _ = self.request_sender.take();
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

/// Reads a `steps.bin` file that tracks per-input adapter, per-step metadata.
pub struct StepReader {
    /// File name.
    path: PathBuf,

    /// Underlying file.
    reader: BufReader<File>,

    /// `None` if we haven't yet read a step; otherwise, the expected step
    /// number for the next step to be read.
    next_step: Option<Step>,
}

/// Return value of [StepReader::read] and [StepReader::seek].
#[allow(clippy::large_enum_variant)]
pub enum ReadResult {
    /// Step that was successfully read.
    Step {
        /// [StepReader] for reading the next step.
        reader: StepReader,

        /// Metadata that was read.
        metadata: StepMetadata,
    },

    /// End-of-file was reached, which turns the [StepReader] into a [StepWriter].
    Writer(
        /// [StepWriter] for writing more steps.
        StepWriter,
    ),
}

impl StepReader {
    /// Opens the existing `steps.bin` file at `path`.
    pub fn open<P>(path: P) -> Result<Self, StepError>
    where
        P: AsRef<Path>,
    {
        let path = PathBuf::from(path.as_ref());
        let reader = BufReader::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|io_error| StepError::io_error(&path, io_error))?,
        );
        Ok(Self {
            path,
            reader,
            next_step: None,
        })
    }

    /// Reads one step from this file. Returns either the step or, if end of
    /// file was reached, a [StepWriter] that can be used to append new steps.
    pub fn read(mut self) -> Result<ReadResult, StepError> {
        let start_offset = self.reader.stream_position().unwrap();
        match rmp_serde::decode::from_read::<_, StepMetadata>(&mut self.reader) {
            Ok(step) => {
                match self.next_step {
                    Some(next) if next != step.step => {
                        return Err(StepError::MissingStep {
                            path: self.path,
                            step: next,
                        })
                    }
                    _ => self.next_step = Some(step.step + 1),
                }
                Ok(ReadResult::Step {
                    reader: self,
                    metadata: step,
                })
            }
            Err(error) => {
                match error {
                    rmp_serde::decode::Error::InvalidMarkerRead(e)
                    | rmp_serde::decode::Error::InvalidDataRead(e)
                        if e.kind() == ErrorKind::UnexpectedEof =>
                    {
                        // End of file.
                    }
                    _ => {
                        return Err(StepError::DecodeError {
                            path: self.path.clone(),
                            error,
                            offset: start_offset,
                        })
                    }
                };
                let mut file = self.reader.into_inner();
                file.set_len(start_offset)
                    .and_then(|()| file.seek(SeekFrom::Start(start_offset)))
                    .map_err(|io_error| StepError::io_error(&self.path, io_error))?;
                Ok(ReadResult::Writer(StepWriter::new(self.path, file)))
            }
        }
    }

    /// Skips forward in this file to the given numbered `step`:
    ///
    /// - If the step is found, returns it plus a reader for further steps, as
    ///   [ReadResult::Step].
    ///
    /// - If the file is empty or contains only steps before `step`, returns a
    ///   writer, as [ReadResult::Writer].
    ///
    /// - If the file is invalid (for example, it contains nonconsecutive steps
    ///   or only steps after `step`) or cannot be read, returns an error.
    pub fn seek(mut self, step: Step) -> Result<ReadResult, StepError> {
        loop {
            let next_step = self.next_step;
            match self.read()? {
                ReadResult::Step { reader, metadata } => {
                    self = reader;
                    match metadata.step.cmp(&step) {
                        Ordering::Less => (),
                        Ordering::Equal => {
                            return Ok(ReadResult::Step {
                                reader: self,
                                metadata,
                            })
                        }
                        Ordering::Greater => {
                            return Err(StepError::MissingStep {
                                path: self.path.clone(),
                                step,
                            })
                        }
                    }
                }
                ReadResult::Writer(writer) => {
                    if next_step.is_none_or(|next| next == step) {
                        return Ok(ReadResult::Writer(writer));
                    } else {
                        return Err(StepError::MissingStep {
                            path: writer.path.clone(),
                            step,
                        });
                    }
                }
            }
        }
    }
}

/// A writer for a `steps.bin` file that records per-input adapter, per-step metadata.
pub struct StepWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    sync: BackgroundSync,
}

impl StepWriter {
    /// Creates a new writer for `path`. Yields an error if `path` already
    /// exists.
    pub fn create<P>(path: P) -> Result<Self, StepError>
    where
        P: AsRef<Path>,
    {
        let path = PathBuf::from(path.as_ref());
        let file =
            File::create_new(&path).map_err(|io_error| StepError::io_error(&path, io_error))?;
        Ok(Self::new(path, file))
    }

    /// Creates a new writer for `path` to append to an existing file.
    ///
    /// This should not be used in the general case (and thus it is not `pub`),
    /// because we cannot be sure that the file does not have trailing garbage
    /// in it from a previous write that crashed midway.  But it is safe for
    /// `StepRw::truncate` because in that case we know exactly what is in the
    /// file.
    fn append<P>(path: P) -> Result<Self, StepError>
    where
        P: AsRef<Path>,
    {
        let path = PathBuf::from(path.as_ref());
        let file = File::options()
            .append(true)
            .open(&path)
            .map_err(|io_error| StepError::io_error(&path, io_error))?;
        Ok(Self::new(path, file))
    }

    fn new(path: PathBuf, file: File) -> Self {
        let sync = BackgroundSync::new(&file);
        let writer = BufWriter::new(file);
        Self { path, writer, sync }
    }

    /// Appends `step` to this writer and starts I/O for the write in the
    /// background.
    pub fn write(&mut self, step: &StepMetadata) -> Result<(), StepError> {
        rmp_serde::encode::write_named(&mut self.writer, step).map_err(|error| {
            StepError::EncodeError {
                path: self.path.to_path_buf(),
                error,
            }
        })?;
        self.writer
            .flush()
            .map_err(|error| StepError::io_error(&self.path, error))?;
        self.sync.sync();
        Ok(())
    }

    /// Blocks until all previous writes have been committed to disk.
    pub fn wait(&mut self) -> Result<(), StepError> {
        self.sync
            .wait()
            .map_err(|error| StepError::io_error(&self.path, error))
    }
}

/// A step reader or writer.
///
/// A step reader turns into a writer once we've read all of the entries, so it
/// makes sense to encapsulate them.
pub enum StepRw {
    Reader(StepReader),
    Writer(StepWriter),
}

impl StepRw {
    /// Opens the existing `steps.bin` file at `path`.
    pub fn open<P>(path: P) -> Result<Self, StepError>
    where
        P: AsRef<Path>,
    {
        StepReader::open(path).map(Self::Reader)
    }

    /// Creates a new write at `path`, which must not exist.
    pub fn create<P>(path: P) -> Result<Self, StepError>
    where
        P: AsRef<Path>,
    {
        StepWriter::create(path).map(Self::Writer)
    }

    /// Returns the inner [StepReader], or `None` if this isn't a reader.
    pub fn into_reader(self) -> Option<StepReader> {
        match self {
            Self::Reader(step_reader) => Some(step_reader),
            Self::Writer(_) => None,
        }
    }

    /// Reads a step, if we're a reader, and returns it along with our
    /// replacement reader (or writer).
    pub fn read(self) -> Result<(Option<StepMetadata>, StepRw), StepError> {
        match self {
            Self::Reader(reader) => match reader.read()? {
                ReadResult::Step { reader, metadata } => Ok((Some(metadata), Self::Reader(reader))),
                ReadResult::Writer(writer) => Ok((None, Self::Writer(writer))),
            },
            Self::Writer(_) => Ok((None, self)),
        }
    }

    /// Returns a mutable reference to our inner writer, or `None` if we're a
    /// reader.
    pub fn as_writer(&mut self) -> Option<&mut StepWriter> {
        match self {
            Self::Reader(_) => None,
            Self::Writer(step_writer) => Some(step_writer),
        }
    }

    /// Returns the log's path.
    pub fn path(&self) -> &PathBuf {
        match self {
            Self::Reader(step_reader) => &step_reader.path,
            Self::Writer(step_writer) => &step_writer.path,
        }
    }

    /// Replaces this log by an empty one and returns it for further writing.
    pub fn truncate(self) -> Result<Self, StepError> {
        let path = self.path();
        write_file_atomically(path, &[]).map_err(|io_error| StepError::io_error(path, io_error))?;
        Ok(Self::Writer(StepWriter::append(path)?))
    }
}

/// A record in the journal, useful for replaying a step.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StepMetadata {
    /// Step number.
    pub step: Step,

    /// Names of input endpoints removed in the step.
    pub remove_inputs: HashSet<String>,

    /// Input endpoints added in the step, with their configurations.
    ///
    /// If a given name is in both `remove_inputs` and `add_inputs`, then the
    /// step replaced an endpoint with the given name by a new, otherwise
    /// unrelated endpoint.
    pub add_inputs: HashMap<String, InputEndpointConfig>,

    /// Logs for the endpoints included in the step.
    ///
    /// A given endpoint is included if it existed before the step and is not in
    /// `remove_inputs`, or if it is included in `add_inputs`.
    pub input_logs: HashMap<String, InputLog>,
}

/// A journal record for a single endpoint for a single step.
///
/// The endpoint's name is the key in [StepMetadata::input_logs].
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct InputLog {
    /// Data for replay.
    ///
    /// This is filled in by input adapters that log actual data records
    /// (e.g. the HTTP and ad hoc query input adapters). For the other adapters,
    /// which only log metadata (such as record offsets), this field is
    /// [RmpValue::Nil].
    pub data: RmpValue,

    /// Metadata for seek and replay.
    ///
    /// This is filled in by input adapters that log metadata (such as record
    /// offsets).
    pub metadata: JsonValue,

    /// Checksums of the input data.
    pub checksums: InputChecksums,
}

/// Input data statistics.
///
/// This allows checking that an input step replayed the same data as the
/// original run.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct InputChecksums {
    /// Number of records.
    pub num_records: u64,

    /// Hash of the records.
    pub hash: u64,
}

/// Checksums for the input endpoints in a step.
///
/// This is a subset of [StepMetadata] that is useful for verifying that an
/// input step replayed the same data as the original run.
#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct StepInputChecksums(
    /// Maps from an input endpoint name to its checksums.
    pub HashMap<String, InputChecksums>,
);

impl From<&HashMap<String, InputLog>> for StepInputChecksums {
    fn from(input_logs: &HashMap<String, InputLog>) -> Self {
        Self(
            input_logs
                .iter()
                .map(|(name, log)| (name.clone(), log.checksums.clone()))
                .collect(),
        )
    }
}

impl From<&StepMetadata> for StepInputChecksums {
    fn from(value: &StepMetadata) -> Self {
        Self::from(&value.input_logs)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use tempfile::TempDir;

    use crate::{controller::journal::ReadResult, test::init_test_logger};

    use super::{StepMetadata, StepReader, StepWriter};

    /// Create and write a steps file and then read it back.
    #[test]
    fn test_create() {
        init_test_logger();

        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.bin");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                remove_inputs: HashSet::new(),
                add_inputs: HashMap::new(),
                input_logs: HashMap::new(),
            })
            .collect::<Vec<_>>();

        let mut step_writer = StepWriter::create(&path).unwrap();
        for step in written_data.iter() {
            step_writer.write(step).unwrap();
            step_writer.wait().unwrap();
        }
        drop(step_writer);

        let mut step_reader = StepReader::open(&path).unwrap();
        let mut read_data = Vec::new();
        while let ReadResult::Step {
            reader: new_reader,
            metadata,
        } = step_reader.read().unwrap()
        {
            read_data.push(metadata);
            step_reader = new_reader;
        }
        assert_eq!(written_data, read_data);
    }

    /// Create and write a steps file, then read it back, and continue adding more steps at the end.
    #[test]
    fn test_append() {
        init_test_logger();

        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.bin");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                remove_inputs: HashSet::new(),
                add_inputs: HashMap::new(),
                input_logs: HashMap::new(),
            })
            .collect::<Vec<_>>();

        // Create an empty file and close it immediately.
        // (Thus, this test also checks that we can open and read an empty file.)
        StepWriter::create(&path).unwrap();

        for new_step in 0..10 {
            let mut step_reader = StepReader::open(&path).unwrap();
            let mut read_data = Vec::new();

            // Exactly `new_step` steps should be readable already.
            println!("read steps 0..{new_step}");
            for _ in 0..new_step {
                match step_reader.read().unwrap() {
                    ReadResult::Step {
                        reader: new_reader,
                        metadata,
                    } => {
                        step_reader = new_reader;
                        read_data.push(metadata);
                    }
                    ReadResult::Writer(_) => unreachable!(),
                }
            }
            assert_eq!(&written_data[..new_step], read_data);

            println!("write step {new_step}");
            let mut step_writer = match step_reader.read().unwrap() {
                ReadResult::Step { .. } => unreachable!(),
                ReadResult::Writer(writer) => writer,
            };
            step_writer.write(&written_data[new_step]).unwrap();
        }
    }

    /// Create and write a steps file and then read it back with seeking.
    #[test]
    fn test_seek() {
        init_test_logger();

        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.bin");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                remove_inputs: HashSet::new(),
                add_inputs: HashMap::new(),
                input_logs: HashMap::new(),
            })
            .collect::<Vec<_>>();

        let mut step_writer = StepWriter::create(&path).unwrap();
        for step in written_data.iter() {
            step_writer.write(step).unwrap();
            step_writer.wait().unwrap();
        }
        drop(step_writer);

        for start in 0..10 {
            println!("seek to {start}");
            let step_reader = StepReader::open(&path).unwrap();

            let mut read_data = Vec::new();
            let ReadResult::Step {
                reader: mut step_reader,
                metadata,
            } = step_reader.seek(start).unwrap()
            else {
                unreachable!()
            };
            read_data.push(metadata);
            while let ReadResult::Step {
                reader: new_reader,
                metadata,
            } = step_reader.read().unwrap()
            {
                read_data.push(metadata);
                step_reader = new_reader;
            }
            assert_eq!(&written_data[start as usize..], &read_data);
        }
    }
}
