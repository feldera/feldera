use dbsp::circuit::checkpointer::CheckpointMetadata;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};
use serde_json::{Error as JsonError, Value as JsonValue};
use std::{
    backtrace::Backtrace,
    cmp::Ordering,
    collections::HashMap,
    fmt::{Display, Formatter, Result as FmtResult},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Error as IoError, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::mpsc::{channel, Receiver, Sender},
    thread::{self, JoinHandle},
};

use crate::{transport::Step, ControllerError};

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum StepError {
    /// I/O error.
    #[serde(serialize_with = "serialize_io_error")]
    IoError {
        path: PathBuf,
        io_error: IoError,
        backtrace: Backtrace,
    },

    ParseError {
        path: PathBuf,
        #[serde(serialize_with = "serialize_json_error")]
        error: JsonError,
        line_number: usize,
        offset: u64,
    },

    MissingStep {
        path: PathBuf,
        step: Step,
    },

    UnexpectedRead,
    UnexpectedWrite,
    UnexpectedWait,
}

fn serialize_io_error<S>(
    path: &PathBuf,
    io_error: &IoError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("IoError", 4)?;
    ser.serialize_field("path", path)?;
    ser.serialize_field("kind", &io_error.kind().to_string())?;
    ser.serialize_field("os_error", &io_error.raw_os_error())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_json_error<S>(error: &JsonError, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&error.to_string())
}

impl Display for StepError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            StepError::ParseError { path, error, line_number, offset } => write!(f, "error parsing step on line {line_number} starting at offset {offset} in {} ({error})", path.display()),
            StepError::MissingStep { path, step } => write!(f, "{} should contain step {step} but it is not present", path.display()),
            StepError::IoError { path, io_error, .. } => write!(f, "I/O error on {}: {io_error}", path.display()),
            StepError::UnexpectedRead => write!(f, "Unexpected read while in write mode"),
            StepError::UnexpectedWrite => write!(f, "Unexpected write while in read mode"),
            StepError::UnexpectedWait => write!(f, "Unexpected wait while in read mode"),
        }
    }
}

impl StepError {
    fn io_error(path: &Path, io_error: IoError) -> StepError {
        StepError::IoError {
            path: path.to_path_buf(),
            io_error,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<StepError> for ControllerError {
    fn from(value: StepError) -> Self {
        ControllerError::StepError(value)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct Checkpoint {
    #[serde(flatten)]
    pub circuit: CheckpointMetadata,
    pub step: Step,
}

pub struct BackgroundSync {
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

/// Reads a `steps.json` file that tracks per-input adapter, per-step metadata.
pub struct StepReader {
    path: PathBuf,
    reader: BufReader<File>,
    line_number: usize,
}

pub enum ReadResult {
    Step {
        reader: StepReader,
        metadata: StepMetadata,
    },
    Writer(StepWriter),
}

impl StepReader {
    /// Opens the existing `steps.json` file at `path`.
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
            line_number: 0,
        })
    }

    /// Reads one step from this file. Returns either the step or, if end of
    /// file was reached, a [StepWriter] that can be used to append new steps.
    pub fn read(mut self) -> Result<ReadResult, StepError> {
        let start_offset = self.reader.stream_position().unwrap();
        self.line_number += 1;
        let mut line = String::new();
        if self
            .reader
            .read_line(&mut line)
            .map_err(|error| StepError::io_error(&self.path, error))?
            > 0
            && line.ends_with('\n')
        {
            let step = serde_json::from_str::<StepMetadata>(&line).map_err(|error| {
                StepError::ParseError {
                    path: self.path.clone(),
                    error,
                    line_number: self.line_number,
                    offset: start_offset,
                }
            })?;
            Ok(ReadResult::Step {
                reader: self,
                metadata: step,
            })
        } else {
            let mut file = self.reader.into_inner();
            file.set_len(start_offset)
                .and_then(|()| file.seek(SeekFrom::Start(start_offset)))
                .map_err(|io_error| StepError::io_error(&self.path, io_error))?;
            Ok(ReadResult::Writer(StepWriter::new(self.path, file)))
        }
    }

    /// Skips forward in this file to the given numbered `step`. If the step is
    /// found, returns it plus a reader for further steps.
    pub fn seek(mut self, step: Step) -> Result<(StepReader, StepMetadata), StepError> {
        loop {
            match self.read()? {
                ReadResult::Step { reader, metadata } => {
                    self = reader;
                    match metadata.step.cmp(&step) {
                        Ordering::Less => (),
                        Ordering::Equal => return Ok((self, metadata)),
                        Ordering::Greater => {
                            return Err(StepError::MissingStep {
                                path: self.path.clone(),
                                step,
                            })
                        }
                    }
                }
                ReadResult::Writer(writer) => {
                    return Err(StepError::MissingStep {
                        path: writer.path.clone(),
                        step,
                    })
                }
            }
        }
    }
}

/// A writer for a `steps.json` file that records per-input adapter, per-step metadata.
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

    fn new(path: PathBuf, file: File) -> Self {
        let sync = BackgroundSync::new(&file);
        let writer = BufWriter::new(file);
        Self { path, writer, sync }
    }

    /// Appends `step` to this writer and starts I/O for the write in the
    /// background.
    pub fn write(&mut self, step: &StepMetadata) -> Result<(), StepError> {
        serde_json::to_writer(&mut self.writer, step)
            .map_err(|error| IoError::from(error.io_error_kind().unwrap()))
            .and_then(|()| writeln!(self.writer))
            .and_then(|()| self.writer.flush())
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StepMetadata {
    pub step: Step,
    pub input_endpoints: HashMap<String, JsonValue>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use crate::controller::metadata::ReadResult;

    use super::{StepMetadata, StepReader, StepWriter};

    /// Create and write a steps file and then read it back.
    #[test]
    fn test_create() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.json");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                input_endpoints: HashMap::new(),
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
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.json");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                input_endpoints: HashMap::new(),
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
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.json");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                input_endpoints: HashMap::new(),
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
            let (mut step_reader, metadata) = step_reader.seek(start).unwrap();
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
