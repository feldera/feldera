//! A reference, in-memory implementation for a state machine test to check a
//! backend.
//!
//! TODO: Currently only functional STM, should later be expanded to cover
//! error/corner cases.

use std::{
    cell::RefCell,
    collections::HashMap,
    fmt::{self, Debug},
    iter,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use proptest::prelude::*;
use proptest_state_machine::ReferenceStateMachine;

use crate::storage::{
    backend::{
        FileHandle, ImmutableFileHandle, StorageControl, StorageError, StorageRead, StorageWrite,
    },
    buffer_cache::FBuf,
};

/// Generates a "random" string to be written in a file.
///
/// The length is always a power of two in the range 2^9..=2^12.
fn pow2_string() -> BoxedStrategy<String> {
    (limited_pow2(), prop::char::range('a', 'z'))
        .prop_map(|(length, c)| iter::repeat(c).take(length as usize).collect::<String>())
        .boxed()
}

/// Generates a power of two in the range 2^9..=2^12.
fn limited_pow2() -> BoxedStrategy<u64> {
    prop_oneof![Just(512), Just(1024), Just(2048), Just(4096)].boxed()
}

/// The maximum number of transitions to be generated for each test case.
pub(crate) const MAX_TRANSITIONS: usize = 20;

/// How many files the test case try to read/write to.
const MAX_FILES: i64 = 10;

/// What the backend does, models the operations that the traits
/// [`StorageControl`], [`StorageWrite`], and [`StorageRead`]) provide.
#[derive(Clone)]
pub enum Transition {
    Create,
    DeleteMut(i64),
    Write(i64, u64, String),
    Complete(i64),
    Read(i64, u64, u64),
}

impl Debug for Transition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transition::Create => write!(f, "Create"),
            Transition::DeleteMut(id) => write!(f, "DeleteMut({})", id),
            Transition::Write(id, offset, content) => {
                write!(
                    f,
                    "Write({}, {}, {}*{})",
                    id,
                    offset,
                    content.get(0..1).unwrap_or(""),
                    content.len()
                )
            }
            Transition::Complete(id) => write!(f, "Complete({})", id),
            Transition::Read(id, offset, length) => {
                write!(f, "Read({}, {}, {})", id, offset, length)
            }
        }
    }
}

/// The in-memory backend that is used as a reference for the state machine
/// tests.
///
/// Note that we have two modes of operation:
/// - `ALLOW_OVERWRITE = true`: allows overwriting data in the file.
/// - `ALLOW_OVERWRITE = false`: does not allow overwriting data in the file
///   (returns an error on writes).
/// This ensures we can test the raw backend as well as the buffer cache (which
/// does not allow overwrites).
#[derive(Default, Debug)]
pub struct InMemoryBackend<const ALLOW_OVERWRITE: bool> {
    file_counter: AtomicI64,
    /// All files we can currently write to.
    pub(crate) files: RefCell<HashMap<i64, Vec<Option<u8>>>>,
    /// All files which have transitioned to read-only.
    pub(crate) immutable_files: RefCell<HashMap<i64, Vec<Option<u8>>>>,
    /// Last encountered error, gets cleared on every transition.
    pub(crate) error: Option<StorageError>,
}

impl<const ALLOW_OVERWRITE: bool> Clone for InMemoryBackend<ALLOW_OVERWRITE> {
    fn clone(&self) -> Self {
        Self {
            files: self.files.clone(),
            file_counter: AtomicI64::new(self.file_counter.load(Ordering::Relaxed)),
            immutable_files: self.immutable_files.clone(),
            error: Default::default(),
        }
    }
}

impl<const ALLOW_OVERWRITE: bool> StorageControl for InMemoryBackend<ALLOW_OVERWRITE> {
    async fn create_named<P: AsRef<Path>>(&self, _name: P) -> Result<FileHandle, StorageError> {
        let file_counter = self.file_counter.fetch_add(1, Ordering::Relaxed);
        self.files.borrow_mut().insert(file_counter, Vec::new());
        Ok(FileHandle(file_counter))
    }

    async fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.immutable_files.borrow_mut().remove(&fd.0);
        Ok(())
    }

    async fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.files.borrow_mut().remove(&fd.0);
        Ok(())
    }
}

fn insert_slice_at_offset(
    vec: &Vec<Option<u8>>,
    offset: usize,
    slice: &[u8],
    allow_overwrite: bool,
) -> Result<Vec<Option<u8>>, StorageError> {
    // we clone the file so the write is 'atomic'. For this particular check
    // (overlapping writes) this happens before any writes to the FS in the
    // BufferCache implementation.
    let mut new_vec = vec.clone();

    if offset > vec.len() {
        new_vec.resize(offset, None);
    }

    // Insert or overwrite the data at the specified offset
    for (i, &byte) in slice.iter().enumerate() {
        if offset + i < new_vec.len() {
            if !allow_overwrite && vec[offset + i].is_some() {
                return Err(StorageError::OverlappingWrites);
            }
            new_vec[offset + i] = Some(byte);
        } else {
            new_vec.push(Some(byte));
        }
    }

    Ok(new_vec)
}

impl<const ALLOW_OVERWRITE: bool> StorageWrite for InMemoryBackend<ALLOW_OVERWRITE> {
    async fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Arc<FBuf>, StorageError> {
        let mut files = self.files.borrow_mut();
        let file = files.get(&fd.0).unwrap();
        let new_file = insert_slice_at_offset(file, offset as usize, &data, ALLOW_OVERWRITE)?;
        files.insert(fd.0, new_file);
        Ok(Arc::new(data))
    }

    async fn complete(
        &self,
        fd: FileHandle,
    ) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        let file = self.files.borrow_mut().remove(&fd.0).unwrap();
        self.immutable_files.borrow_mut().insert(fd.0, file);
        Ok((ImmutableFileHandle(fd.0), PathBuf::from("")))
    }
}

impl<const ALLOW_OVERWRITE: bool> StorageRead for InMemoryBackend<ALLOW_OVERWRITE> {
    async fn prefetch(&self, _fd: &ImmutableFileHandle, _offset: u64, _size: usize) {}

    async fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Arc<FBuf>, StorageError> {
        let files = self.immutable_files.borrow();
        let file = files.get(&fd.0).unwrap();
        let offset = offset as usize;
        if offset > file.len() || offset + size > file.len() {
            return Err(StorageError::ShortRead);
        }
        let end = offset + size;
        debug_assert!(end <= file.len());

        let mut buf = FBuf::with_capacity(end - offset);
        // reads to unwritten regions will still return 0s
        let slice: Vec<u8> = file[offset..end]
            .iter()
            .map(|x| x.unwrap_or_default())
            .collect();
        buf.extend_from_slice(&slice);
        Ok(Arc::new(buf))
    }

    async fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        let files = self.immutable_files.borrow();
        let file = files.get(&fd.0).unwrap();
        Ok(file.len() as u64)
    }
}

impl<const ALLOW_OVERWRITE: bool> ReferenceStateMachine for InMemoryBackend<ALLOW_OVERWRITE> {
    type State = Self;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(Default::default()).boxed()
    }

    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        prop_oneof![
            1 => Just(Transition::Create),
            2 => (1..=MAX_FILES).prop_map(Transition::DeleteMut),
            3 => (1..=MAX_FILES, limited_pow2(), pow2_string()).prop_map(|(ident, offset, content)| Transition::Write(ident, offset, content)),
            4 => (1..=MAX_FILES).prop_map(Transition::Complete),
            5 => (1..=MAX_FILES, limited_pow2(), limited_pow2()).prop_map(|(ident, offset, length)| Transition::Read(ident, offset, length)),
        ]
            .boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        state.error = None;
        match transition {
            Transition::Create => {
                let r = futures::executor::block_on(state.create());
                state.error = r.err();
            }
            Transition::DeleteMut(id) => {
                let r = futures::executor::block_on(state.delete_mut(FileHandle(*id)));
                state.error = r.err();
            }
            Transition::Write(id, offset, content) => {
                let mut buf = InMemoryBackend::<ALLOW_OVERWRITE>::allocate_buffer(content.len());
                buf.resize(content.len(), 0);
                buf.copy_from_slice(content.as_bytes());
                let r =
                    futures::executor::block_on(state.write_block(&FileHandle(*id), *offset, buf));
                state.error = r.err()
            }
            Transition::Complete(id) => {
                let r = futures::executor::block_on(state.complete(FileHandle(*id)));
                state.error = r.err();
            }
            Transition::Read(_ident, _offset, _length) => {}
        };
        state
    }

    /// We currently avoid testing the error cases in the state-machine. This
    /// can be added later by relaxing the preconditions.
    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::Create => true,
            Transition::DeleteMut(id) => state.files.borrow().contains_key(id),
            Transition::Write(id, _offset, _content) => state.files.borrow().contains_key(id),
            Transition::Complete(id) => state.files.borrow().contains_key(id),
            Transition::Read(id, _offset, _length) => {
                state.immutable_files.borrow().contains_key(id)
            }
        }
    }
}
