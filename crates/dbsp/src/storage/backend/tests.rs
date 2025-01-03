//! A reference, in-memory implementation for a state machine test to check a
//! backend.
//!
//! TODO: Currently only functional STM, should later be expanded to cover
//! error/corner cases.

use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
};

use rand::{thread_rng, Fill, Rng};
use tokio::sync::oneshot;

use crate::storage::{backend::BlockLocation, buffer_cache::FBuf, test::init_test_logger};

use super::{FileReader, StorageBackend};

/// Returns a random length for reading a file of `size` bytes starting at
/// `offset`.
fn random_block_length(size: usize, offset: usize) -> usize {
    let remaining = size - offset;
    let mut length = 1 << thread_rng().gen_range(9..=20);
    while length > remaining {
        length /= 2;
    }
    assert!(offset == size || length > 0);
    length
}

/// Returns a random offset for reading in a file of `size` bytes.
fn random_block_offset(size: usize) -> usize {
    thread_rng().gen_range(0..=size / 512) * 512
}

fn random_block(size: usize) -> BlockLocation {
    assert!(size > 0);
    let offset = random_block_offset(size - 512);
    let size = random_block_length(size, offset);
    BlockLocation::new(offset as u64, size).unwrap()
}

fn test_read_block(reader: &dyn FileReader, data: &[u8], offset: usize) -> usize {
    let length = random_block_length(data.len(), offset);
    if length > 0 {
        let location = BlockLocation::new(offset as u64, length).unwrap();
        let block = reader.read_block(location).unwrap();
        assert_eq!(block.as_slice(), &data[offset..offset + length]);
    }
    length
}

fn test_read_async(reader: &dyn FileReader, data: &[u8]) {
    if data.is_empty() {
        // `read_async` disallows 0-length reads.
        return;
    }

    let n_blocks = thread_rng().gen_range(0..100);
    let blocks = (0..n_blocks)
        .map(|_| random_block(data.len()))
        .collect::<Vec<_>>();
    let (sender, receiver) = oneshot::channel();
    reader.read_async(
        blocks.clone(),
        Box::new(|results| sender.send(results).unwrap()),
    );
    let results = receiver.blocking_recv().unwrap();

    assert_eq!(results.len(), blocks.len());
    for (result, block) in results.into_iter().zip(blocks.into_iter()) {
        let result = result.unwrap();
        let range = block.offset as usize..block.offset as usize + block.size;
        assert_eq!(result.as_slice(), &data[range]);
    }
}

fn test_read(reader: &dyn FileReader, data: &[u8]) {
    assert_eq!(reader.get_size().unwrap(), data.len() as u64);
    let mut offset = 0;
    while offset < data.len() {
        offset += test_read_block(reader, data, offset);
    }
    for _ in 0..100 {
        test_read_block(reader, data, random_block_offset(data.len()));
    }
    for _ in 0..100 {
        test_read_async(reader, data);
    }
    reader
        .read_block(BlockLocation::new(data.len() as u64, 512).unwrap())
        .unwrap_err();
}

pub(super) fn test_backend(
    create_backend: Box<dyn FnOnce(&Path) -> Arc<dyn StorageBackend>>,
    writes: &[usize],
    mark_for_checkpoint: bool,
) {
    init_test_logger();
    let tmpdir = tempfile::tempdir().unwrap();
    let backend = create_backend(tmpdir.path());
    let mut rng = thread_rng();
    let mut writer = backend.create().unwrap();
    let mut data = Vec::new();

    let expected_size = writes.iter().sum::<usize>() as i64;
    for size in writes.iter().copied() {
        let mut block = FBuf::with_capacity(size);
        block.resize(size, 0);
        block.try_fill(&mut rng).unwrap();
        data.extend_from_slice(&block);
        writer.write_block(block).unwrap();
    }

    let (reader, name) = writer.complete().unwrap();
    assert_eq!(backend.usage().load(Ordering::Relaxed), expected_size);
    test_read(reader.as_ref(), &data);
    if mark_for_checkpoint {
        reader.mark_for_checkpoint();
    }
    drop(reader);

    if mark_for_checkpoint {
        assert_eq!(backend.usage().load(Ordering::Relaxed), expected_size);
        let reader = backend.open(&name).unwrap();
        test_read(reader.as_ref(), &data);
        drop(reader);
    } else {
        assert_eq!(backend.usage().load(Ordering::Relaxed), 0);
        let Err(_) = backend.open(&name) else {
            unreachable!()
        };
    }
}

pub(super) fn random_sizes() -> Vec<usize> {
    let mut rng = thread_rng();
    let mut blocks = Vec::new();
    let mut total = 0;
    while total < 1024 * 10 {
        let size = 1 << rng.gen_range(9..=20);
        blocks.push(size);
        total += size;
    }
    blocks
}
