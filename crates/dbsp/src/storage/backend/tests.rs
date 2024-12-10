//! A reference, in-memory implementation for a state machine test to check a
//! backend.
//!
//! TODO: Currently only functional STM, should later be expanded to cover
//! error/corner cases.

use std::path::Path;

use rand::{thread_rng, Fill, Rng};

use crate::storage::{buffer_cache::FBuf, test::init_test_logger};

use super::{Backend, FileReader};

fn test_read_block(reader: &dyn FileReader, data: &[u8], offset: usize) -> usize {
    let remaining = data.len() - offset;
    let mut length = 1 << thread_rng().gen_range(9..=20);
    while length > remaining {
        length /= 2;
    }
    assert!(offset == data.len() || length > 0);
    let block = reader.read_block(offset as u64, length).unwrap();
    assert_eq!(block.as_slice(), &data[offset..offset + length]);
    length
}

fn test_read(reader: &dyn FileReader, data: &[u8]) {
    let mut rng = thread_rng();
    assert_eq!(reader.get_size().unwrap(), data.len() as u64);
    let mut offset = 0;
    while offset < data.len() {
        offset += test_read_block(reader, data, offset);
    }
    for _ in 0..100 {
        test_read_block(reader, data, rng.gen_range(0..=data.len() / 512) * 512);
    }
    reader.read_block(data.len() as u64, 512).unwrap_err();
}

pub(super) fn test_backend(
    create_backend: Box<dyn FnOnce(&Path) -> Backend>,
    writes: &[usize],
    sequential: bool,
    mark_for_checkpoint: bool,
) {
    init_test_logger();
    let tmpdir = tempfile::tempdir().unwrap();
    let backend = create_backend(tmpdir.path());
    let mut rng = thread_rng();
    let mut writer = backend.create().unwrap();
    let mut data = Vec::new();
    for (index, size) in writes.iter().copied().enumerate() {
        if sequential || rng.gen_range(0..10) != 0 || index == writes.len() - 1 {
            let mut block = FBuf::with_capacity(size);
            block.resize(size, 0);
            block.try_fill(&mut rng).unwrap();
            let offset = data.len() as u64;
            data.extend_from_slice(&block);
            writer.write_block(offset, block).unwrap();
        } else {
            // Occasionally skip over a block.
            data.resize(data.len() + size, 0);
        }
    }

    let (reader, name) = writer.complete().unwrap();
    test_read(reader.as_ref(), &data);
    if mark_for_checkpoint {
        reader.mark_for_checkpoint();
    }
    drop(reader);

    if mark_for_checkpoint {
        let reader = backend.open(&name).unwrap();
        test_read(reader.as_ref(), &data);
        drop(reader);
    } else {
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
