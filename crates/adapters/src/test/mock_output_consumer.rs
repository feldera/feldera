#![allow(clippy::type_complexity)]

use crate::{transport::Step, OutputConsumer};
use std::sync::{Arc, Mutex};

pub struct MockOutputConsumer {
    pub data: Arc<
        Mutex<
            Vec<(
                Option<Vec<u8>>,
                Option<Vec<u8>>,
                Vec<(String, Option<Vec<u8>>)>,
            )>,
        >,
    >,
    max_buffer_size_bytes: usize,
}

impl Default for MockOutputConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl MockOutputConsumer {
    pub fn new() -> Self {
        Self::with_max_buffer_size_bytes(usize::MAX)
    }

    pub fn state(
        &self,
    ) -> Vec<(
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Vec<(String, Option<Vec<u8>>)>,
    )> {
        self.data.lock().unwrap().clone()
    }

    pub fn with_max_buffer_size_bytes(bytes: usize) -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
            max_buffer_size_bytes: bytes,
        }
    }

    pub fn with_buffer(
        data: Arc<
            Mutex<
                Vec<(
                    Option<Vec<u8>>,
                    Option<Vec<u8>>,
                    Vec<(String, Option<Vec<u8>>)>,
                )>,
            >,
        >,
    ) -> Self {
        Self {
            data,
            max_buffer_size_bytes: usize::MAX,
        }
    }
}

impl OutputConsumer for MockOutputConsumer {
    fn max_buffer_size_bytes(&self) -> usize {
        self.max_buffer_size_bytes
    }

    fn batch_start(&mut self, _step: Step) {}
    fn push_buffer(&mut self, buffer: &[u8], _num_records: usize) {
        self.data
            .lock()
            .unwrap()
            .push((None, Some(buffer.to_vec()), vec![]))
    }
    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        // println!("push_key {:?} , {:?}", key, val);
        self.data.lock().unwrap().push((
            key.map(|k| k.to_vec()),
            val.map(|v| v.to_vec()),
            headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.map(|bytes| bytes.to_vec())))
                .collect::<Vec<_>>(),
        ))
    }
    fn batch_end(&mut self) {}
}
