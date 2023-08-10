use crate::OutputConsumer;
use std::sync::{Arc, Mutex};

pub struct MockOutputConsumer {
    pub data: Arc<Mutex<Vec<u8>>>,
}

impl MockOutputConsumer {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl OutputConsumer for MockOutputConsumer {
    fn batch_start(&mut self) {}
    fn push_buffer(&mut self, buffer: &[u8]) {
        self.data.lock().unwrap().extend_from_slice(buffer)
    }
    fn batch_end(&mut self) {}
}
