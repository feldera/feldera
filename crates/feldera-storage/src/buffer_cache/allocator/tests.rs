//! A state machine to perform tests for the allocator.
//! todo!()

use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr;

#[derive(Clone, Debug)]
enum Transition {
    Alloc(usize),
    Dealloc(ptr::NonNull<u8>, usize),
    Mutate(ptr::NonNull<u8>, String),
}

#[derive(Default, Debug)]
struct MallocReference {
    pub(crate) state: RefCell<HashMap<i64, Vec<u8>>>,
}
