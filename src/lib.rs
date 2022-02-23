/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) 2021 VMware, Inc
*/

extern crate num;

mod shared_ref;
pub use shared_ref::SharedRef;
pub mod algebra;
pub mod circuit;
pub mod operator;
mod test;
