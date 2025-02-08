// Implement the function signatures from stubs.rs

use crate::*;
use feldera_sqllib::*;
use base64::prelude::*; // Able to use external crates declared in udf.toml

pub fn base64(s: Option<ByteArray>) -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
    Ok(s.map(|v| SqlString::from_ref(&BASE64_STANDARD.encode(v.as_slice()))))
}
