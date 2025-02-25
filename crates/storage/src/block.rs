use std::fmt::Display;

/// A block that can be read or written in a [crate::FileReader] or [crate::FileWriter].
#[derive(Copy, Clone, Debug)]
pub struct BlockLocation {
    /// Byte offset, a multiple of 512.
    pub offset: u64,

    /// Size in bytes, a multiple of 512, less than `2**31`.
    ///
    /// (The upper limit is because some kernel APIs return the number of bytes
    /// read as an `i32`.)
    pub size: usize,
}

impl BlockLocation {
    /// Constructs a new [BlockLocation], validating `offset` and `size`.
    pub fn new(offset: u64, size: usize) -> Result<Self, InvalidBlockLocation> {
        if (offset % 512) != 0 || !(512..1 << 31).contains(&size) || (size % 512) != 0 {
            Err(InvalidBlockLocation { offset, size })
        } else {
            Ok(Self { offset, size })
        }
    }

    /// File offset just after this block.
    pub fn after(&self) -> u64 {
        self.offset + self.size as u64
    }
}

impl Display for BlockLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} bytes at offset {}", self.size, self.offset)
    }
}

/// A range of bytes in a file that doesn't satisfy the constraints for
/// [BlockLocation].
#[derive(Copy, Clone, Debug)]
pub struct InvalidBlockLocation {
    /// Byte offset.
    pub offset: u64,

    /// Number of bytes.
    pub size: usize,
}
