//! Bounded nullable keys whose byte order equals the requested SQL order.
//! This experimental encoding is not a persisted DBSP archive or hash format.

use crate::{Batch, Error};

const BYTES_PER_FIELD: usize = 9;
const MAX_KEY_BYTES: usize = 16 * 1024 * 1024;

/// NULL placement is independent of the direction used for non-NULL values.
#[derive(Clone, Copy, Debug)]
pub struct KeyField {
    pub column: usize,
    pub descending: bool,
    pub nulls_first: bool,
}

/// One typed column of a batch. Decimal mantissas must share a fixed scale.
pub enum KeyColumn<'a> {
    BigInt(&'a [Option<i64>]),
    WideInt(&'a [Option<i128>]),
    Text(&'a [Option<&'a str>]),
}

impl KeyColumn<'_> {
    fn len(&self) -> usize {
        match self {
            Self::BigInt(values) => values.len(),
            Self::WideInt(values) => values.len(),
            Self::Text(values) => values.len(),
        }
    }

    fn value(&self, row: usize) -> Option<KeyValue<'_>> {
        match self {
            Self::BigInt(values) => values[row].map(KeyValue::BigInt),
            Self::WideInt(values) => values[row].map(KeyValue::WideInt),
            Self::Text(values) => values[row].map(KeyValue::Text),
        }
    }
}

enum KeyValue<'a> {
    BigInt(i64),
    WideInt(i128),
    Text(&'a str),
}

impl KeyValue<'_> {
    fn append(self, output: &mut Vec<u8>, descending: bool) -> Result<(), Error> {
        let payload_size = match self {
            Self::BigInt(_) => size_of::<i64>(),
            Self::WideInt(_) => size_of::<i128>(),
            Self::Text(value) => {
                if value.len() > MAX_KEY_BYTES {
                    return Err(key_size_error());
                }
                value.len() + value.bytes().filter(|byte| *byte == 0).count() + 2
            }
        };
        check_capacity(output, payload_size)?;
        let start = output.len();
        match self {
            Self::BigInt(value) => {
                output.extend_from_slice(&((value as u64) ^ (1 << 63)).to_be_bytes())
            }
            Self::WideInt(value) => {
                output.extend_from_slice(&((value as u128) ^ (1 << 127)).to_be_bytes())
            }
            Self::Text(value) => {
                // Encode NUL as [0, 255] and terminate with [0, 0]. A shorter
                // prefix sorts first and a following field cannot extend it.
                for byte in value.bytes() {
                    output.push(byte);
                    if byte == 0 {
                        output.push(255);
                    }
                }
                output.extend_from_slice(&[0, 0]);
            }
        }
        if descending {
            for byte in &mut output[start..] {
                *byte = !*byte;
            }
        }
        Ok(())
    }
}

fn key_size_error() -> Error {
    Error::InvalidKeyLayout("encoded keys exceed 16 MiB".into())
}

fn check_capacity(output: &[u8], additional: usize) -> Result<(), Error> {
    if additional > MAX_KEY_BYTES - output.len() {
        return Err(key_size_error());
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct KeyLayout {
    columns: usize,
    fields: Vec<KeyField>,
}

impl KeyLayout {
    pub fn new(columns: usize, fields: Vec<KeyField>) -> Result<Self, Error> {
        if columns > 64 || fields.len() > 64 || fields.iter().any(|f| f.column >= columns) {
            return Err(Error::InvalidKeyLayout(
                "expected at most 64 columns and key fields, with every field in bounds".into(),
            ));
        }
        Ok(Self { columns, fields })
    }

    /// Encode once before sorting or merging. Duplicate fields and an empty
    /// key remain meaningful; the latter makes all rows compare equal.
    pub fn encode(&self, batch: &Batch) -> Result<NormalizedKeys, Error> {
        batch.check_columns(self.columns)?;
        self.encode_slots(batch.rows(), BYTES_PER_FIELD, |row, column| {
            batch.value(row, column).map(i128::from)
        })
    }

    /// Full-width integer keys, also usable for decimal mantissas when each
    /// field has one fixed scale. The SQL type adapter must validate that scale;
    /// this method neither rescales values nor compares different decimal types.
    pub fn encode_wide(&self, rows: &[Vec<Option<i128>>]) -> Result<NormalizedKeys, Error> {
        if rows.len() > 65_536
            || rows.len().saturating_mul(self.columns) > 1_048_576
            || rows.iter().any(|row| row.len() != self.columns)
        {
            return Err(Error::InvalidKeyLayout(
                "wide key batch shape or size differs".into(),
            ));
        }
        self.encode_slots(rows.len(), 17, |row, column| rows[row][column])
    }

    /// Encode a mixed batch. Text order follows UTF-8 byte order, as used by
    /// Rust strings. Supply fixed-scale mantissas for decimal columns.
    pub fn encode_columns(
        &self,
        rows: usize,
        columns: &[KeyColumn<'_>],
    ) -> Result<NormalizedKeys, Error> {
        if rows > 65_536
            || rows * self.columns > 1_048_576
            || columns.len() != self.columns
            || columns.iter().any(|column| column.len() != rows)
        {
            return Err(Error::InvalidKeyLayout(
                "mixed key batch shape or size differs".into(),
            ));
        }

        let mut bytes = Vec::new();
        let mut boundaries = Vec::with_capacity(rows + 1);
        boundaries.push(0);
        for row in 0..rows {
            for field in &self.fields {
                let value = columns[field.column].value(row);
                check_capacity(&bytes, 1)?;
                bytes.push(u8::from(value.is_some() == field.nulls_first));
                if let Some(value) = value {
                    value.append(&mut bytes, field.descending)?;
                }
            }
            boundaries.push(bytes.len());
        }
        Ok(NormalizedKeys {
            bytes,
            offsets: Offsets::Boundaries(boundaries),
        })
    }

    fn encode_slots(
        &self,
        rows: usize,
        bytes_per_field: usize,
        value: impl Fn(usize, usize) -> Option<i128>,
    ) -> Result<NormalizedKeys, Error> {
        let width = self.fields.len() * bytes_per_field;
        let size = width
            .checked_mul(rows)
            .filter(|n| *n <= MAX_KEY_BYTES)
            .ok_or_else(|| Error::InvalidKeyLayout("encoded keys exceed 16 MiB".into()))?;
        let mut bytes = Vec::with_capacity(size);
        for row in 0..rows {
            for field in &self.fields {
                match value(row, field.column) {
                    None => {
                        bytes.push(u8::from(!field.nulls_first));
                        bytes.resize(bytes.len() + bytes_per_field - 1, 0);
                    }
                    Some(value) => {
                        bytes.push(u8::from(field.nulls_first));
                        // Flip the sign bit, then encode most-significant byte
                        // first. NULL order stays independent of value direction.
                        if bytes_per_field == BYTES_PER_FIELD {
                            let ordered = (value as u64) ^ (1_u64 << 63);
                            let ordered = if field.descending { !ordered } else { ordered };
                            bytes.extend_from_slice(&ordered.to_be_bytes());
                        } else {
                            let ordered = (value as u128) ^ (1_u128 << 127);
                            let ordered = if field.descending { !ordered } else { ordered };
                            bytes.extend_from_slice(&ordered.to_be_bytes());
                        }
                    }
                }
            }
        }
        Ok(NormalizedKeys {
            bytes,
            offsets: Offsets::Fixed { width, rows },
        })
    }
}

/// Contiguous owned keys avoid one allocation per row. A key borrows this
/// storage and cannot outlive the batch that owns it.
#[derive(Clone, Debug)]
pub struct NormalizedKeys {
    bytes: Vec<u8>,
    offsets: Offsets,
}

#[derive(Clone, Debug)]
enum Offsets {
    Fixed { width: usize, rows: usize },
    Boundaries(Vec<usize>),
}

impl NormalizedKeys {
    pub fn rows(&self) -> usize {
        match &self.offsets {
            Offsets::Fixed { rows, .. } => *rows,
            Offsets::Boundaries(boundaries) => boundaries.len() - 1,
        }
    }

    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    pub fn key(&self, row: usize) -> Option<&[u8]> {
        if row >= self.rows() {
            return None;
        }
        let range = match &self.offsets {
            Offsets::Fixed { width, .. } => row * width..(row + 1) * width,
            Offsets::Boundaries(boundaries) => boundaries[row]..boundaries[row + 1],
        };
        Some(&self.bytes[range])
    }
}
