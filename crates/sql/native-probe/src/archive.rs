//! Native writes of bounded, fixed-width integer tuple archives.
//!
//! The caller supplies offsets from the reference archive layout. This is not a
//! stable storage ABI: the rkyv features, Rust target, tuple arity and field types
//! must match that reference. No Rust tuple or rkyv serializer runs in the encoder.
//! Results are bytes, never unchecked typed archive references.

use cranelift_codegen::ir::{AbiParam, InstBuilder, MemFlags, condcodes::IntCC, types};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_module::{Linkage, Module};
use dlopen2::wrapper::{Container, WrapperApi};
use tempfile::TempDir;

use crate::{Batch, Error, codegen, runtime::link_object};

const MAX_ARCHIVE_FIELDS: usize = 8;

#[derive(Clone, Copy, Debug)]
pub enum ArchiveInteger {
    Bool,
    I32,
    U32,
    I64,
    U64,
    I128,
}

impl ArchiveInteger {
    pub(crate) fn size(self) -> usize {
        match self {
            Self::Bool => 1,
            Self::I32 | Self::U32 => 4,
            Self::I64 | Self::U64 => 8,
            Self::I128 => 16,
        }
    }

    fn fits(self, value: i128) -> bool {
        match self {
            Self::Bool => matches!(value, 0 | 1),
            Self::I32 => i32::try_from(value).is_ok(),
            Self::U32 => u32::try_from(value).is_ok(),
            Self::I64 => i64::try_from(value).is_ok(),
            Self::U64 => u64::try_from(value).is_ok(),
            Self::I128 => true,
        }
    }
}

/// Locations of one integer payload and its optional rkyv option discriminant.
/// A discriminant is 0 for NULL and 1 for a present value. NULL payloads are zero.
/// Fields inside one optional tuple share a discriminant and must have the same
/// presence in every input row.
#[derive(Clone, Copy, Debug)]
pub struct ArchiveField {
    pub integer: ArchiveInteger,
    pub payload_offset: usize,
    pub null_tag_offset: Option<usize>,
}

/// Checked bounds and disjoint field writes, limited to eight fields / 256 bytes.
/// Validation proves memory safety, not equivalence to a particular Rust type.
#[derive(Clone, Debug)]
pub struct ArchiveLayout {
    row_size: usize,
    fields: Vec<ArchiveField>,
    shared_null_masks: Vec<u64>,
}

impl ArchiveLayout {
    pub fn new(row_size: usize, fields: Vec<ArchiveField>) -> Result<Self, Error> {
        let invalid = |reason: &str| Error::InvalidArchiveLayout(reason.into());
        if !cfg!(target_endian = "little") {
            return Err(invalid(
                "wide archive slots require the tested little-endian target",
            ));
        }
        if row_size > 256
            || fields.len() > MAX_ARCHIVE_FIELDS
            || (row_size == 0) != fields.is_empty()
        {
            return Err(invalid(
                "requires a zero-byte unit row or up to 256 row bytes and 8 integer fields",
            ));
        }
        let mut occupied = vec![false; row_size];
        for field in &fields {
            let end = field
                .payload_offset
                .checked_add(field.integer.size())
                .filter(|end| *end <= row_size)
                .ok_or_else(|| invalid("payload extends beyond the row"))?;
            for byte in &mut occupied[field.payload_offset..end] {
                if std::mem::replace(byte, true) {
                    return Err(invalid("integer payloads must not overlap"));
                }
            }
        }
        let mut tag_masks = vec![0_u64; row_size];
        for (column, field) in fields.iter().enumerate() {
            if let Some(tag) = field.null_tag_offset {
                let payload = occupied
                    .get(tag)
                    .ok_or_else(|| invalid("null tag extends beyond the row"))?;
                if *payload {
                    return Err(invalid("payloads and null tags must not overlap"));
                }
                tag_masks[tag] |= 1 << column;
            }
        }
        let shared_null_masks = tag_masks
            .into_iter()
            .filter(|mask| mask.count_ones() > 1)
            .collect();
        Ok(Self {
            row_size,
            fields,
            shared_null_masks,
        })
    }

    fn check_batch(&self, batch: &Batch) -> Result<(), Error> {
        batch.check_columns(self.fields.len())?;
        for row in 0..batch.rows() {
            for (column, field) in self.fields.iter().enumerate() {
                match batch.value(row, column) {
                    None if field.null_tag_offset.is_none() => {
                        return Err(Error::InvalidBatch(format!(
                            "NULL in required archive field {column}, row {row}"
                        )));
                    }
                    Some(value) if !field.integer.fits(value.into()) => {
                        return Err(Error::InvalidBatch(format!(
                            "archive field {column}, row {row} does not fit {:?}",
                            field.integer
                        )));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn wide_row_slots(&self, row: &[Option<i128>], slots: &mut [i128]) -> Result<u64, Error> {
        if row.len() != self.fields.len() {
            return Err(Error::InvalidBatch("wide archive row width differs".into()));
        }
        let mut mask = 0_u64;
        for (index, (value, field)) in row.iter().zip(&self.fields).enumerate() {
            match value {
                None if field.null_tag_offset.is_none() => {
                    return Err(Error::InvalidBatch(
                        "NULL in required wide archive field".into(),
                    ));
                }
                None => mask |= 1 << index,
                Some(value) if !field.integer.fits(*value) => {
                    return Err(Error::InvalidBatch("wide archive integer overflow".into()));
                }
                Some(_) => {}
            }
            slots[index] = value.unwrap_or(0);
        }
        Ok(mask)
    }

    fn check_presence(&self, masks: &[u64]) -> Result<(), Error> {
        for &group in &self.shared_null_masks {
            if let Some(row) = masks.iter().position(|mask| {
                let null_fields = mask & group;
                null_fields != 0 && null_fields != group
            }) {
                return Err(Error::InvalidBatch(format!(
                    "inconsistent presence in optional tuple at archive row {row}"
                )));
            }
        }
        Ok(())
    }
}

/// Owned byte archives remain valid after the encoder and its library are dropped.
/// Rows are not promised to be aligned for typed rkyv access.
pub struct ArchivedBatch {
    bytes: Vec<u8>,
    row_size: usize,
    rows: usize,
}

impl ArchivedBatch {
    pub fn rows(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        (0..self.rows).map(|row| &self.bytes[row * self.row_size..(row + 1) * self.row_size])
    }
}

#[derive(WrapperApi)]
struct ArchiveApi {
    feldera_archive_batch:
        unsafe extern "C" fn(values: *const i128, nulls: *const u64, rows: usize, output: *mut u8),
}

/// Owns the validated layout and code; no callable pointer escapes this owner.
pub struct CompiledArchive {
    library: Container<ArchiveApi>,
    _directory: TempDir,
    layout: ArchiveLayout,
}

impl CompiledArchive {
    /// Reads one fixed-width row using the layout that wrote it.
    pub fn decode_row(&self, row: &[u8]) -> Result<Vec<Option<i128>>, Error> {
        if row.len() != self.layout.row_size {
            return Err(Error::InvalidBatch("archive row size differs".into()));
        }
        self.layout
            .fields
            .iter()
            .map(|field| {
                if let Some(tag) = field.null_tag_offset {
                    match row[tag] {
                        0 => return Ok(None),
                        1 => {}
                        _ => return Err(Error::InvalidBatch("invalid archive null tag".into())),
                    }
                }
                let offset = field.payload_offset;
                let bytes = &row[offset..offset + field.integer.size()];
                let value = match field.integer {
                    ArchiveInteger::Bool => match bytes[0] {
                        bit @ (0 | 1) => i128::from(bit),
                        _ => return Err(Error::InvalidBatch("invalid archive boolean".into())),
                    },
                    ArchiveInteger::I32 => i32::from_le_bytes(bytes.try_into().unwrap()).into(),
                    ArchiveInteger::U32 => u32::from_le_bytes(bytes.try_into().unwrap()).into(),
                    ArchiveInteger::I64 => i64::from_le_bytes(bytes.try_into().unwrap()).into(),
                    ArchiveInteger::U64 => u64::from_le_bytes(bytes.try_into().unwrap()).into(),
                    ArchiveInteger::I128 => i128::from_le_bytes(bytes.try_into().unwrap()),
                };
                Ok(Some(value))
            })
            .collect()
    }

    pub fn compile(layout: ArchiveLayout) -> Result<Self, Error> {
        let (directory, library_path) = link_object(object(&layout)?)?;
        // SAFETY: This private object defines the exact ArchiveApi C ABI on the
        // host ISA, without constructors, callbacks or unwinding. The container
        // owns the library until after every encode call has returned.
        let library = unsafe { Container::load(&library_path) }.map_err(|error| {
            Error::Compile(format!("cannot load native archive encoder: {error}"))
        })?;
        Ok(Self {
            library,
            _directory: directory,
            layout,
        })
    }

    pub fn encode(&self, batch: &Batch) -> Result<ArchivedBatch, Error> {
        self.layout.check_batch(batch)?;
        let values: Vec<i128> = batch
            .values
            .iter()
            .map(|value| i128::from(*value))
            .collect();
        self.encode_slots(&values, &batch.nulls, batch.rows())
    }

    /// Full-width scalar slots for exact integer/decimal mantissas. Decimal
    /// precision and scale belong to the caller's SQL type descriptor; this
    /// method checks width, NULLs and allocation bounds, never rescales a value.
    pub fn encode_wide(&self, rows: &[Vec<Option<i128>>]) -> Result<ArchivedBatch, Error> {
        if rows.len() > 65_536 {
            return Err(Error::InvalidBatch(
                "archive batch exceeds 65536 rows".into(),
            ));
        }
        let width = self.layout.fields.len();
        let mut values = vec![0; rows.len() * width];
        let mut masks = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            let start = index * width;
            masks.push(
                self.layout
                    .wide_row_slots(row, &mut values[start..start + width])?,
            );
        }
        self.encode_slots(&values, &masks, rows.len())
    }

    /// Encode one wide row into a caller-owned buffer without allocating.
    /// A rejected row leaves the output buffer untouched.
    pub fn encode_wide_row_into(
        &self,
        row: &[Option<i128>],
        output: &mut [u8],
    ) -> Result<(), Error> {
        if output.len() != self.layout.row_size {
            return Err(Error::InvalidBatch(
                "wide archive output size differs".into(),
            ));
        }
        let mut slots = [0; MAX_ARCHIVE_FIELDS];
        let slots = &mut slots[..self.layout.fields.len()];
        let mask = self.layout.wide_row_slots(row, slots)?;
        self.layout.check_presence(std::slice::from_ref(&mask))?;
        output.fill(0);
        self.write_slots(slots, std::slice::from_ref(&mask), output);
        Ok(())
    }

    fn encode_slots(
        &self,
        values: &[i128],
        masks: &[u64],
        rows: usize,
    ) -> Result<ArchivedBatch, Error> {
        self.layout.check_presence(masks)?;
        let mut bytes = vec![0; rows * self.layout.row_size];
        self.write_slots(values, masks, &mut bytes);
        Ok(ArchivedBatch {
            bytes,
            row_size: self.layout.row_size,
            rows,
        })
    }

    fn write_slots(&self, values: &[i128], masks: &[u64], output: &mut [u8]) {
        let rows = masks.len();
        debug_assert_eq!(values.len(), rows * self.layout.fields.len());
        debug_assert_eq!(output.len(), rows * self.layout.row_size);
        // Both private callers validate row count, width, NULLs and narrowing.
        // Checked layouts have <=8 fields and <=256 bytes, so all allocations
        // and generated offsets stay bounded, including 128-bit input slots.
        // SAFETY: validated inputs contain rows*fields i128 slots and rows masks.
        // Generated code loads two 64-bit limbs for I128 and writes only checked,
        // disjoint field ranges. Buffers and the loaded library remain owned for
        // the call; empty batches dereference no pointer. The C ABI cannot unwind.
        unsafe {
            self.library.feldera_archive_batch(
                values.as_ptr(),
                masks.as_ptr(),
                rows,
                output.as_mut_ptr(),
            );
        }
    }
}

fn object(layout: &ArchiveLayout) -> Result<Vec<u8>, Error> {
    let failure = |error: String| Error::Compile(error);
    let mut module = codegen::module()?;
    let mut context = module.make_context();
    context.func.signature.params = vec![AbiParam::new(types::I64); 4];
    let function = module
        .declare_function(
            "feldera_archive_batch",
            Linkage::Export,
            &context.func.signature,
        )
        .map_err(|error| failure(error.to_string()))?;
    let mut builder_context = FunctionBuilderContext::new();
    let mut builder = FunctionBuilder::new(&mut context.func, &mut builder_context);
    emit_batch(&mut builder, layout);
    builder.seal_all_blocks();
    builder.finalize();
    module
        .define_function(function, &mut context)
        .map_err(|error| failure(error.to_string()))?;
    module
        .finish()
        .emit()
        .map_err(|error| failure(error.to_string()))
}

fn emit_batch(builder: &mut FunctionBuilder<'_>, layout: &ArchiveLayout) {
    let entry = builder.create_block();
    builder.append_block_params_for_function_params(entry);
    builder.switch_to_block(entry);
    let arguments = builder.block_params(entry).to_vec();
    let header = builder.create_block();
    let body = builder.create_block();
    let done = builder.create_block();
    builder.append_block_param(header, types::I64);
    let zero = builder.ins().iconst(types::I64, 0);
    builder.ins().jump(header, &[zero.into()]);
    builder.switch_to_block(header);
    let row = builder.block_params(header)[0];
    let more = builder
        .ins()
        .icmp(IntCC::UnsignedLessThan, row, arguments[2]);
    builder.ins().brif(more, body, &[], done, &[]);
    builder.switch_to_block(body);
    let input_offset = builder
        .ins()
        .imul_imm(row, (layout.fields.len() * 16) as i64);
    let input = builder.ins().iadd(arguments[0], input_offset);
    let mask_offset = builder.ins().imul_imm(row, 8);
    let mask_address = builder.ins().iadd(arguments[1], mask_offset);
    let mask = builder
        .ins()
        .load(types::I64, MemFlags::new(), mask_address, 0);
    let output_offset = builder.ins().imul_imm(row, layout.row_size as i64);
    let output = builder.ins().iadd(arguments[3], output_offset);
    for (column, field) in layout.fields.iter().enumerate() {
        let mut value =
            builder
                .ins()
                .load(types::I64, MemFlags::new(), input, (column * 16) as i32);
        let mut high = if matches!(field.integer, ArchiveInteger::I128) {
            Some(
                builder
                    .ins()
                    .load(types::I64, MemFlags::new(), input, (column * 16 + 8) as i32),
            )
        } else {
            None
        };
        if let Some(tag_offset) = field.null_tag_offset {
            let bit = builder.ins().ushr_imm(mask, column as i64);
            let bit = builder.ins().band_imm(bit, 1);
            let present = builder.ins().icmp_imm(IntCC::Equal, bit, 0);
            if !layout.fields[..column]
                .iter()
                .any(|field| field.null_tag_offset == Some(tag_offset))
            {
                builder
                    .ins()
                    .store(MemFlags::new(), present, output, tag_offset as i32);
            }
            value = builder.ins().select(present, value, zero);
            high = high.map(|high| builder.ins().select(present, high, zero));
        }
        value = match field.integer {
            ArchiveInteger::Bool => builder.ins().ireduce(types::I8, value),
            ArchiveInteger::I32 | ArchiveInteger::U32 => builder.ins().ireduce(types::I32, value),
            ArchiveInteger::I64 | ArchiveInteger::U64 | ArchiveInteger::I128 => value,
        };
        builder
            .ins()
            .store(MemFlags::new(), value, output, field.payload_offset as i32);
        if let Some(high) = high {
            builder.ins().store(
                MemFlags::new(),
                high,
                output,
                (field.payload_offset + 8) as i32,
            );
        }
    }
    let next = builder.ins().iadd_imm(row, 1);
    builder.ins().jump(header, &[next.into()]);
    builder.switch_to_block(done);
    builder.ins().return_(&[]);
}
