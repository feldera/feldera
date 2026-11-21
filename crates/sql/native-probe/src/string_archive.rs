//! Native writes of the pinned little-endian rkyv size_64 ArchivedString layout.
//! Planning computes allocation offsets in tuple field order. Generated code
//! writes string bytes, inline headers, relative pointers and optional tags.
//! Returned archives own their bytes and contain no process pointer.
use crate::{ArchiveField, ArchiveLayout, CompiledArchive, Error, codegen, runtime::link_object};
use cranelift_codegen::ir::{
    AbiParam, Block, InstBuilder, MemFlags, Value, condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_module::{Linkage, Module};
use dlopen2::wrapper::{Container, WrapperApi};
use std::{
    mem::{offset_of, size_of},
    ops::Range,
};
use tempfile::TempDir;
const HEADER: usize = 16;
const INLINE: usize = 15;
const MAX_STRING_BYTES: usize = 1024 * 1024;
const MAX_BYTES: usize = 16 * 1024 * 1024;
const NO_TAG: u64 = u64::MAX;

#[derive(Clone, Copy, Debug)]
pub struct StringArchiveField {
    pub header_offset: usize,
    pub null_tag_offset: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct StringArchiveLayout {
    row_size: usize,
    root_alignment: usize,
    fields: Vec<StringArchiveField>,
}

impl StringArchiveLayout {
    pub fn new(
        row_size: usize,
        string_offset: usize,
        null_tag: Option<usize>,
    ) -> Result<Self, Error> {
        Self::tuple(
            row_size,
            vec![StringArchiveField {
                header_offset: string_offset,
                null_tag_offset: null_tag,
            }],
        )
    }

    /// Layout fields are in serialization order, independent of root offsets.
    pub fn tuple(row_size: usize, fields: Vec<StringArchiveField>) -> Result<Self, Error> {
        Self::aligned_tuple(row_size, 8, fields)
    }

    fn aligned_tuple(
        row_size: usize,
        root_alignment: usize,
        fields: Vec<StringArchiveField>,
    ) -> Result<Self, Error> {
        let invalid = |reason: &str| Error::InvalidArchiveLayout(reason.into());
        if !cfg!(all(target_endian = "little", target_pointer_width = "64"))
            || !(1..=256).contains(&row_size)
            || root_alignment < 8
            || !root_alignment.is_power_of_two()
            || !row_size.is_multiple_of(root_alignment)
            || !(1..=8).contains(&fields.len())
        {
            return Err(invalid(
                "expected an aligned little-endian size_64 string tuple",
            ));
        }
        let mut occupied = [false; 256];
        for field in &fields {
            let end = field
                .header_offset
                .checked_add(HEADER)
                .filter(|&end| field.header_offset.is_multiple_of(8) && end <= row_size)
                .ok_or_else(|| invalid("unaligned string header or header outside the row"))?;
            for offset in (field.header_offset..end).chain(field.null_tag_offset) {
                if offset >= row_size || std::mem::replace(&mut occupied[offset], true) {
                    return Err(invalid(
                        "string headers and null tags must be disjoint and in bounds",
                    ));
                }
            }
        }
        Ok(Self {
            row_size,
            root_alignment,
            fields,
        })
    }
}
#[repr(C)]
struct StringWrite {
    source: *const u8,
    len: u64,
    header: u64,
    data: u64,
    tag: u64,
    present: u64,
}
#[derive(WrapperApi)]
struct StringApi {
    feldera_archive_strings: unsafe extern "C" fn(
        writes: *const StringWrite,
        count: usize,
        output: *mut u8,
        copy_payload: u64,
    ),
}
pub struct CompiledStringArchive {
    library: Container<StringApi>,
    _directory: TempDir,
    layout: StringArchiveLayout,
}
pub struct StringArchives {
    bytes: Vec<u8>,
    rows: Vec<(Range<usize>, usize)>,
}
impl StringArchives {
    /// Each slice is a self-contained archive; the position locates its root.
    pub fn rows(&self) -> impl ExactSizeIterator<Item = (&[u8], usize)> {
        self.rows
            .iter()
            .map(|(range, root)| (&self.bytes[range.clone()], *root))
    }
}
impl CompiledStringArchive {
    pub fn compile(layout: StringArchiveLayout) -> Result<Self, Error> {
        let (directory, path) = link_object(object()?)?;
        // SAFETY: this private generated object exports exactly StringApi, has
        // no constructors/callbacks, and its owner retains it through every call.
        let library =
            unsafe { Container::load(&path) }.map_err(|e| Error::Compile(e.to_string()))?;
        Ok(Self {
            library,
            _directory: directory,
            layout,
        })
    }

    pub fn encode(&self, values: &[Option<&str>]) -> Result<StringArchives, Error> {
        self.encode_columns(&[values])
    }

    /// Reads the single UTF-8 field described by this layout.
    ///
    /// The returned string borrows the archive. Both the root and any
    /// out-of-line payload must be within the supplied byte slice.
    pub fn decode_single<'a>(
        &self,
        bytes: &'a [u8],
        root_position: usize,
    ) -> Result<Option<&'a str>, Error> {
        let invalid = || Error::InvalidBatch("invalid single-string archive".into());
        let [field] = self.layout.fields.as_slice() else {
            return Err(invalid());
        };
        let root_end = root_position
            .checked_add(self.layout.row_size)
            .ok_or_else(invalid)?;
        let root = bytes.get(root_position..root_end).ok_or_else(invalid)?;
        if let Some(tag) = field.null_tag_offset {
            match root[tag] {
                0 => return Ok(None),
                1 => {}
                _ => return Err(invalid()),
            }
        }
        let header = &root[field.header_offset..field.header_offset + HEADER];
        let text = if header[INLINE] & 0x80 == 0 {
            if usize::from(header[INLINE]) > INLINE {
                return Err(invalid());
            }
            &header[..usize::from(header[INLINE])]
        } else {
            let len = usize::try_from(u64::from_le_bytes(header[..8].try_into().unwrap()))
                .map_err(|_| invalid())?;
            if !(HEADER..=MAX_STRING_BYTES).contains(&len) {
                return Err(invalid());
            }
            let relative = isize::try_from(i64::from_le_bytes(header[8..].try_into().unwrap()))
                .map_err(|_| invalid())?;
            let start = (root_position + field.header_offset)
                .checked_add_signed(relative)
                .ok_or_else(invalid)?;
            let end = start
                .checked_add(len)
                .filter(|end| *end <= root_position)
                .ok_or_else(invalid)?;
            &bytes[start..end]
        };
        std::str::from_utf8(text).map(Some).map_err(|_| invalid())
    }

    /// Whether the single-field writer accepts this value and layout.
    pub fn supports_single_value(&self, value: Option<&str>) -> bool {
        let [field] = self.layout.fields.as_slice() else {
            return false;
        };
        value.map_or(field.null_tag_offset.is_some(), |text| {
            text.len() <= MAX_STRING_BYTES
        })
    }

    /// Write the root of a single-string archive into caller-owned storage.
    ///
    /// For long strings the caller writes the UTF-8 payload separately and
    /// supplies its absolute archive position. The generated root points to
    /// those bytes from its final position, including any intervening fields.
    pub fn encode_single_root_into(
        &self,
        value: Option<&str>,
        root_position: usize,
        payload_position: Option<usize>,
        output: &mut [u8],
    ) -> Result<(), Error> {
        let invalid =
            || Error::InvalidBatch("invalid single-string archive root or payload".into());
        let [field] = self.layout.fields.as_slice() else {
            return Err(invalid());
        };
        if !self.supports_single_value(value)
            || output.len() != self.layout.row_size
            || !root_position.is_multiple_of(self.layout.root_alignment)
            || root_position
                .checked_add(output.len())
                .is_none_or(|end| i64::try_from(end).is_err())
        {
            return Err(invalid());
        }
        let text = value.unwrap_or("");
        let data = match payload_position {
            Some(position)
                if text.len() > INLINE
                    && position
                        .checked_add(text.len())
                        .is_some_and(|end| end <= root_position) =>
            {
                let position = i64::try_from(position).map_err(|_| invalid())?;
                let root = i64::try_from(root_position).map_err(|_| invalid())?;
                (position - root) as u64
            }
            None if text.len() <= INLINE => 0,
            _ => return Err(invalid()),
        };
        let write = StringWrite {
            source: text.as_ptr(),
            len: text.len() as u64,
            header: field.header_offset as u64,
            data,
            tag: field.null_tag_offset.map_or(NO_TAG, |tag| tag as u64),
            present: u64::from(value.is_some()),
        };
        output.fill(0);
        // SAFETY: the validated layout fits the output buffer. The native
        // writer reads inline bytes from text and writes headers and tags only.
        // It encodes the long payload's displacement without dereferencing it.
        unsafe {
            self.library
                .feldera_archive_strings(&write, 1, output.as_mut_ptr(), 0);
        }
        Ok(())
    }

    pub fn encode_columns(&self, columns: &[&[Option<&str>]]) -> Result<StringArchives, Error> {
        let count = columns.first().map_or(0, |column| column.len());
        if count > 65_536
            || columns.len() != self.layout.fields.len()
            || columns.iter().any(|column| column.len() != count)
        {
            return Err(Error::InvalidBatch(
                "string archive column count, row count, or column length differs from its layout"
                    .into(),
            ));
        }
        let mut writes = Vec::with_capacity(count * columns.len());
        let mut rows = Vec::with_capacity(count);
        let mut total = 0_usize;
        for row in 0..count {
            let first = writes.len();
            let mut payload_end = total;
            for (column, field) in columns.iter().zip(&self.layout.fields) {
                let value = column[row];
                if value.is_none() && field.null_tag_offset.is_none() {
                    return Err(Error::InvalidBatch(
                        "NULL in required string archive".into(),
                    ));
                }
                let text = value.unwrap_or("");
                if text.len() > MAX_STRING_BYTES {
                    return Err(Error::InvalidBatch("string exceeds 1 MiB".into()));
                }
                writes.push(StringWrite {
                    source: text.as_ptr(),
                    len: text.len() as u64,
                    header: field.header_offset as u64,
                    data: payload_end as u64,
                    tag: field.null_tag_offset.map_or(NO_TAG, |tag| tag as u64),
                    present: u64::from(value.is_some()),
                });
                if text.len() > INLINE {
                    payload_end += text.len();
                }
            }
            // Payloads are serialized consecutively at byte alignment. The
            // archived tuple root is aligned once, after the last payload.
            let padding_mask = self.layout.root_alignment - 1;
            let root = (payload_end + padding_mask) & !padding_mask;
            let end = root + self.layout.row_size;
            if end > MAX_BYTES {
                return Err(Error::InvalidBatch("string archive exceeds 16 MiB".into()));
            }
            for write in &mut writes[first..] {
                write.header += root as u64;
                if write.tag != NO_TAG {
                    write.tag += root as u64;
                }
            }
            rows.push((total..end, root - total));
            total = end;
        }
        let mut bytes = vec![0; total];
        // SAFETY: writes borrow live UTF-8 text. Each header and optional tag
        // belongs to a validated disjoint root range. Long payloads occupy
        // separate ranges preceding the root. Native loops copy the validated
        // length, and NULL values skip both the header and payload.
        unsafe {
            self.library.feldera_archive_strings(
                writes.as_ptr(),
                writes.len(),
                bytes.as_mut_ptr(),
                1,
            );
        }
        Ok(StringArchives { bytes, rows })
    }
}

/// Combines the two native writers for a tuple with ordered string payloads
/// and fixed-width integer slots. The checked slots are merged into each root.
pub struct CompiledMixedArchive {
    strings: CompiledStringArchive,
    integers: CompiledArchive,
    integer_ranges: Vec<Range<usize>>,
}

impl CompiledMixedArchive {
    pub fn compile(
        row_size: usize,
        strings: Vec<StringArchiveField>,
        integers: Vec<ArchiveField>,
    ) -> Result<Self, Error> {
        Self::compile_aligned(row_size, 8, strings, integers)
    }

    pub fn compile_aligned(
        row_size: usize,
        root_alignment: usize,
        strings: Vec<StringArchiveField>,
        integers: Vec<ArchiveField>,
    ) -> Result<Self, Error> {
        let string_layout = StringArchiveLayout::aligned_tuple(row_size, root_alignment, strings)?;
        let integer_layout = ArchiveLayout::new(row_size, integers.clone())?;
        let integer_ranges: Vec<_> = integers
            .iter()
            .flat_map(|field| {
                let payload = field.payload_offset..field.payload_offset + field.integer.size();
                std::iter::once(payload).chain(field.null_tag_offset.map(|tag| tag..tag + 1))
            })
            .collect();
        for field in &string_layout.fields {
            let header = field.header_offset..field.header_offset + HEADER;
            if integer_ranges.iter().any(|range| {
                range.start < header.end && header.start < range.end
                    || field
                        .null_tag_offset
                        .is_some_and(|tag| range.contains(&tag))
            }) {
                return Err(Error::InvalidArchiveLayout(
                    "string and integer archive fields must be disjoint".into(),
                ));
            }
        }
        Ok(Self {
            strings: CompiledStringArchive::compile(string_layout)?,
            integers: CompiledArchive::compile(integer_layout)?,
            integer_ranges,
        })
    }

    pub fn encode(
        &self,
        strings: &[&[Option<&str>]],
        integers: &[Vec<Option<i128>>],
    ) -> Result<StringArchives, Error> {
        if strings
            .first()
            .is_none_or(|first| first.len() != integers.len())
        {
            return Err(Error::InvalidBatch(
                "string and integer archive batches have different row counts".into(),
            ));
        }
        let mut combined = self.strings.encode_columns(strings)?;
        let fixed = self.integers.encode_wide(integers)?;
        for ((range, root), fixed_row) in combined.rows.iter().zip(fixed.rows()) {
            let target = &mut combined.bytes[range.start + root..range.end];
            for slot in &self.integer_ranges {
                target[slot.clone()].copy_from_slice(&fixed_row[slot.clone()]);
            }
        }
        Ok(combined)
    }
}

fn object() -> Result<Vec<u8>, Error> {
    let mut module = codegen::module()?;
    let mut context = module.make_context();
    context.func.signature.params = vec![AbiParam::new(types::I64); 4];
    let id = module
        .declare_function(
            "feldera_archive_strings",
            Linkage::Export,
            &context.func.signature,
        )
        .map_err(|e| Error::Compile(e.to_string()))?;
    let mut fbc = FunctionBuilderContext::new();
    let mut b = FunctionBuilder::new(&mut context.func, &mut fbc);
    emit(&mut b);
    b.seal_all_blocks();
    b.finalize();
    module
        .define_function(id, &mut context)
        .map_err(|e| Error::Compile(e.to_string()))?;
    module
        .finish()
        .emit()
        .map_err(|e| Error::Compile(e.to_string()))
}
fn copy_bytes(b: &mut FunctionBuilder<'_>, source: Value, target: Value, len: Value, done: Block) {
    let header = b.create_block();
    let body = b.create_block();
    b.append_block_param(header, types::I64);
    let zero = b.ins().iconst(types::I64, 0);
    b.ins().jump(header, &[zero.into()]);
    b.switch_to_block(header);
    let index = b.block_params(header)[0];
    let more = b.ins().icmp(IntCC::UnsignedLessThan, index, len);
    b.ins().brif(more, body, &[], done, &[]);
    b.switch_to_block(body);
    let from = b.ins().iadd(source, index);
    let to = b.ins().iadd(target, index);
    let byte = b.ins().load(types::I8, MemFlags::new(), from, 0);
    b.ins().store(MemFlags::new(), byte, to, 0);
    let next = b.ins().iadd_imm(index, 1);
    b.ins().jump(header, &[next.into()]);
}
fn emit(b: &mut FunctionBuilder<'_>) {
    let entry = b.create_block();
    b.append_block_params_for_function_params(entry);
    b.switch_to_block(entry);
    let args = b.block_params(entry).to_vec();
    let header = b.create_block();
    let row_block = b.create_block();
    let write_tag = b.create_block();
    let check_presence = b.create_block();
    let present_block = b.create_block();
    let inline = b.create_block();
    let outline = b.create_block();
    let copy_payload = b.create_block();
    let next = b.create_block();
    let done = b.create_block();
    b.append_block_param(header, types::I64);
    let zero = b.ins().iconst(types::I64, 0);
    b.ins().jump(header, &[zero.into()]);
    b.switch_to_block(header);
    let row = b.block_params(header)[0];
    let more = b.ins().icmp(IntCC::UnsignedLessThan, row, args[1]);
    b.ins().brif(more, row_block, &[], done, &[]);
    b.switch_to_block(row_block);
    let offset = b.ins().imul_imm(row, size_of::<StringWrite>() as i64);
    let plan = b.ins().iadd(args[0], offset);
    let source = b.ins().load(
        types::I64,
        MemFlags::new(),
        plan,
        offset_of!(StringWrite, source) as i32,
    );
    let len = b.ins().load(
        types::I64,
        MemFlags::new(),
        plan,
        offset_of!(StringWrite, len) as i32,
    );
    let string_position = b.ins().load(
        types::I64,
        MemFlags::new(),
        plan,
        offset_of!(StringWrite, header) as i32,
    );
    let data = b.ins().load(
        types::I64,
        MemFlags::new(),
        plan,
        offset_of!(StringWrite, data) as i32,
    );
    let tag = b.ins().load(
        types::I64,
        MemFlags::new(),
        plan,
        offset_of!(StringWrite, tag) as i32,
    );
    let present = b.ins().load(
        types::I64,
        MemFlags::new(),
        plan,
        offset_of!(StringWrite, present) as i32,
    );
    let has_tag = b.ins().icmp_imm(IntCC::NotEqual, tag, NO_TAG as i64);
    b.ins().brif(has_tag, write_tag, &[], check_presence, &[]);
    b.switch_to_block(write_tag);
    let tag_address = b.ins().iadd(args[2], tag);
    let tag_value = b.ins().ireduce(types::I8, present);
    b.ins().store(MemFlags::new(), tag_value, tag_address, 0);
    b.ins().jump(check_presence, &[]);
    b.switch_to_block(check_presence);
    let exists = b.ins().icmp_imm(IntCC::NotEqual, present, 0);
    b.ins().brif(exists, present_block, &[], next, &[]);
    b.switch_to_block(present_block);
    let string_address = b.ins().iadd(args[2], string_position);
    let short = b
        .ins()
        .icmp_imm(IntCC::UnsignedLessThanOrEqual, len, INLINE as i64);
    b.ins().brif(short, inline, &[], outline, &[]);
    b.switch_to_block(inline);
    let inline_len = b.ins().ireduce(types::I8, len);
    b.ins()
        .store(MemFlags::new(), inline_len, string_address, INLINE as i32);
    copy_bytes(b, source, string_address, len, next);
    b.switch_to_block(outline);
    b.ins().store(MemFlags::new(), len, string_address, 0);
    let relative = b.ins().isub(data, string_position);
    b.ins().store(MemFlags::new(), relative, string_address, 8);
    let standalone = b.ins().icmp_imm(IntCC::NotEqual, args[3], 0);
    b.ins().brif(standalone, copy_payload, &[], next, &[]);
    b.switch_to_block(copy_payload);
    let data_address = b.ins().iadd(args[2], data);
    copy_bytes(b, source, data_address, len, next);
    b.switch_to_block(next);
    let next_row = b.ins().iadd_imm(row, 1);
    b.ins().jump(header, &[next_row.into()]);
    b.switch_to_block(done);
    b.ins().return_(&[]);
}
