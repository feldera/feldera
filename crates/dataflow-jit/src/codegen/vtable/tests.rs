#![cfg(test)]

use crate::{
    codegen::{Codegen, CodegenConfig},
    ir::{ColumnType, RowLayoutBuilder, RowLayoutCache},
    row::UninitRow,
    ThinStr,
};
use dbsp::{trace::layers::erased::DataVTable, utils::DynVec};
use size_of::{Context, SizeOf, TotalSize};
use std::{
    cmp::Ordering,
    collections::hash_map::DefaultHasher,
    fmt::{self, Debug, Write},
    hash::{BuildHasher, BuildHasherDefault, Hasher},
};

// TODO: Test nullable fields
// TODO: Proptesting

#[test]
fn empty() {
    let layout_cache = RowLayoutCache::new();
    let empty_layout = layout_cache.add(RowLayoutBuilder::new().build());

    {
        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
        let vtable = codegen.vtable_for(empty_layout);

        let (module, layouts) = codegen.finalize_definitions();
        let vtable = vtable.erased(&module);

        let layout = layouts.layout_of(empty_layout);
        assert_eq!(layout.size(), 0);
        assert!(layout.is_zero_sized());

        let (lhs, rhs) = (
            layout.alloc().unwrap().as_ptr(),
            layout.alloc().unwrap().as_ptr(),
        );

        unsafe {
            assert!((vtable.eq)(lhs, rhs));
            assert!(!(vtable.lt)(lhs, rhs));
            assert_eq!((vtable.cmp)(lhs, rhs), Ordering::Equal);

            let clone = layout.alloc().unwrap().as_ptr();
            (vtable.clone)(lhs, clone);
            assert!((vtable.eq)(lhs, clone));

            let debug = DebugRow(lhs, vtable.debug).debug();
            assert_eq!(debug, "{}");

            let type_name = vtable.type_name();
            assert_eq!(type_name, "{}");

            let mut ctx = Context::new();
            (vtable.size_of_children)(lhs, &mut ctx);
            assert_eq!(ctx.total_size(), TotalSize::zero());

            let builder = BuildHasherDefault::<DefaultHasher>::default();
            let lhs_hash_1 = {
                let mut hasher = builder.build_hasher();
                (vtable.hash)(&mut (&mut hasher as &mut dyn Hasher), lhs);
                hasher.finish()
            };
            let lhs_hash_2 = {
                let mut hasher = builder.build_hasher();
                (vtable.hash)(&mut (&mut hasher as &mut dyn Hasher), lhs);
                hasher.finish()
            };
            let rhs_hash = {
                let mut hasher = builder.build_hasher();
                (vtable.hash)(&mut (&mut hasher as &mut dyn Hasher), rhs);
                hasher.finish()
            };
            assert_eq!(lhs_hash_1, lhs_hash_2);
            assert_eq!(lhs_hash_1, rhs_hash);

            (vtable.drop_slice_in_place)(lhs, 1);
            (vtable.drop_in_place)(rhs);
            (vtable.drop_in_place)(clone);
            layout.dealloc(lhs);
            layout.dealloc(rhs);
            layout.dealloc(clone);

            module.free_memory();
        }
    }
}

#[test]
fn string_smoke() {
    let layout_cache = RowLayoutCache::new();
    let string_layout = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::String, false)
            .build(),
    );

    {
        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
        let vtable = codegen.vtable_for(string_layout);

        let (module, layouts) = codegen.finalize_definitions();
        let vtable = vtable.erased(&module);

        let layout = layouts.layout_of(string_layout);
        let (lhs, rhs) = (
            layout.alloc().unwrap().as_ptr(),
            layout.alloc().unwrap().as_ptr(),
        );

        unsafe {
            let offset = layout.offset_of(0) as usize;
            lhs.add(offset)
                .cast::<ThinStr>()
                .write(ThinStr::from("foobar"));
            rhs.add(offset)
                .cast::<ThinStr>()
                .write(ThinStr::from("foobar"));

            assert!((vtable.eq)(lhs, rhs));
            assert!(!(vtable.lt)(lhs, rhs));
            assert_eq!((vtable.cmp)(lhs, rhs), Ordering::Equal);

            let clone = layout.alloc().unwrap().as_ptr();
            (vtable.clone)(lhs, clone);
            assert!((vtable.eq)(lhs, clone));

            for ptr in [lhs, rhs, clone] {
                let debug = DebugRow(ptr, vtable.debug).debug();
                assert_eq!(debug, r#"{ "foobar" }"#);
            }

            assert_eq!(vtable.type_name(), "{str}");

            // Ensure a slice drop of length zero doesn't do anything
            (vtable.drop_slice_in_place)(lhs, 0);

            let mut ctx = Context::new();
            (vtable.size_of_children)(lhs, &mut ctx);
            let expected = {
                let mut ctx = Context::new();
                ThinStr::from("foobar").size_of_children(&mut ctx);
                ctx.total_size()
            };
            assert_eq!(ctx.total_size(), expected);

            let builder = BuildHasherDefault::<DefaultHasher>::default();
            let lhs_hash_1 = {
                let mut hasher = builder.build_hasher();
                (vtable.hash)(&mut (&mut hasher as &mut dyn Hasher), lhs);
                hasher.finish()
            };
            let lhs_hash_2 = {
                let mut hasher = builder.build_hasher();
                (vtable.hash)(&mut (&mut hasher as &mut dyn Hasher), lhs);
                hasher.finish()
            };
            let rhs_hash = {
                let mut hasher = builder.build_hasher();
                (vtable.hash)(&mut (&mut hasher as &mut dyn Hasher), rhs);
                hasher.finish()
            };
            assert_eq!(lhs_hash_1, lhs_hash_2);
            assert_eq!(lhs_hash_1, rhs_hash);

            (vtable.drop_slice_in_place)(lhs, 1);
            (vtable.drop_in_place)(rhs);
            (vtable.drop_in_place)(clone);

            layout.dealloc(lhs);
            layout.dealloc(rhs);
            layout.dealloc(clone);

            let values = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"];

            let src = layout.alloc_array(values.len()).unwrap().as_ptr();
            let dest = layout.alloc_array(values.len()).unwrap().as_ptr();

            // Initialize src
            for (idx, &val) in values.iter().enumerate() {
                src.add(idx * layout.size() as usize)
                    .cast::<ThinStr>()
                    .write(ThinStr::from(val));
            }
            for (idx, &val) in values.iter().enumerate() {
                let string = &*src.add(idx * layout.size() as usize).cast::<ThinStr>();
                assert_eq!(string.as_str(), val);
            }

            // Clone src into dest
            (vtable.clone_into_slice)(src, dest, values.len());

            // Ensure dest contains the correct values
            for (idx, &val) in values.iter().enumerate() {
                let string = &*dest.add(idx * layout.size() as usize).cast::<ThinStr>();
                assert_eq!(string.as_str(), val);
            }

            // Drop all values within src and dest
            (vtable.drop_slice_in_place)(src, values.len());
            (vtable.drop_slice_in_place)(dest, values.len());

            // Deallocate the arrays
            layout.dealloc_array(src, values.len());
            layout.dealloc_array(dest, values.len());

            module.free_memory();
        }
    }
}

#[test]
fn dyn_vec() {
    let types = [
        ColumnType::Bool,
        ColumnType::U16,
        ColumnType::U32,
        ColumnType::U64,
        ColumnType::I16,
        ColumnType::I32,
        ColumnType::I64,
        ColumnType::F32,
        ColumnType::F64,
        ColumnType::Unit,
        ColumnType::String,
    ];

    // TODO: Proptest data generation
    let data = &[
        (
            (true, Some(false)),
            (53u16, Some(1255u16)),
            (u32::MAX, Some(u32::MAX / 2)),
            (u64::MAX / 5, Some(u64::MAX / 4)),
            (i16::MAX, Some(i16::MIN)),
            (i32::MAX, Some(i32::MIN)),
            (i64::MAX, Some(i64::MIN)),
            (std::f32::consts::PI, Some(std::f32::consts::FRAC_1_SQRT_2)),
            (std::f64::consts::LN_2, Some(std::f64::consts::TAU)),
            ((), Some(())),
            (
                ThinStr::from("foobarbazbop"),
                Some(ThinStr::from("lorem ipsum")),
            ),
        ),
        (
            (false, None),
            (95u16, None),
            (u32::MAX / 2, None),
            (u64::MAX / 10, None),
            (i16::MAX, None),
            (i32::MAX, None),
            (i64::MAX, None),
            (std::f32::consts::LOG2_10, None),
            (std::f64::consts::TAU, None),
            ((), None),
            (ThinStr::from("foobarbaz"), None),
        ),
    ];

    let layout_cache = RowLayoutCache::new();

    let mut builder = RowLayoutBuilder::new();
    for ty in types {
        builder.add_column(ty, false).add_column(ty, true);
    }
    let layout_id = layout_cache.add(builder.build());

    {
        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
        let vtable = codegen.vtable_for(layout_id);
        let (jit, layout_cache) = codegen.finalize_definitions();

        let layout = layout_cache.layout_of(layout_id);
        let data_vtable = Box::into_raw(Box::new(DataVTable {
            common: vtable.erased(&jit),
        }));
        let vtable = Box::into_raw(Box::new(vtable.marshalled(&jit)));

        {
            // Create a dynamic vector
            let mut vec = DynVec::new(unsafe { &*data_vtable });

            // Initialize the dynvec
            {
                // We don't statically know the layout of the value we're creating, so we
                // allocate a place for it on the heap, write to it and then push to our DynVec
                // from that place
                let mut place = UninitRow::new(unsafe { &*vtable });

                macro_rules! emplace {
                    ($($value:ident: $ty:ty),+ $(,)?) => {
                        let mut column = 0;

                        $(
                            place
                                .as_mut_ptr()
                                .add(layout.offset_of(column) as usize)
                                .cast::<$ty>()
                                .write($value.0.clone());
                            column += 1;

                            place.set_column_null(column, &layout, $value.1.is_none());

                            if let Some(val) = $value.1.clone() {
                                place
                                    .as_mut_ptr()
                                    .add(layout.offset_of(column) as usize)
                                    .cast::<$ty>()
                                    .write(val);
                            }

                            #[allow(unused_assignments)]
                            { column += 1 };
                        )+
                    };
                }

                for (bools, u16s, u32s, u64s, i16s, i32s, i64s, f32s, f64s, units, strs) in data {
                    unsafe {
                        // Fill `place` with our data
                        emplace! {
                            bools: bool,
                            u16s: u16,
                            u32s: u32,
                            u64s: u64,
                            i16s: i16,
                            i32s: i32,
                            i64s: i64,
                            f32s: f32,
                            f64s: f64,
                            units: (),
                            strs: ThinStr,
                        }

                        // Push the value we created to the vec
                        vec.push_raw(place.as_mut_ptr());
                    }
                }
            }

            let clone = vec.clone();
            assert_eq!(vec, clone);
            drop(clone);

            println!("{vec:#?}");
        }

        unsafe {
            drop(Box::from_raw(vtable));
            drop(Box::from_raw(data_vtable));
            jit.free_memory();
        }
    }
}

struct DebugRow(
    *const u8,
    unsafe extern "C" fn(*const u8, *mut fmt::Formatter) -> bool,
);

impl DebugRow {
    pub fn debug(self) -> String {
        format!("{self:?}")
    }
}

impl Debug for DebugRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if unsafe { (self.1)(self.0, f) } {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

struct FailWriter;

impl Write for FailWriter {
    fn write_str(&mut self, _: &str) -> fmt::Result {
        Err(fmt::Error)
    }
}

mod proptests {
    use crate::{
        codegen::{Codegen, CodegenConfig},
        ir::{ColumnType, RowLayout, RowLayoutBuilder, RowLayoutCache},
        row::UninitRow,
        utils::NativeRepr,
        ThinStr,
    };
    use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use proptest::{
        prelude::any, prop_assert, prop_assert_eq, prop_assert_ne, prop_compose, proptest,
        strategy::Strategy, test_runner::TestCaseResult,
    };
    use proptest_derive::Arbitrary;
    use rust_decimal::Decimal;
    use size_of::SizeOf;
    use std::{
        cmp::Ordering,
        collections::hash_map::DefaultHasher,
        hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
        mem::align_of,
    };

    #[derive(Debug, Clone, Arbitrary)]
    enum Column {
        Unit,
        U8(u8),
        I8(i8),
        U16(u16),
        I16(i16),
        U32(u32),
        I32(i32),
        U64(u64),
        I64(i64),
        F32(f32),
        F64(f64),
        Bool(bool),
        // Usize(usize),
        String(String),
        #[proptest(strategy = "date().prop_map(|date| Column::Date(date.num_days_from_ce()))")]
        Date(i32),
        #[proptest(strategy = "timestamp().prop_map(|date| Column::Timestamp(date.timestamp()))")]
        Timestamp(i64),
        Decimal(Decimal),
    }

    prop_compose! {
        fn date()(days in NaiveDate::MIN.num_days_from_ce()..=NaiveDate::MAX.num_days_from_ce()) -> NaiveDate {
            NaiveDate::from_num_days_from_ce_opt(days).unwrap()
        }
    }

    prop_compose! {
        fn time()(secs in 0..=86_399u32, nanos in 0..=1_999_999_999u32) -> NaiveTime {
            NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).unwrap()
        }
    }

    prop_compose! {
        fn timestamp()(date in date(), time in time()) -> DateTime<Utc> {
            let datetime = NaiveDateTime::new(date, time);
            DateTime::from_utc(datetime, Utc)
        }
    }

    impl Column {
        fn row_type(&self) -> ColumnType {
            match self {
                Self::Unit => ColumnType::Unit,
                Self::U8(_) => ColumnType::U8,
                Self::I8(_) => ColumnType::I8,
                Self::U16(_) => ColumnType::U16,
                Self::I16(_) => ColumnType::I16,
                Self::U32(_) => ColumnType::U32,
                Self::I32(_) => ColumnType::I32,
                Self::U64(_) => ColumnType::U64,
                Self::I64(_) => ColumnType::I64,
                Self::F32(_) => ColumnType::F32,
                Self::F64(_) => ColumnType::F64,
                Self::Bool(_) => ColumnType::Bool,
                // Column::Usize(_) => todo!(),
                Self::String(_) => ColumnType::String,
                Self::Date(_) => ColumnType::Date,
                Self::Timestamp(_) => ColumnType::Timestamp,
                Self::Decimal(_) => ColumnType::Decimal,
            }
        }

        unsafe fn write_to(&self, ptr: *mut u8) -> TestCaseResult {
            prop_assert!(!ptr.is_null());

            match *self {
                Self::Unit => {
                    prop_assert_eq!(ptr as usize % align_of::<()>(), 0);
                    ptr.cast::<()>().write(());
                }

                Self::U8(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<u8>(), 0);
                    ptr.cast::<u8>().write(value);
                }
                Self::I8(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<i8>(), 0);
                    ptr.cast::<i8>().write(value);
                }

                Self::U16(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<u16>(), 0);
                    ptr.cast::<u16>().write(value);
                }
                Self::I16(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<i16>(), 0);
                    ptr.cast::<i16>().write(value);
                }

                Self::U32(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<u32>(), 0);
                    ptr.cast::<u32>().write(value);
                }
                Self::I32(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<i32>(), 0);
                    ptr.cast::<i32>().write(value);
                }

                Self::U64(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<u64>(), 0);
                    ptr.cast::<u64>().write(value);
                }
                Self::I64(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<i64>(), 0);
                    ptr.cast::<i64>().write(value);
                }

                Self::F32(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<f32>(), 0);
                    ptr.cast::<f32>().write(value);
                }
                Self::F64(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<f64>(), 0);
                    ptr.cast::<f64>().write(value);
                }

                Self::Bool(value) => {
                    prop_assert_eq!(ptr as usize % align_of::<bool>(), 0);
                    ptr.cast::<bool>().write(value);
                }

                Self::String(ref value) => {
                    prop_assert_eq!(ptr as usize % align_of::<ThinStr>(), 0);
                    ptr.cast::<ThinStr>().write(ThinStr::from(&**value));
                }

                Self::Date(date) => {
                    prop_assert_eq!(ptr as usize % align_of::<i32>(), 0);
                    ptr.cast::<i32>().write(date);
                }
                Self::Timestamp(timestamp) => {
                    prop_assert_eq!(ptr as usize % align_of::<i64>(), 0);
                    ptr.cast::<i64>().write(timestamp);
                }

                Self::Decimal(decimal) => {
                    prop_assert_eq!(
                        ptr as usize % align_of::<<Decimal as NativeRepr>::Repr>(),
                        0
                    );
                    ptr.cast::<<Decimal as NativeRepr>::Repr>()
                        .write(decimal.to_repr());
                }
            }

            Ok(())
        }
    }

    impl PartialEq for Column {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (Self::U8(lhs), Self::U8(rhs)) => lhs == rhs,
                (Self::I8(lhs), Self::I8(rhs)) => lhs == rhs,
                (Self::U16(lhs), Self::U16(rhs)) => lhs == rhs,
                (Self::I16(lhs), Self::I16(rhs)) => lhs == rhs,
                (Self::U32(lhs), Self::U32(rhs)) => lhs == rhs,
                (Self::I32(lhs), Self::I32(rhs)) => lhs == rhs,
                (Self::U64(lhs), Self::U64(rhs)) => lhs == rhs,
                (Self::I64(lhs), Self::I64(rhs)) => lhs == rhs,
                (Self::F32(lhs), Self::F32(rhs)) => {
                    if lhs.is_nan() {
                        rhs.is_nan()
                    } else {
                        lhs == rhs
                    }
                }
                (Self::F64(lhs), Self::F64(rhs)) => {
                    if lhs.is_nan() {
                        rhs.is_nan()
                    } else {
                        lhs == rhs
                    }
                }
                (Self::Bool(lhs), Self::Bool(rhs)) => lhs == rhs,
                // (Self::Usize(l0), Self::Usize(r0)) => l0 == r0,
                (Self::String(lhs), Self::String(rhs)) => lhs == rhs,
                (Self::Date(lhs), Self::Date(rhs)) => lhs == rhs,
                (Self::Timestamp(lhs), Self::Timestamp(rhs)) => lhs == rhs,
                (Self::Decimal(lhs), Self::Decimal(rhs)) => lhs == rhs,
                _ => unreachable!(),
            }
        }
    }

    impl Eq for Column {}

    impl PartialOrd for Column {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Column {
        fn cmp(&self, other: &Self) -> Ordering {
            match (self, other) {
                (Self::U8(lhs), Self::U8(rhs)) => lhs.cmp(rhs),
                (Self::I8(lhs), Self::I8(rhs)) => lhs.cmp(rhs),
                (Self::U16(lhs), Self::U16(rhs)) => lhs.cmp(rhs),
                (Self::I16(lhs), Self::I16(rhs)) => lhs.cmp(rhs),
                (Self::U32(lhs), Self::U32(rhs)) => lhs.cmp(rhs),
                (Self::I32(lhs), Self::I32(rhs)) => lhs.cmp(rhs),
                (Self::U64(lhs), Self::U64(rhs)) => lhs.cmp(rhs),
                (Self::I64(lhs), Self::I64(rhs)) => lhs.cmp(rhs),
                (Self::F32(lhs), Self::F32(rhs)) => match lhs.partial_cmp(rhs) {
                    Some(ordering) => ordering,
                    None => {
                        if lhs.is_nan() {
                            if rhs.is_nan() {
                                Ordering::Equal
                            } else {
                                Ordering::Greater
                            }
                        } else {
                            Ordering::Less
                        }
                    }
                },
                (Self::F64(lhs), Self::F64(rhs)) => match lhs.partial_cmp(rhs) {
                    Some(ordering) => ordering,
                    None => {
                        if lhs.is_nan() {
                            if rhs.is_nan() {
                                Ordering::Equal
                            } else {
                                Ordering::Greater
                            }
                        } else {
                            Ordering::Less
                        }
                    }
                },
                (Self::Bool(lhs), Self::Bool(rhs)) => lhs.cmp(rhs),
                // (Self::Usize(l0), Self::Usize(r0)) => l0.cmp(r0),
                (Self::String(lhs), Self::String(rhs)) => lhs.cmp(rhs),
                (Self::Date(lhs), Self::Date(rhs)) => lhs.cmp(rhs),
                (Self::Timestamp(lhs), Self::Timestamp(rhs)) => lhs.cmp(rhs),
                (Self::Decimal(lhs), Self::Decimal(rhs)) => lhs.cmp(rhs),
                _ => unreachable!(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
    enum MaybeColumn {
        Nonnull(Column),
        Nullable(Column, bool),
    }

    impl MaybeColumn {
        fn row_type(&self) -> ColumnType {
            match self {
                Self::Nonnull(column) | Self::Nullable(column, _) => column.row_type(),
            }
        }

        fn nullable(&self) -> bool {
            matches!(self, Self::Nullable(..))
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
    struct PropLayout {
        columns: Vec<MaybeColumn>,
    }

    impl PropLayout {
        fn new(columns: Vec<MaybeColumn>) -> Self {
            Self { columns }
        }

        fn row_layout(&self) -> RowLayout {
            let mut builder = RowLayoutBuilder::new();
            for column in &self.columns {
                builder.add_column(column.row_type(), column.nullable());
            }

            builder.build()
        }
    }

    fn test_layout(value: &PropLayout, debug: bool) -> TestCaseResult {
        let cache = RowLayoutCache::new();
        let layout_id = cache.add(value.row_layout());

        let config = if debug {
            CodegenConfig::debug()
        } else {
            CodegenConfig::release()
        };
        let mut codegen = Codegen::new(cache, config);
        let vtable = codegen.vtable_for(layout_id);

        let (jit, cache) = codegen.finalize_definitions();
        let vtable = Box::into_raw(Box::new(vtable.marshalled(&jit)));
        let layout = cache.layout_of(layout_id);
        prop_assert_ne!(layout.align(), 0);
        prop_assert!(layout.align().is_power_of_two());

        let mut row = UninitRow::new(unsafe { &*vtable });
        for (idx, column) in value.columns.iter().enumerate() {
            let offset = layout.offset_of(idx) as usize;

            unsafe {
                match column {
                    MaybeColumn::Nonnull(value) => {
                        // Write the column's value
                        let column = row.as_mut_ptr().add(offset);
                        value.write_to(column)?;
                    }

                    MaybeColumn::Nullable(value, false) => {
                        // Write the column's value
                        let column = row.as_mut_ptr().add(offset);
                        value.write_to(column)?;

                        // Set the column to not be null
                        row.set_column_null(idx, &layout, false);
                        prop_assert!(!row.column_is_null(idx, &layout));
                    }

                    MaybeColumn::Nullable(_, true) => {
                        // Set the column to be null
                        row.set_column_null(idx, &layout, true);
                        prop_assert!(row.column_is_null(idx, &layout));
                    }
                }
            }
        }
        let row = unsafe { row.assume_init() };

        let clone = row.clone();
        prop_assert_eq!(&row, &clone);
        prop_assert_eq!(row.partial_cmp(&clone), Some(Ordering::Equal));
        prop_assert_eq!(row.cmp(&clone), Ordering::Equal);
        prop_assert!(!row.ne(&clone));
        prop_assert!(!row.lt(&clone));
        prop_assert!(row.le(&clone));
        prop_assert!(!row.gt(&clone));
        prop_assert!(row.ge(&clone));

        let builder = BuildHasherDefault::<DefaultHasher>::default();
        let row_hash_1 = {
            let mut hasher = builder.build_hasher();
            row.hash(&mut hasher);
            hasher.finish()
        };
        let row_hash_2 = {
            let mut hasher = builder.build_hasher();
            row.hash(&mut hasher);
            hasher.finish()
        };
        let clone_hash = {
            let mut hasher = builder.build_hasher();
            clone.hash(&mut hasher);
            hasher.finish()
        };
        prop_assert_eq!(row_hash_1, row_hash_2);
        prop_assert_eq!(row_hash_1, clone_hash);

        // TODO: Assert that these are correct
        let debug1 = format!("{row:?}");
        let debug2 = format!("{row:?}");
        prop_assert_eq!(debug1, debug2);

        // TODO: Assert that these are correct
        let size_of1 = row.size_of();
        let size_of2 = row.size_of();
        prop_assert_eq!(size_of1, size_of2);

        // TODO: Assert that these are correct
        let type_name1 = row.type_name();
        let type_name2 = row.type_name();
        prop_assert_eq!(type_name1, type_name2);

        // TODO: Test clone_into_slice and drop_slice

        unsafe {
            drop(row);
            drop(clone);
            drop(Box::from_raw(vtable));
            jit.free_memory();
        }

        Ok(())
    }

    proptest! {
        #[test]
        fn vtables(value in any::<PropLayout>()) {
            // crate::utils::test_logger();

            tracing::debug!("testing layout in debug mode");
            test_layout(&value, true)?;

            tracing::debug!("testing layout in release mode");
            test_layout(&value, false)?;
        }
    }

    macro_rules! corpus {
        ($($test:ident = [$($column:expr),* $(,)?]),+ $(,)?) => {
            mod corpus {
                use super::{test_layout, Column::*, MaybeColumn::*, PropLayout};
                use std::str::FromStr;

                $(
                    #[test]
                    fn $test() {
                        crate::utils::test_logger();

                        let layout = PropLayout::new(vec![$($column,)*]);
                        test_layout(&layout, true).unwrap();

                        let layout = PropLayout::new(vec![$($column,)*]);
                        test_layout(&layout, false).unwrap();
                    }
                )+
            }
        };
    }

    corpus! {
        empty_layout = [],
        null_string = [Nullable(String("".to_owned()), true)],
        non_null_string = [Nullable(String("supercalifragilisticexpialidocious".to_owned()), true)],
        prop1 = [Nullable(U16(0), false), Nonnull(U32(0)), Nullable(Unit, false)],
        prop2 =  [
            Nullable(F32(4.8600124e-10), false), Nonnull(I64(2232805474518099604)),
            Nonnull(String("𐭁𐣬¹+<🕴'B7&Ó".to_owned())), Nonnull(U64(487956905190284356)), Nonnull(Unit),
            Nonnull(F32(-3.953167e-39)), Nullable(Unit, true), Nonnull(F32(-0.0)),
            Nullable(F64(2.754179169397046e291), true), Nonnull(Unit),
            Nullable(I64(-7367344613201638534), false), Nonnull(F32(-3.0101515e-7)),
            Nonnull(Bool(true)), Nonnull(Unit), Nullable(U16(42881), true),
            Nullable(F32(1.9122925e-36), true), Nullable(I32(358538438), true),
            Nonnull(U64(11955347133353482063)), Nullable(Bool(false), false),
            Nonnull(I64(6482597537832823096)), Nonnull(I16(4496)), Nonnull(U64(3550115232309980997)),
            Nonnull(U32(2261946021)), Nullable(Unit, false), Nonnull(I16(26910)),
            Nullable(I16(-19084), false), Nullable(U32(2936118268), false), Nullable(I16(-30350), true),
            Nonnull(U16(43532)), Nullable(I64(-785993172122454184), true), Nullable(U16(43117), true),
            Nonnull(I16(4328)), Nonnull(U16(43791)), Nonnull(F32(-0.0)), Nullable(U32(1389196361), true),
            Nullable(Unit, false), Nonnull(I32(-1048961965)), Nonnull(I32(-1959357799)), Nullable(Unit, false),
            Nonnull(U16(36978)), Nullable(F64(1.354132361276038e-13), true), Nonnull(U32(3885263228)),
            Nullable(F32(-22546940000.0), true), Nullable(I64(-9195538540107804925), true),
            Nullable(String("𒒖വbȺ࿙డ:𑊍\"𐺰 ힼ ቘ<*ವ{𫟀�R\u{c55}�X𑃑?𑵔=$n~".to_owned()), true),
            Nonnull(String("÷.`&Ѩ𞁐\u{acd}𐳧kI𞟤ðਸ਼𖩎bf:𐞟ጟ".to_owned())), Nonnull(Unit), Nonnull(Unit),
            Nonnull(F64(-2.559548142729777e-169)), Nullable(I16(-15518), true), Nullable(I64(8983696900450479918), true),
            Nonnull(U64(11828502426741007917)), Nonnull(Unit), Nonnull(U64(2125243705778231595)), Nullable(U16(19333), true),
            Nullable(String("Z=¥*]bgÖ".to_owned()), true), Nullable(U32(4022893128), false),
        ],
        prop3 = [
            Nonnull(U32(3909529098)), Nonnull(U64(10605666329718850673)), Nonnull(I64(-2314311395266768928)),
            Nonnull(F64(-1.823927764512688e-308)), Nonnull(U64(2869405416292076595)), Nonnull(String("3\\%\"𐖹b.p{K𐌸𞹾".to_owned())),
            Nullable(String("f🝩%𖫁𐤿#ú🪢`&L&r𞹉.n𐖻{𑤕𞸤𖽺\\%<౷".to_owned()), true), Nonnull(U16(32779)), Nonnull(I16(-30881)),
            Nullable(Unit, true), Nullable(I32(2146881542), true), Nonnull(Unit), Nonnull(String("F`🕴M?Y_'Ê`>𐡓`ⶴ/`*¥𑽙ኾ$ඎ<y`𑊦ଭ:Ѩ�".to_owned())),
            Nullable(String("$6".to_owned()), true), Nullable(Unit, true), Nonnull(F32(272125960000000.0)), Nonnull(U16(56778)),
            Nonnull(String("I𐼻ßr$=\u{c04}�\u{b57}7\\ኼk䂒ௐ=🨩N𞹛Oîᯉ'C?\u{1e01c}".to_owned())), Nullable(Bool(true), true),
            Nullable(F32(5.305297e19), true), Nullable(Unit, true), Nonnull(U32(3654367315)), Nullable(I64(-7238850361520039905), false),
            Nullable(F32(-0.0), false), Nullable(String("ᝃ?𐋄ÖR2𞸻cഌ{j*h5G\\`F\u{c56}6᧘𞺣:?`e?*\u{ccd}=¥<".to_owned()), true),
            Nullable(Bool(false), false), Nullable(Bool(false), true), Nonnull(I16(-19542)), Nullable(I32(-1060354183), true),
            Nullable(U32(2418426440), true), Nullable(Unit, false), Nonnull(I64(144516911663848025)), Nullable(F32(237023740.0), false),
            Nullable(I64(-4647334442770949597), true), Nullable(Unit, true), Nonnull(Bool(true)), Nullable(I64(-8550750707006844420), true),
            Nullable(U64(5260375725616250552), false), Nonnull(Unit), Nullable(String("Ⱥ᠃D|<ৈT\u{1e08f}".to_owned()), false),
            Nonnull(I64(4295936019938857040)), Nonnull(String("ఖ𐄁<A𞹭kA𭌆𒑁)\"".to_owned())), Nonnull(U32(1896902802)), Nonnull(U32(3505270827)),
            Nonnull(U64(9940296529824199105)), Nonnull(I64(8170460105644749421)), Nonnull(F32(-7.9133356e-36)), Nullable(U32(4086615590), false),
            Nonnull(I16(-31848)), Nullable(I16(-6716), false), Nonnull(I16(5356)), Nonnull(String("^9\"﹩ꬉAਲA.*𐕛៸/௸\"%Ⱥ𐮩𖩥,=0\u{b43}*".to_owned())),
            Nonnull(U16(64272)), Nonnull(I64(-6402860959690993697)), Nonnull(String("<🢡?ᜨ8²%R𑵤Uໄ<Ä=".to_owned())), Nullable(U64(6535961236791020282), true),
            Nonnull(F64(-4.073033910186329e-232)), Nonnull(F64(3.544570087127284e230)), Nonnull(Unit), Nonnull(U16(58524)),
            Nonnull(String("ⳃ?h¥&p`Ê\"d𑌲\"D𐨕Ꞃ?*%&🕴$𐂦ዄ🕴𐰃w\"\"Ѩ'd.".to_owned())), Nullable(I32(1233491127), true), Nonnull(F32(-1.4334434e-9)),
            Nullable(F32(8.5936e-41), false), Nonnull(I16(27159)), Nonnull(F64(4.162877932852521e52)), Nullable(Unit, true), Nullable(Bool(true), true),
            Nonnull(I16(-31852)), Nonnull(U16(4098)), Nonnull(F32(6.021699e33)), Nullable(I64(8794997021803162938), true), Nonnull(F32(-7.201521e-39)),
            Nullable(Unit, true), Nullable(Unit, true), Nonnull(String("𞸹l\u{16f90}*\u{f84}<&/ௐ/0=:¥O០".to_owned())), Nonnull(String("𐴜T?*:ই{UH".to_owned())),
            Nullable(U16(43135), true), Nullable(I64(7543280535653552197), false), Nonnull(U32(1032940059)), Nonnull(F64(-4.036630120788224e-309)),
            Nonnull(F64(-2.4875724249312143e278)), Nullable(I64(3022650268638072993), true), Nonnull(I64(1464328889940382111)),
            Nonnull(I64(2999178767494140240)), Nullable(I64(6013727988339051187), true), Nonnull(U64(18310463897315494354)),
            Nonnull(U64(12850858043488381073)), Nullable(U64(14852611330729036951), false), Nonnull(U64(173283812513893613)),
            Nullable(I16(20711), false), Nonnull(Bool(true)), Nonnull(I64(-716704178666587603)), Nonnull(U64(8296114501989671217)),
            Nonnull(U16(40022)), Nullable(I64(6376951496006352246), true), Nonnull(F32(-8.540973e-39)), Nullable(String("\"*wⷎ".to_owned()), false),
            Nullable(F64(0.0), false),
        ],
        decimal = [Nullable(Decimal(rust_decimal::Decimal::from_str("-418972098951.06177358336234255").unwrap()), false)],
    }
}
