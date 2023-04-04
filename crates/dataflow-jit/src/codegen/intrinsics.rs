use crate::{
    codegen::{pretty_clif::CommentWriter, VTable},
    row::{Row, UninitRow},
    thin_str::ThinStrRef,
    ThinStr,
};
use chrono::{DateTime, Datelike, LocalResult, NaiveDate, TimeZone, Timelike, Utc};
use cranelift::{
    codegen::ir::{FuncRef, Function},
    prelude::{types, AbiParam, Signature as ClifSignature},
};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Linkage, Module};
use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    mem::ManuallyDrop,
    ptr::drop_in_place,
    rc::Rc,
    slice,
};

macro_rules! intrinsics {
    ($($intrinsic:ident = fn($($arg:tt),*) $(-> $ret:tt)?),+ $(,)?) => {
        #[derive(Debug, Clone)]
        pub(crate) struct Intrinsics {
            $(pub $intrinsic: FuncId,)+
        }

        impl Intrinsics {
            /// Register all intrinsics within the given [`JITModule`],
            /// returning the imported function's ids within the returned
            /// `Intrinsics`
            ///
            /// Should be proceeded by a call to [`Intrinsics::register()`]
            /// on the [`JITBuilder`] that the given [`JITModule`] came from
            pub(crate) fn new(module: &mut JITModule) -> Self {
                let ptr_type = module.isa().pointer_type();
                let call_conv = module.isa().default_call_conv();

                paste::paste! {
                    $(
                        let $intrinsic = module
                            .declare_function(
                                stringify!($intrinsic),
                                Linkage::Import,
                                &{
                                    let mut sig = ClifSignature::new(call_conv);
                                    $(sig.params.push(AbiParam::new(intrinsics!(@type ptr_type $arg)));)*
                                    $(sig.returns.push(AbiParam::new(intrinsics!(@type ptr_type $ret)));)?
                                    sig
                                },
                            )
                            .unwrap();
                    )+
                }

                Self { $($intrinsic,)+ }
            }

            /// Registers all intrinsics within the given [`JITBuilder`]
            ///
            /// Should be called before [`Intrinsics::new()`]
            pub(crate) fn register(builder: &mut JITBuilder) {
                paste::paste! {
                    $(
                        // Ensure all functions have `extern "C"` abi
                        let _: unsafe extern "C" fn($(intrinsics!(@replace $arg _),)+) $(-> intrinsics!(@replace $ret _))?
                            = $intrinsic;

                        builder.symbol(
                            stringify!($intrinsic),
                            $intrinsic as *const u8,
                        );
                    )+
                }

            }

            pub(crate) fn import(&self, comment_writer: Option<Rc<RefCell<CommentWriter>>>) -> ImportIntrinsics {
                ImportIntrinsics::new(self, comment_writer)
            }
        }

        #[derive(Debug, Clone)]
        pub(crate) struct ImportIntrinsics {
            $($intrinsic: Result<FuncRef, FuncId>,)+
            comment_writer: Option<Rc<RefCell<CommentWriter>>>,
        }

        impl ImportIntrinsics {
            pub(crate) fn new(intrinsics: &Intrinsics, comment_writer: Option<Rc<RefCell<CommentWriter>>>) -> Self {
                Self {
                    $($intrinsic: Err(intrinsics.$intrinsic),)+
                    comment_writer,
                }
            }

            paste::paste! {
                $(
                    pub fn $intrinsic(&mut self, module: &mut JITModule, func: &mut Function) -> FuncRef {
                        match self.$intrinsic {
                            Ok(func_ref) => func_ref,
                            Err(func_id) => {
                                let func_ref = module.declare_func_in_func(func_id, func);
                                self.$intrinsic = Ok(func_ref);

                                if let Some(writer) = self.comment_writer.as_deref() {
                                    writer.borrow_mut().add_comment(
                                        func_ref,
                                        stringify!($intrinsic),
                                    );
                                }

                                func_ref
                            }
                        }
                    }
                )+
            }
        }
    };

    (@type $ptr_type:ident ptr) => { $ptr_type };
    (@type $ptr_type:ident bool) => { types::I8 };
    (@type $ptr_type:ident u8) => { types::I8 };
    (@type $ptr_type:ident i8) => { types::I8 };
    (@type $ptr_type:ident u16) => { types::I16 };
    (@type $ptr_type:ident i16) => { types::I16 };
    (@type $ptr_type:ident u32) => { types::I32 };
    (@type $ptr_type:ident i32) => { types::I32 };
    (@type $ptr_type:ident u64) => { types::I64 };
    (@type $ptr_type:ident i64) => { types::I64 };
    (@type $ptr_type:ident f32) => { types::F32 };
    (@type $ptr_type:ident f64) => { types::F64 };

    (@replace $x:tt $y:tt) => { $y };
}

intrinsics! {
    string_eq = fn(ptr, ptr) -> bool,
    string_lt = fn(ptr, ptr) -> bool,
    string_cmp = fn(ptr, ptr) -> i8,
    string_clone = fn(ptr) -> ptr,
    string_drop_in_place = fn(ptr),
    string_size_of_children = fn(ptr, ptr),
    string_debug = fn(ptr, ptr) -> bool,
    str_debug = fn(ptr, ptr, ptr) -> bool,
    bool_debug = fn(bool, ptr) -> bool,
    int_debug = fn(i64, ptr) -> bool,
    uint_debug = fn(u64, ptr) -> bool,
    f32_debug = fn(f32, ptr) -> bool,
    f64_debug = fn(f64, ptr) -> bool,
    date_debug = fn(i32, ptr) -> bool,
    timestamp_debug = fn(i64, ptr) -> bool,
    u8_hash = fn(ptr, u8),
    i8_hash = fn(ptr, i8),
    u16_hash = fn(ptr, u16),
    i16_hash = fn(ptr, i16),
    u32_hash = fn(ptr, u32),
    i32_hash = fn(ptr, i32),
    u64_hash = fn(ptr, u64),
    i64_hash = fn(ptr, i64),
    string_hash = fn(ptr, ptr),
    row_vec_push = fn(ptr, ptr, ptr),
    string_with_capacity = fn(ptr) -> ptr,

    // Timestamp functions
    // timestamp_year = fn(i64) -> i64,
    timestamp_month = fn(i64) -> i64,
    timestamp_day = fn(i64) -> i64,
    timestamp_quarter = fn(i64) -> i64,
    timestamp_decade = fn(i64) -> i64,
    timestamp_century = fn(i64) -> i64,
    timestamp_millennium = fn(i64) -> i64,
    timestamp_iso_year = fn(i64) -> i64,
    timestamp_week = fn(i64) -> i64,
    timestamp_day_of_week = fn(i64) -> i64,
    timestamp_iso_day_of_week = fn(i64) -> i64,
    timestamp_day_of_year = fn(i64) -> i64,
    timestamp_millisecond = fn(i64) -> i64,
    timestamp_microsecond = fn(i64) -> i64,
    timestamp_second = fn(i64) -> i64,
    timestamp_minute = fn(i64) -> i64,
    timestamp_hour = fn(i64) -> i64,
    timestamp_floor_week = fn(i64) -> i64,

    // Date functions
    date_year = fn(i32) -> i32,
    date_month = fn(i32) -> i32,
    date_day = fn(i32) -> i32,
    date_quarter = fn(i32) -> i32,
    date_decade = fn(i32) -> i32,
    date_century = fn(i32) -> i32,
    date_millennium = fn(i32) -> i32,
    date_iso_year = fn(i32) -> i32,
    date_week = fn(i32) -> i32,
    date_day_of_week = fn(i32) -> i32,
    date_iso_day_of_week = fn(i32) -> i32,
    date_day_of_year = fn(i32) -> i32,

    fmod = fn(f64, f64) -> f64,
    fmodf = fn(f32, f32) -> f32,
}

/// Returns `true` if `lhs` is equal to `rhs`
// FIXME: Technically this can unwind
extern "C" fn string_eq(lhs: ThinStrRef, rhs: ThinStrRef) -> bool {
    lhs == rhs
}

/// Returns `true` if `lhs` is less than `rhs`
// FIXME: Technically this can unwind
extern "C" fn string_lt(lhs: ThinStrRef, rhs: ThinStrRef) -> bool {
    lhs < rhs
}

/// Compares the given strings
// FIXME: Technically this can unwind
extern "C" fn string_cmp(lhs: ThinStrRef, rhs: ThinStrRef) -> Ordering {
    lhs.cmp(&rhs)
}

/// Clones a thin string
// FIXME: Technically this can unwind
extern "C" fn string_clone(string: ThinStrRef) -> ThinStr {
    string.to_owned()
}

/// Drops the given [`ThinStr`]
// FIXME: Technically this can unwind
unsafe extern "C" fn string_drop_in_place(mut string: ManuallyDrop<ThinStr>) {
    drop_in_place(&mut string);
}

unsafe extern "C" fn string_size_of_children(string: ThinStrRef, context: &mut size_of::Context) {
    string.owned_size_of_children(context);
}

unsafe extern "C" fn string_debug(string: ThinStrRef, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(string.as_str(), &mut *fmt).is_ok()
}

unsafe extern "C" fn str_debug(
    ptr: *const u8,
    length: usize,
    fmt: *mut fmt::Formatter<'_>,
) -> bool {
    debug_assert!(!ptr.is_null() && !fmt.is_null());

    let bytes = slice::from_raw_parts(ptr, length);
    let string = std::str::from_utf8_unchecked(bytes);
    (*fmt).write_str(string).is_ok()
}

unsafe extern "C" fn bool_debug(boolean: bool, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&boolean, &mut *fmt).is_ok()
}

unsafe extern "C" fn int_debug(int: i64, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&int, &mut *fmt).is_ok()
}

unsafe extern "C" fn uint_debug(uint: u64, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&uint, &mut *fmt).is_ok()
}

unsafe extern "C" fn f32_debug(float: f32, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&float, &mut *fmt).is_ok()
}

unsafe extern "C" fn f64_debug(double: f64, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&double, &mut *fmt).is_ok()
}

unsafe extern "C" fn date_debug(date: i32, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());

    // TODO: UTC?
    if let Some(date) = NaiveDate::from_num_days_from_ce_opt(date) {
        write!(&mut *fmt, "{}", date.format("%Y-%m-%d")).is_ok()
    } else {
        tracing::error!("failed to create date from {date}");
        false
    }
}

unsafe extern "C" fn timestamp_debug(timestamp: i64, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());

    if let LocalResult::Single(timestamp) = Utc.timestamp_millis_opt(timestamp) {
        write!(&mut *fmt, "{}", timestamp.format("%+")).is_ok()
    } else {
        tracing::error!("failed to create timestamp from {timestamp}");
        false
    }
}

unsafe extern "C" fn row_vec_push(vec: &mut Vec<Row>, vtable: &'static VTable, row: *mut u8) {
    let mut uninit = UninitRow::new(vtable);
    unsafe {
        uninit
            .as_mut_ptr()
            .copy_from_nonoverlapping(row, vtable.size_of);
    }

    let row = unsafe { uninit.assume_init() };
    vec.push(row);
}

unsafe extern "C" fn string_with_capacity(capacity: usize) -> ThinStr {
    ThinStr::with_capacity(capacity)
}

unsafe extern "C" fn fmod(lhs: f64, rhs: f64) -> f64 {
    libm::fmod(lhs, rhs)
}

unsafe extern "C" fn fmodf(lhs: f32, rhs: f32) -> f32 {
    libm::fmodf(lhs, rhs)
}

macro_rules! timestamp_intrinsics {
    ($($name:ident => $expr:expr),+ $(,)?) => {
        paste::paste! {
            $(
                unsafe extern "C" fn [<timestamp_ $name>](millis: i64) -> i64 {
                    if let LocalResult::Single(timestamp) = Utc.timestamp_millis_opt(millis) {
                        let expr: fn(DateTime<Utc>) -> i64 = $expr;
                        expr(timestamp)
                    } else {
                        tracing::error!(
                            "failed to create timestamp from {millis} in {}",
                            stringify!([<timestamp_ $name>]),
                        );
                        0
                    }
                }
            )+
        }
    }
}

// TODO: Some of these really return u32 or i32
timestamp_intrinsics! {
    millennium => |time| ((time.year() + 999) / 1000) as i64,
    century => |time| ((time.year() + 99) / 100) as i64,
    decade => |time| (time.year() / 10) as i64,
    // year => |time| time.year() as i64,
    iso_year => |time| time.iso_week().year() as i64,
    quarter => |time| (time.month0() / 3 + 1) as i64,
    month => |time| time.month() as i64,
    week => |time| time.iso_week().week() as i64,
    day => |time| time.day() as i64,
    hour => |time| time.hour() as i64,
    minute => |time| time.minute() as i64,
    second => |time| time.second() as i64,
    millisecond => |time| (time.second() * 1000 + time.timestamp_subsec_millis()) as i64,
    microsecond => |time| (time.second() * 1_000_000 + time.timestamp_subsec_micros()) as i64,
    day_of_week => |time| (time.weekday().num_days_from_sunday() as i64) + 1,
    iso_day_of_week => |time| (time.weekday().num_days_from_monday() as i64) + 1,
    day_of_year => |time| time.ordinal() as i64,
    floor_week => |time| {
        let no_time = time
            .with_hour(0).unwrap()
            .with_minute(0).unwrap()
            .with_second(0).unwrap()
            .with_nanosecond(0).unwrap();
        let weekday = time.weekday().num_days_from_sunday() as i64;
        no_time.timestamp_millis() - (weekday * 86400 * 1000)
    }
}

macro_rules! date_intrinsics {
    ($($name:ident => $expr:expr),+ $(,)?) => {
        paste::paste! {
            $(
                unsafe extern "C" fn [<date_ $name>](days: i32) -> i32 {
                    if let LocalResult::Single(date) = Utc.timestamp_opt(days as i64 * 86400, 0) {
                        let expr: fn(DateTime<Utc>) -> i32 = $expr;
                        expr(date)
                    } else {
                        tracing::error!(
                            "failed to create date from {days} in {}",
                            stringify!([<date_ $name>]),
                        );
                        0
                    }
                }
            )+
        }
    }
}

// TODO: Some of these really return u32s
date_intrinsics! {
    year => |date| date.year(),
    month => |date| date.month() as i32,
    day => |date| date.day() as i32,
    quarter => |date| date.month0() as i32 / 3 + 1,
    decade => |date| date.year() / 10,
    century => |date| (date.year() + 99) / 100,
    millennium => |date| (date.year() + 999) / 1000,
    iso_year => |date| date.iso_week().year(),
    week => |date| date.iso_week().week() as i32,
    day_of_week => |date| date.weekday().num_days_from_sunday() as i32 + 1,
    iso_day_of_week => |date| date.weekday().num_days_from_monday() as i32 + 1,
    day_of_year => |date| date.ordinal() as i32,
}

macro_rules! hash {
    ($($name:ident = $ty:ty),+ $(,)?) => {
        paste::paste! {
            $(
                unsafe extern "C" fn [< $name _hash>](hasher: &mut &mut dyn Hasher, value: $ty) {
                    value.hash(hasher);
                }
            )+
        }
    };
}

hash! {
    u8 = u8,
    i8 = i8,
    u16 = u16,
    i16 = i16,
    u32 = u32,
    i32 = i32,
    u64 = u64,
    i64 = i64,
    string = ThinStrRef,
}
