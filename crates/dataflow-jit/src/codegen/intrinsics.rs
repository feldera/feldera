use crate::{
    codegen::{pretty_clif::CommentWriter, utils::FunctionBuilderExt, CodegenCtx, VTable},
    ir::{exprs::Call, ExprId},
    row::{Row, UninitRow},
    thin_str::ThinStrRef,
    ThinStr,
};
use chrono::{DateTime, Datelike, LocalResult, NaiveDate, TimeZone, Timelike, Utc};
use cranelift::{
    codegen::ir::{FuncRef, Function},
    prelude::{types, AbiParam, FunctionBuilder, Signature as ClifSignature},
};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Linkage, Module};
use std::{
    alloc::Layout,
    cell::RefCell,
    cmp::Ordering,
    collections::HashMap,
    fmt::{self, Debug, Write},
    hash::{Hash, Hasher},
    rc::Rc,
    slice, str,
};

macro_rules! intrinsics {
    ($($intrinsic:ident = $(($func_attr:ident))? fn($($arg:ident $(: $arg_attr:ident)? ),*) $(-> $ret:tt)?),+ $(,)?) => {
        const TOTAL_INTRINSICS: usize = [$(stringify!($intrinsic),)*].len();

        #[derive(Debug, Clone)]
        pub(crate) struct Intrinsics {
            intrinsics: HashMap<&'static str, FuncId>,
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

                let mut intrinsics = HashMap::with_capacity(TOTAL_INTRINSICS);

                $(
                    let $intrinsic = module
                        .declare_function(
                            stringify!($intrinsic),
                            Linkage::Import,
                            &{
                                let mut sig = ClifSignature::new(call_conv);
                                $(sig.params.push(AbiParam::new(intrinsics!(@clif_type ptr_type $arg)));)*
                                $(sig.returns.push(AbiParam::new(intrinsics!(@clif_type ptr_type $ret)));)?
                                sig
                            },
                        )
                        .unwrap();

                    let displaced = intrinsics.insert(stringify!($intrinsic), $intrinsic);
                    debug_assert_eq!(displaced, None, "duplicate intrinsic `{}`", stringify!($intrinsic));
                )+

                Self { intrinsics }
            }

            /// Registers all intrinsics within the given [`JITBuilder`]
            ///
            /// Should be called before [`Intrinsics::new()`]
            pub(crate) fn register(builder: &mut JITBuilder) {
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

            pub(crate) fn import(&self, comment_writer: Option<Rc<RefCell<CommentWriter>>>) -> ImportIntrinsics {
                ImportIntrinsics::new(self, comment_writer)
            }
        }

        /*
        pub fn intrinsics() -> HashMap<&'static str, Intrinsic> {
            let mut intrinsics = HashMap::with_capacity(TOTAL_INTRINSICS);

            $(
                let replaced = intrinsics.insert(
                    stringify!($intrinsic),
                    Intrinsic {
                        name: stringify!($intrinsic),
                        args: tiny_vec![[ArgType; 2] => $(ArgType::Scalar(intrinsics!(@type $arg)),)*],
                        arg_flags: tiny_vec![[ArgFlag; INLINE_FLAGS] => $(intrinsics!(@arg_flag $($arg_attr)?),)*],
                        ret: ArgType::Scalar(intrinsics!(@type $($ret)?)),
                        flags: intrinsics!(@func_flag $($func_attr)?),
                    },
                );

                debug_assert!(
                    replaced.is_none(),
                    concat!("duplicate intrinsic: ", stringify!($intrinsic)),
                );
            )*

            intrinsics
        }
        */
    };

    // (@func_flag pure) => { FunctionFlag::Pure };
    // (@func_flag effectful) => { FunctionFlag::Effectful };
    // (@func_flag) => { FunctionFlag::None };
    //
    // (@arg_flag consume) => { ArgFlag::Consume };
    // (@arg_flag mutable) => { ArgFlag::Mutable };
    // (@arg_flag immutable) => { ArgFlag::Immutable };
    // (@arg_flag) => { ArgFlag::Immutable };

    (@clif_type $ptr_type:ident ptr) => { $ptr_type };
    (@clif_type $ptr_type:ident usize) => { $ptr_type };
    (@clif_type $ptr_type:ident str) => { $ptr_type };
    (@clif_type $ptr_type:ident bool) => { types::I8 };
    (@clif_type $ptr_type:ident u8) => { types::I8 };
    (@clif_type $ptr_type:ident i8) => { types::I8 };
    (@clif_type $ptr_type:ident u16) => { types::I16 };
    (@clif_type $ptr_type:ident i16) => { types::I16 };
    (@clif_type $ptr_type:ident u32) => { types::I32 };
    (@clif_type $ptr_type:ident i32) => { types::I32 };
    (@clif_type $ptr_type:ident u64) => { types::I64 };
    (@clif_type $ptr_type:ident i64) => { types::I64 };
    (@clif_type $ptr_type:ident f32) => { types::F32 };
    (@clif_type $ptr_type:ident f64) => { types::F64 };
    (@clif_type $ptr_type:ident date) => { types::I32 };
    (@clif_type $ptr_type:ident timestamp) => { types::I64 };

    (@type) => { ColumnType::Unit };
    (@type ptr) => { ColumnType::Ptr };
    (@type usize) => { ColumnType::Usize };
    (@type str) => { ColumnType::String };
    (@type bool) => { ColumnType::Bool };
    (@type u8) => { ColumnType::U8 };
    (@type i8) => { ColumnType::I8 };
    (@type u16) => { ColumnType::U16 };
    (@type i16) => { ColumnType::I16 };
    (@type u32) => { ColumnType::U32 };
    (@type i32) => { ColumnType::I32 };
    (@type u64) => { ColumnType::U64 };
    (@type i64) => { ColumnType::I64 };
    (@type f32) => { ColumnType::F32 };
    (@type f64) => { ColumnType::F64 };
    (@type f32) => { ColumnType::F32 };
    (@type f64) => { ColumnType::F64 };
    (@type date) => { ColumnType::Date };
    (@type timestamp) => { ColumnType::Timestamp };

    (@replace $x:tt $y:tt) => { $y };
}

#[derive(Debug, Clone)]
pub(crate) struct ImportIntrinsics {
    intrinsics: HashMap<&'static str, Result<FuncRef, FuncId>>,
    comment_writer: Option<Rc<RefCell<CommentWriter>>>,
}

impl ImportIntrinsics {
    pub(crate) fn new(
        intrinsics: &Intrinsics,
        comment_writer: Option<Rc<RefCell<CommentWriter>>>,
    ) -> Self {
        Self {
            intrinsics: intrinsics
                .intrinsics
                .iter()
                .map(|(&name, &id)| (name, Err(id)))
                .collect(),
            comment_writer,
        }
    }

    pub fn get(&mut self, intrinsic: &str, module: &mut JITModule, func: &mut Function) -> FuncRef {
        match self
            .intrinsics
            .get_mut(intrinsic)
            .expect("got intrinsic that doesn't exist")
        {
            Ok(func_ref) => *func_ref,
            func_id => {
                let func_ref = module.declare_func_in_func(func_id.unwrap_err(), func);
                *func_id = Ok(func_ref);

                if let Some(writer) = self.comment_writer.as_deref() {
                    writer.borrow_mut().add_comment(func_ref, intrinsic);
                }

                func_ref
            }
        }
    }
}

/*
struct IntrinsicValidator {
    intrinsics: HashMap<&'static str, Intrinsic>,
}

impl IntrinsicValidator {
    pub fn new() -> Self {
        Self {
            intrinsics: intrinsics(),
        }
    }

    pub fn validate(
        &self,
        expr_id: ExprId,
        call: &Call,
        func: &mut FunctionValidator,
    ) -> ValidationResult {
        if let Some(intrinsic) = self.intrinsics.get(&call.function()) {
            if call.args().len() != intrinsic.args.len() {
                return Err(ValidationError::IncorrectFunctionArgLen {
                    expr_id,
                    function: call.function().to_owned(),
                    expected_args: intrinsic.args.len(),
                    args: call.args().len(),
                });
            }

            let actual_arg_types = call
                .args()
                .iter()
                .map(|&arg| {
                    Ok(match func.expr_type(arg)? {
                        Ok(scalar) => ArgType::Scalar(scalar),
                        Err(layout) => ArgType::Row(layout),
                    })
                })
                .collect::<ValidationResult<Vec<_>>>()?;
            assert_eq!(actual_arg_types, call.arg_types());

            if !actual_arg_types[0]
                .as_scalar()
                .map_or(false, ColumnType::is_unsigned_int)
            {
                todo!(
                    "mismatched argument type in {expr_id}, should be an unsigned integer but instead got {:?}",
                    actual_arg_types[0],
                );
            }

            func.expr_types
                .insert(expr_id, Ok(intrinsic.ret.as_scalar().unwrap()));

            Ok(())
        } else {
            Err(ValidationError::UnknownFunction {
                expr_id,
                function: call.function().to_owned(),
            })
        }
    }
}

const INLINE_FLAGS: usize = (usize::BITS as usize * 3) - 1;

#[derive(Debug, Clone, Copy, Default)]
pub enum ArgFlag {
    Consume,
    Mutable,
    #[default]
    Immutable,
}

impl ArgFlag {
    #[must_use]
    #[inline]
    pub const fn is_consume(&self) -> bool {
        matches!(self, Self::Consume)
    }

    #[must_use]
    #[inline]
    pub const fn is_mutable(&self) -> bool {
        matches!(self, Self::Mutable)
    }

    #[must_use]
    #[inline]
    pub const fn is_immutable(&self) -> bool {
        matches!(self, Self::Immutable)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum FunctionFlag {
    /// The function has no particular flags
    #[default]
    None,
    /// The function is pure
    Pure,
    /// The function has side effects
    Effectful,
}

impl FunctionFlag {
    /// Returns `true` if the function flags is [`Pure`].
    ///
    /// [`Pure`]: FunctionFlags::Pure
    #[must_use]
    #[inline]
    pub const fn is_pure(&self) -> bool {
        matches!(self, Self::Pure)
    }

    /// Returns `true` if the function flags is [`Effectful`].
    ///
    /// [`Effectful`]: FunctionFlags::Effectful
    #[must_use]
    #[inline]
    pub const fn is_effectful(&self) -> bool {
        matches!(self, Self::Effectful)
    }
}

#[derive(Debug, Clone)]
pub struct Intrinsic {
    pub name: &'static str,
    pub args: TinyVec<[ArgType; 2]>,
    pub arg_flags: TinyVec<[ArgFlag; INLINE_FLAGS]>,
    pub ret: ArgType,
    pub flags: FunctionFlag,
}

impl Intrinsic {
    pub fn mutables_arg(&self, arg: usize) -> bool {
        self.arg_flags[arg].is_mutable()
    }

    #[inline]
    pub const fn is_pure(&self) -> bool {
        self.flags.is_pure()
    }

    #[inline]
    pub const fn is_effectful(&self) -> bool {
        self.flags.is_effectful()
    }
}
*/

intrinsics! {
    alloc = fn(usize, usize) -> ptr,
    dealloc = fn(ptr: mutable, usize, usize),
    row_vec_push = fn(ptr: mutable, ptr, ptr: consume),

    // Debug functions
    string_debug = fn(str, ptr: mutable) -> bool,
    str_debug = fn(ptr, usize, ptr: mutable) -> bool,
    bool_debug = fn(bool, ptr: mutable) -> bool,
    i64_debug = fn(i64, ptr: mutable) -> bool,
    u64_debug = fn(u64, ptr: mutable) -> bool,
    f32_debug = fn(f32, ptr: mutable) -> bool,
    f64_debug = fn(f64, ptr: mutable) -> bool,
    date_debug = fn(date, ptr: mutable) -> bool,
    timestamp_debug = fn(timestamp, ptr: mutable) -> bool,

    // Hash functions
    u8_hash = fn(ptr: mutable, u8),
    i8_hash = fn(ptr: mutable, i8),
    u16_hash = fn(ptr: mutable, u16),
    i16_hash = fn(ptr: mutable, i16),
    u32_hash = fn(ptr: mutable, u32),
    i32_hash = fn(ptr: mutable, i32),
    u64_hash = fn(ptr: mutable, u64),
    i64_hash = fn(ptr: mutable, i64),
    string_hash = fn(ptr: mutable, str),

    // Write functions
    write_i8_to_string = fn(str: consume, i8) -> str,
    write_u8_to_string = fn(str: consume, u8) -> str,
    write_u16_to_string = fn(str: consume, u16) -> str,
    write_i16_to_string = fn(str: consume, i16) -> str,
    write_u32_to_string = fn(str: consume, u32) -> str,
    write_i32_to_string = fn(str: consume, i32) -> str,
    write_u64_to_string = fn(str: consume, u64) -> str,
    write_i64_to_string = fn(str: consume, i64) -> str,
    write_f32_to_string = fn(str: consume, f32) -> str,
    write_f64_to_string = fn(str: consume, f64) -> str,
    write_timestamp_to_string = fn(str: consume, timestamp) -> str,
    write_date_to_string = fn(str: consume, date) -> str,

    // String functions
    string_eq = fn(str, str) -> bool,
    string_lt = fn(str, str) -> bool,
    string_cmp = fn(str, str) -> i8,
    string_clone = fn(str) -> str,
    string_drop_in_place = fn(str: consume),
    string_size_of_children = fn(str, ptr),

    string_with_capacity = fn(usize) -> str,
    string_push_str = fn(str: consume, ptr, usize) -> str,
    string_push_str_variadic = fn(str: consume, ptr, usize) -> str,

    string_count_chars = fn(ptr, usize) -> usize,
    string_is_nfc = fn(ptr, usize) -> bool,
    string_is_nfd = fn(ptr, usize) -> bool,
    string_is_nfkc = fn(ptr, usize) -> bool,
    string_is_nfkd = fn(ptr, usize) -> bool,
    string_is_lowercase = fn(ptr, usize) -> bool,
    string_is_uppercase = fn(ptr, usize) -> bool,
    string_is_ascii = fn(ptr, usize) -> bool,

    // Timestamp functions
    // timestamp_year = fn(i64) -> i64,
    timestamp_month = fn(timestamp) -> i64,
    timestamp_day = fn(timestamp) -> i64,
    timestamp_quarter = fn(timestamp) -> i64,
    timestamp_decade = fn(timestamp) -> i64,
    timestamp_century = fn(timestamp) -> i64,
    timestamp_millennium = fn(timestamp) -> i64,
    timestamp_iso_year = fn(timestamp) -> i64,
    timestamp_week = fn(timestamp) -> i64,
    timestamp_day_of_week = fn(timestamp) -> i64,
    timestamp_iso_day_of_week = fn(timestamp) -> i64,
    timestamp_day_of_year = fn(timestamp) -> i64,
    timestamp_millisecond = fn(timestamp) -> i64,
    timestamp_microsecond = fn(timestamp) -> i64,
    timestamp_second = fn(timestamp) -> i64,
    timestamp_minute = fn(timestamp) -> i64,
    timestamp_hour = fn(timestamp) -> i64,
    timestamp_floor_week = fn(timestamp) -> i64,

    // Date functions
    date_year = fn(date) -> i32,
    date_month = fn(date) -> i32,
    date_day = fn(date) -> i32,
    date_quarter = fn(date) -> i32,
    date_decade = fn(date) -> i32,
    date_century = fn(date) -> i32,
    date_millennium = fn(date) -> i32,
    date_iso_year = fn(date) -> i32,
    date_week = fn(date) -> i32,
    date_day_of_week = fn(date) -> i32,
    date_iso_day_of_week = fn(date) -> i32,
    date_day_of_year = fn(date) -> i32,

    // Float functions
    fmod = fn(f64, f64) -> f64,
    fmodf = fn(f32, f32) -> f32,
    cos = fn(f64) -> f64,
    cosf = fn(f32) -> f32,
    cosh = fn(f64) -> f64,
    coshf = fn(f32) -> f32,
    acos = fn(f64) -> f64,
    acosf = fn(f32) -> f32,
    acosh = fn(f64) -> f64,
    acoshf = fn(f32) -> f32,
    sin = fn(f64) -> f64,
    sinf = fn(f32) -> f32,
    sinh = fn(f64) -> f64,
    sinhf = fn(f32) -> f32,
    asin = fn(f64) -> f64,
    asinf = fn(f32) -> f32,
    asinh = fn(f64) -> f64,
    asinhf = fn(f32) -> f32,
    tan = fn(f64) -> f64,
    tanf = fn(f32) -> f32,
    tanh = fn(f64) -> f64,
    tanhf = fn(f32) -> f32,
    atan = fn(f64) -> f64,
    atanf = fn(f32) -> f32,
    atanh = fn(f64) -> f64,
    atanhf = fn(f32) -> f32,
    log = fn(f64) -> f64,
    logf = fn(f32) -> f32,
    log2 = fn(f64) -> f64,
    log2f = fn(f32) -> f32,
    log10 = fn(f64) -> f64,
    log10f = fn(f32) -> f32,
    log1p = fn(f64) -> f64,
    log1pf = fn(f32) -> f32,
    sqrt = fn(f64) -> f64,
    sqrtf = fn(f32) -> f32,
    cbrt = fn(f64) -> f64,
    cbrtf = fn(f32) -> f32,
    tgamma = fn(f64) -> f64,
    tgammaf = fn(f32) -> f32,
    lgamma = fn(f64) -> f64,
    lgammaf = fn(f32) -> f32,
    exp = fn(f64) -> f64,
    expf = fn(f32) -> f32,
    exp2 = fn(f64) -> f64,
    exp2f = fn(f32) -> f32,
    exp10 = fn(f64) -> f64,
    exp10f = fn(f32) -> f32,
    expm1 = fn(f64) -> f64,
    expm1f = fn(f32) -> f32,
}

/// Allocates memory with the given size and alignment
///
/// Calls [`std::alloc::handle_alloc_error`] in the event that
/// an allocation fails
///
/// # Safety
///
/// See [`std::alloc::alloc`] and [`Layout::from_size_align_unchecked`]
unsafe extern "C" fn alloc(size: usize, align: usize) -> *mut u8 {
    let layout = unsafe { Layout::from_size_align_unchecked(size, align) };
    debug_assert!(Layout::from_size_align(size, align).is_ok());

    let ptr = unsafe { std::alloc::alloc(layout) };
    if ptr.is_null() {
        std::alloc::handle_alloc_error(layout);
    }

    ptr
}

/// Deallocates the given pointer with the given size and alignment
///
/// # Safety
///
/// See [`std::alloc::dealloc`] and [`Layout::from_size_align_unchecked`]
unsafe extern "C" fn dealloc(ptr: *mut u8, size: usize, align: usize) {
    let layout = unsafe { Layout::from_size_align_unchecked(size, align) };
    debug_assert!(Layout::from_size_align(size, align).is_ok());

    unsafe { std::alloc::dealloc(ptr, layout) }
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
unsafe extern "C" fn string_drop_in_place(string: ThinStr) {
    drop(string);
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

macro_rules! debug_primitives {
    ($($primitive:ident),+ $(,)?) => {
        paste::paste! {
            $(
                unsafe extern "C" fn [<$primitive _debug>](value: $primitive, fmt: *mut fmt::Formatter<'_>) -> bool {
                    debug_assert!(!fmt.is_null());
                    Debug::fmt(&value, &mut *fmt).is_ok()
                }
            )+
        }
    };
}

debug_primitives! {
    bool,
    i64,
    u64,
    f32,
    f64,
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

macro_rules! write_primitives {
    ($($primitive:ident),+ $(,)?) => {
        paste::paste! {
            $(
                unsafe extern "C" fn [<write_ $primitive _to_string>](mut string: ThinStr, value: $primitive) -> ThinStr {
                    write!(string, "{value}").unwrap();
                    string
                }
            )+
        }
    };
}

write_primitives! {
    i8,  u8,
    u16, i16,
    u32, i32,
    u64, i64,
    f32,
    f64,
}

unsafe extern "C" fn write_timestamp_to_string(mut string: ThinStr, millis: i64) -> ThinStr {
    if let LocalResult::Single(timestamp) = Utc.timestamp_millis_opt(millis) {
        if let Err(error) = write!(string, "{}", timestamp.format("%+")) {
            tracing::error!("error while writing timestamp {timestamp} to string: {error}");
        }
    } else {
        tracing::error!("failed to create timestamp from {millis} in write_timestamp_to_string");
    }

    string
}

unsafe extern "C" fn write_date_to_string(mut string: ThinStr, days: i32) -> ThinStr {
    if let LocalResult::Single(date) = Utc.timestamp_opt(days as i64 * 86400, 0) {
        if let Err(error) = write!(string, "{}", date.format("%Y-%m-%d")) {
            tracing::error!("error while writing date {days} to string: {error}");
        }
    } else {
        tracing::error!("failed to create date from {days} in write_date_to_string");
    }

    string
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

#[inline(always)]
unsafe fn str_from_raw_parts<'a>(ptr: *const u8, len: usize) -> &'a str {
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };
    debug_assert!(str::from_utf8(bytes).is_ok());
    unsafe { str::from_utf8_unchecked(bytes) }
}

unsafe extern "C" fn string_push_str(mut target: ThinStr, ptr: *const u8, len: usize) -> ThinStr {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    target.push_str(string);
    target
}

#[derive(Debug)]
#[repr(C)]
struct Slice {
    ptr: *const u8,
    length: usize,
}

unsafe extern "C" fn string_push_str_variadic(
    mut target: ThinStr,
    strings: *const Slice,
    len: usize,
) -> ThinStr {
    let strings = unsafe { slice::from_raw_parts(strings, len) };

    // Reserve the space we'll need up-front
    let required_capacity = strings.iter().map(|slice| slice.length).sum();
    target.reserve(required_capacity);

    // Push all strings to the vec
    for &Slice { ptr, length } in strings {
        let string = unsafe { str_from_raw_parts(ptr, length) };
        target.push_str(string);
    }

    target
}

unsafe extern "C" fn string_count_chars(ptr: *const u8, len: usize) -> usize {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    string.chars().count()
}

unsafe extern "C" fn string_is_nfc(ptr: *const u8, len: usize) -> bool {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    unicode_normalization::is_nfc(string)
}

unsafe extern "C" fn string_is_nfd(ptr: *const u8, len: usize) -> bool {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    unicode_normalization::is_nfd(string)
}

unsafe extern "C" fn string_is_nfkc(ptr: *const u8, len: usize) -> bool {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    unicode_normalization::is_nfkc(string)
}

unsafe extern "C" fn string_is_nfkd(ptr: *const u8, len: usize) -> bool {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    unicode_normalization::is_nfkd(string)
}

unsafe extern "C" fn string_is_lowercase(ptr: *const u8, len: usize) -> bool {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    string.chars().all(char::is_lowercase)
}

unsafe extern "C" fn string_is_uppercase(ptr: *const u8, len: usize) -> bool {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    string.chars().all(char::is_uppercase)
}

unsafe extern "C" fn string_is_ascii(ptr: *const u8, len: usize) -> bool {
    let string = unsafe { str_from_raw_parts(ptr, len) };
    string.is_ascii()
}

unsafe extern "C" fn fmod(lhs: f64, rhs: f64) -> f64 {
    libm::fmod(lhs, rhs)
}

unsafe extern "C" fn fmodf(lhs: f32, rhs: f32) -> f32 {
    libm::fmodf(lhs, rhs)
}

macro_rules! trig_intrinsics {
    ($($name:ident),+ $(,)?) => {
        paste::paste! {
            $(
                unsafe extern "C" fn $name(x: f64) -> f64 {
                    libm::$name(x)
                }

                unsafe extern "C" fn [<$name f>](x: f32) -> f32 {
                    libm::[<$name f>](x)
                }
            )+

            pub static TRIG_INTRINSICS: &[&str] = &[
                $(concat!("dbsp.math.", stringify!($name)),)+
            ];

            impl CodegenCtx<'_> {
                pub(super) fn trig_intrinsic(&mut self, expr_id: ExprId, call: &Call, function: &str, builder: &mut FunctionBuilder<'_>) {
                    match function {
                        $(
                            concat!("dbsp.math.", stringify!($name)) => {
                                let float = self.value(call.args()[0]);
                                let float_ty = builder.func.dfg.value_type(float);

                                let intrinsic = if float_ty == types::F32 {
                                    stringify!([<$name f>])
                                } else if float_ty == types::F64 {
                                    stringify!($name)
                                } else {
                                    unreachable!("called math function with {float_ty}, expected f32 or f64")
                                };
                                let intrinsic = self.imports.get(intrinsic, self.module, builder.func);

                                let result = builder.call_fn(intrinsic, &[float]);
                                self.add_expr(expr_id, result, call.arg_types()[0].as_scalar().unwrap(), None);
                            }
                        )+

                        _ => unreachable!(),
                    }
                }
            }
        }
    };
}

trig_intrinsics! {
    cos,
    cosh,
    acos,
    acosh,

    sin,
    sinh,
    asin,
    asinh,

    tan,
    tanh,
    atan,
    atanh,

    log,
    log2,
    log10,
    log1p,

    sqrt,
    cbrt,

    tgamma,
    lgamma,

    exp,
    exp2,
    exp10,
    expm1,
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
