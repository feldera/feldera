use crate::{thin_str::ThinStrRef, ThinStr};
use cranelift::{
    codegen::ir::{FuncRef, Function},
    prelude::{types, AbiParam, Signature as ClifSignature},
};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Linkage, Module};
use std::{
    cmp::Ordering,
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    mem::ManuallyDrop,
    ptr::drop_in_place,
    slice,
};

macro_rules! intrinsics {
    ($($intrinsic:ident = fn($($arg:tt),*) $(-> $ret:tt)?),+ $(,)?) => {
        #[derive(Debug, Clone)]
        pub struct Intrinsics {
            $(pub $intrinsic: FuncId,)+
        }

        impl Intrinsics {
            /// Register all intrinsics within the given [`JITModule`],
            /// returning the imported function's ids within the returned
            /// `Intrinsics`
            ///
            /// Should be proceeded by a call to [`Intrinsics::register()`]
            /// on the [`JITBuilder`] that the given [`JITModule`] came from
            pub fn new(module: &mut JITModule) -> Self {
                let ptr_type = module.isa().pointer_type();
                let call_conv = module.isa().default_call_conv();

                paste::paste! {
                    $(
                        let $intrinsic = module
                            .declare_function(
                                stringify!([<dataflow_jit_ $intrinsic>]),
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
            pub fn register(builder: &mut JITBuilder) {
                paste::paste! {
                    $(
                        // Ensure all functions have `extern "C"` abi
                        let _: unsafe extern "C" fn($(intrinsics!(@replace $arg _),)+) $(-> intrinsics!(@replace $ret _))?
                            = [<dataflow_jit_ $intrinsic>];

                        builder.symbol(
                            stringify!([<dataflow_jit_ $intrinsic>]),
                            [<dataflow_jit_ $intrinsic>] as *const u8,
                        );
                    )+
                }

            }

            pub fn import(&self) -> ImportIntrinsics {
                ImportIntrinsics::new(self)
            }
        }

        #[derive(Debug, Clone)]
        pub struct ImportIntrinsics {
            $($intrinsic: Result<FuncRef, FuncId>,)+
        }

        impl ImportIntrinsics {
            pub fn new(intrinsics: &Intrinsics) -> Self {
                Self {
                    $($intrinsic: Err(intrinsics.$intrinsic),)+
                }
            }

            $(
                pub fn $intrinsic(&mut self, module: &mut JITModule, func: &mut Function) -> FuncRef {
                    match self.$intrinsic {
                        Ok(func_ref) => func_ref,
                        Err(func_id) => {
                            let func_ref = module.declare_func_in_func(func_id, func);
                            self.$intrinsic = Ok(func_ref);
                            func_ref
                        }
                    }
                }
            )+
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
    u8_hash = fn(ptr, u8),
    i8_hash = fn(ptr, i8),
    u16_hash = fn(ptr, u16),
    i16_hash = fn(ptr, i16),
    u32_hash = fn(ptr, u32),
    i32_hash = fn(ptr, i32),
    u64_hash = fn(ptr, u64),
    i64_hash = fn(ptr, i64),
    string_hash = fn(ptr, ptr),
}

/// Returns `true` if `lhs` is equal to `rhs`
// FIXME: Technically this can unwind
extern "C" fn dataflow_jit_string_eq(lhs: ThinStrRef, rhs: ThinStrRef) -> bool {
    lhs == rhs
}

/// Returns `true` if `lhs` is less than `rhs`
// FIXME: Technically this can unwind
extern "C" fn dataflow_jit_string_lt(lhs: ThinStrRef, rhs: ThinStrRef) -> bool {
    lhs < rhs
}

/// Compares the given strings
// FIXME: Technically this can unwind
extern "C" fn dataflow_jit_string_cmp(lhs: ThinStrRef, rhs: ThinStrRef) -> Ordering {
    lhs.cmp(&rhs)
}

/// Clones a thin string
// FIXME: Technically this can unwind
extern "C" fn dataflow_jit_string_clone(string: ThinStrRef) -> ThinStr {
    string.to_owned()
}

/// Drops the given [`ThinStr`]
// FIXME: Technically this can unwind
unsafe extern "C" fn dataflow_jit_string_drop_in_place(mut string: ManuallyDrop<ThinStr>) {
    drop_in_place(&mut string);
}

unsafe extern "C" fn dataflow_jit_string_size_of_children(
    string: ThinStrRef,
    context: &mut size_of::Context,
) {
    string.owned_size_of_children(context);
}

unsafe extern "C" fn dataflow_jit_string_debug(
    string: ThinStrRef,
    fmt: *mut fmt::Formatter<'_>,
) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(string.as_str(), &mut *fmt).is_ok()
}

unsafe extern "C" fn dataflow_jit_str_debug(
    ptr: *const u8,
    length: usize,
    fmt: *mut fmt::Formatter<'_>,
) -> bool {
    debug_assert!(!ptr.is_null() && !fmt.is_null());

    let bytes = slice::from_raw_parts(ptr, length);
    let string = std::str::from_utf8_unchecked(bytes);
    (*fmt).write_str(string).is_ok()
}

unsafe extern "C" fn dataflow_jit_bool_debug(boolean: bool, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&boolean, &mut *fmt).is_ok()
}

unsafe extern "C" fn dataflow_jit_int_debug(int: i64, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&int, &mut *fmt).is_ok()
}

unsafe extern "C" fn dataflow_jit_uint_debug(uint: u64, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&uint, &mut *fmt).is_ok()
}

unsafe extern "C" fn dataflow_jit_f32_debug(float: f32, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&float, &mut *fmt).is_ok()
}

unsafe extern "C" fn dataflow_jit_f64_debug(double: f64, fmt: *mut fmt::Formatter<'_>) -> bool {
    debug_assert!(!fmt.is_null());
    Debug::fmt(&double, &mut *fmt).is_ok()
}

macro_rules! hash {
    ($($name:ident = $ty:ty),+ $(,)?) => {
        paste::paste! {
            $(
                unsafe extern "C" fn [<dataflow_jit_ $name _hash>](hasher: &mut &mut dyn Hasher, value: $ty) {
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
