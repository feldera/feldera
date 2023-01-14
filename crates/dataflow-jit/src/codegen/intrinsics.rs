use std::ptr::drop_in_place;

use crate::{thin_str::ThinStrRef, ThinStr};
use cranelift::prelude::{types, AbiParam, Signature as ClifSignature};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Linkage, Module};

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

                $(
                    let $intrinsic = module
                        .declare_function(stringify!($intrinsic), Linkage::Import, &{
                            let mut sig = ClifSignature::new(call_conv);
                            $(sig.params.push(AbiParam::new(intrinsics!(@type ptr_type $arg)));)*
                            $(sig.returns.push(AbiParam::new(intrinsics!(@type ptr_type $ret)));)?
                            sig
                        })
                        .unwrap();
                )+

                Self { $($intrinsic,)+ }
            }

            /// Registers all intrinsics within the given [`JITBuilder`]
            ///
            /// Should be called before [`Intrinsics::new()`]
            pub fn register(builder: &mut JITBuilder) {
                $(
                    builder.symbol(stringify!($intrinsic), $intrinsic as *const u8);
                )+
            }
        }
    };

    (@type $ptr_type:ident ptr) => { $ptr_type };
    (@type $ptr_type:ident bool) => { types::I8 };
}

intrinsics! {
    dataflow_jit_string_clone = fn(ptr) -> ptr,
    dataflow_jit_string_lt = fn(ptr, ptr) -> bool,
    dataflow_jit_string_drop_in_place = fn(ptr),
}

/// Clones a thin string
// FIXME: Technically this can unwind
extern "C" fn dataflow_jit_string_clone(string: ThinStrRef) -> ThinStr {
    string.to_owned()
}

/// Returns `true` if `lhs` is less than `rhs`
// FIXME: Technically this can unwind
extern "C" fn dataflow_jit_string_lt(lhs: ThinStrRef, rhs: ThinStrRef) -> bool {
    lhs < rhs
}

/// Drops the given [`ThinStr`]
// FIXME: Technically this can unwind
unsafe extern "C" fn dataflow_jit_string_drop_in_place(string: &mut ThinStr) {
    drop_in_place(string);
}
