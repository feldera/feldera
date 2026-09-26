//! Procedural macros for Feldera tuple types and utility traits.
//!
//! The `declare_tuple!` macro decides which layout to use based on tuple size
//! and the active storage format rules.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

mod ord_repr;
mod tuples;

/// Parses input of the form: `declare_tuple!(Tup1<T0>);`
/// and generates dbsp tuple structs.
#[proc_macro]
pub fn declare_tuple(input: TokenStream) -> TokenStream {
    let tuple = parse_macro_input!(input as tuples::TupleDef);
    let expanded = tuples::declare_tuple_impl(tuple);
    if std::env::var_os("FELDERA_DEV_MACROS_DUMP").is_some() {
        let parsed_file: syn::File = syn::parse2(expanded.clone()).expect("Failed to parse output");
        let formatted = prettyplease::unparse(&parsed_file);
        eprintln!("{}", formatted);
    }

    expanded.into()
}

/// Derives `OrdRepr` for the rkyv archived form of a struct or enum, so
/// that `ArchivedFoo` can be ordered against a `Foo` without deserializing.
/// It applies to types whose archived form comes from `#[derive(Archive)]`,
/// and therefore is named `ArchivedFoo` and mirrors `Foo`'s fields.
///
/// The generated comparison follows `#[derive(Ord)]`: struct fields compare
/// in declaration order, and enum variants compare by declaration position
/// before their payloads. It therefore agrees with `Ord` on the original
/// whenever `Ord` is derived. A type with a hand-written `Ord` must
/// hand-write `OrdRepr` to match it, and so must an enum with an explicit
/// discriminant, which `#[derive(Ord)]` orders by discriminant value rather
/// than by position; the derive rejects such an enum.
///
/// Every field type `T` gets the bound `<T as Archive>::Archived: OrdRepr<T>`,
/// in the same way that rkyv's derive bounds field types. A field marked
/// `#[omit_bounds]` (rkyv's attribute for recursive fields) contributes no
/// bound, which is what keeps a recursive type from requiring itself.
#[proc_macro_derive(OrdRepr)]
pub fn derive_ord_repr(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match ord_repr::derive_ord_repr_impl(input) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_derive(IsNone)]
pub fn derive_not_none(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics ::dbsp::utils::IsNone for #ident #ty_generics #where_clause {
            type Inner = Self;

            #[inline]
            fn is_none(&self) -> bool {
                false
            }

            #[inline]
            fn unwrap_or_self(&self) -> &Self::Inner {
                self
            }

            #[inline]
            fn from_inner(inner: Self::Inner) -> Self {
                inner
            }
        }

        impl #impl_generics ::dbsp::utils::SupportsRoaring for #ident #ty_generics #where_clause {}
    };

    TokenStream::from(expanded)
}
