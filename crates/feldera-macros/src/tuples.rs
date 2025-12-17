use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{punctuated::Punctuated, Ident, Index, Token};

pub(super) fn declare_tuple_impl(tuple: TupleDef) -> TokenStream2 {
    let mut expanded = quote! {};

    let name = tuple.name;
    let elements = tuple.elements;
    let generics = elements.iter().map(|e| quote!(#e)).collect::<Vec<_>>();
    let fields = elements
        .iter()
        .enumerate()
        .map(|(idx, _e)| format_ident!("t{}", idx)) // Generate lowercase t0, t1, ...
        .collect::<Vec<_>>();
    let fields_of_other = elements
        .iter()
        .enumerate()
        .map(|(idx, _e)| format_ident!("other_t{}", idx)) // Generate lowercase t0, t1, ...
        .collect::<Vec<_>>();
    let archive_bounds = elements
        .iter()
        .map(|e| quote!(#e: rkyv::Archive, <#e as rkyv::Archive>::Archived: Ord,))
        .map(|e| e.to_string())
        .fold(String::new(), |a, b| format!("{} {}", a, b));
    let self_indexes = elements
        .iter()
        .enumerate()
        .map(|(idx, _e)| Index::from(idx))
        .collect::<Vec<_>>();
    let num_elements = elements.len();

    // Struct definition
    let struct_def = quote! {
        #[derive(
            Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd,
            derive_more::Neg,
            serde::Serialize, serde::Deserialize,
            size_of::SizeOf, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize
        )]
        #[archive_attr(
            derive(Ord, Eq, PartialEq, PartialOrd),
        )]
        #[archive(
            bound(archive = #archive_bounds)
        )]
        pub struct #name<#(#generics),*>( #(pub #generics),* );
    };

    // Constructor
    let constructor = quote! {
    impl<#(#generics),*> #name<#(#generics),*> {
        #[allow(clippy::too_many_arguments)]
        pub fn new(#(#fields: #generics),*) -> Self {
            Self(#(#fields),*)
        }
    }
    };

    // get_x and get_x_mut methods
    let mut getter_setter = quote! {};
    for (i, (self_idx, typ)) in self_indexes.iter().zip(generics.iter()).enumerate() {
        let getter_name = format_ident!("get_{}", i);
        let getter_mut_name = format_ident!("get_{}_mut", i);

        getter_setter.extend(quote! {
            impl<#(#generics),*> #name<#(#generics),*> {
                #[inline]
                pub fn #getter_name(&self) -> &#typ {
                    &self.#self_idx
                }

                #[inline]
                pub fn #getter_mut_name(&mut self) -> &mut #typ {
                    &mut self.#self_idx
                }
            }
        });
    }

    // Trait implementations
    let algebra_traits = quote! {
        impl<#(#generics),*, W> ::dbsp::algebra::MulByRef<W> for #name<#(#generics),*>
        where
            #(#generics: ::dbsp::algebra::MulByRef<W, Output = #generics>,)*
                W: ::dbsp::algebra::ZRingValue,
        {
            type Output = Self;
            fn mul_by_ref(&self, other: &W) -> Self::Output {
                let #name( #(#fields),*) = self;
                #name( #(#fields.mul_by_ref(other)),*)
            }
        }

        impl<#(#generics),*> ::dbsp::algebra::HasZero for #name<#(#generics),*>
        where
            #(#generics: ::dbsp::algebra::HasZero,)*
        {
            fn zero() -> Self {
                #name( #(#generics::zero()),*)
            }
            fn is_zero(&self) -> bool {
                let mut result = true;
                let #name( #(#fields),*) = self;
                #(result = result && #fields.is_zero();)*
                result
            }
        }

        impl<#(#generics),*> ::dbsp::algebra::AddByRef for #name<#(#generics),*>
        where
            #(#generics: ::dbsp::algebra::AddByRef,)*
        {
            fn add_by_ref(&self, other: &Self) -> Self {
                let #name( #(#fields),*) = self;
                let #name(  #(#fields_of_other),*) = other;
                #name( #(#fields.add_by_ref(#fields_of_other)),*)
            }
        }

        impl<#(#generics),*> ::dbsp::algebra::AddAssignByRef for #name<#(#generics),*>
        where
            #(#generics: ::dbsp::algebra::AddAssignByRef,)*
        {
            fn add_assign_by_ref(&mut self, other: &Self) {
                let #name( #(ref mut #fields),* ) = self;
                let #name( #(ref #fields_of_other),* ) = other;

                #(#fields.add_assign_by_ref(#fields_of_other);)*
            }
        }

        impl<#(#generics),*> ::dbsp::algebra::NegByRef for #name<#(#generics),*>
        where
            #(#generics: ::dbsp::algebra::NegByRef,)*
        {
            fn neg_by_ref(&self) -> Self {
                let #name( #(#fields),* ) = self;
                #name( #(#fields.neg_by_ref()),* )
            }
        }
    };

    let conversion_traits = quote! {
        impl<#(#generics),*> From<(#(#generics),*)> for #name<#(#generics),*> {
            fn from((#(#fields),*): (#(#generics),*)) -> Self {
                Self(#(#fields),*)
            }
        }

        impl<'a, #(#generics),*> Into<(#(&'a #generics),*,)> for &'a #name<#(#generics),*> {
            #[allow(clippy::from_over_into)]
            fn into(self) -> (#(&'a #generics),*,) {
                let #name( #(#fields),* ) = &self;
                (#(#fields),*,)
            }
        }

        impl<#(#generics),*> Into<(#(#generics),*,)> for #name<#(#generics),*> {
            #[allow(clippy::from_over_into)]
            fn into(self) -> (#(#generics),*,) {
                let #name( #(#fields),* ) = self;
                (#(#fields),*,)
            }
        }
    };

    let debug_impl = quote! {
        impl<#(#generics: core::fmt::Debug),*> core::fmt::Debug for #name<#(#generics),*> {
            fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
                let #name( #(#fields),* ) = self;
                f.debug_tuple("")
                    #( .field(&#fields) )*
                    .finish()
            }
        }
    };

    let num_entries_impl = quote! {
        impl<#(#generics),*> ::dbsp::NumEntries for #name<#(#generics),*>
        where
            #(#generics: ::dbsp::NumEntries,)*
        {
            const CONST_NUM_ENTRIES: Option<usize> = None;//Some(#num_elements);

            fn num_entries_shallow(&self) -> usize {
                #num_elements
            }

            fn num_entries_deep(&self) -> usize {
                let #name( #(#fields),* ) = self;
                0 #( + (#fields).num_entries_deep() )*
            }
        }
    };

    let copy_impl = quote! {
        impl<#(#generics: Copy),*> Copy for #name<#(#generics),*> {}
    };

    let not_an_option = quote! {
        impl<#(#generics),*> ::dbsp::utils::IsNone for #name<#(#generics),*> {
            fn is_none(&self) -> bool { false }
        }
    };

    let checkpoint_impl = quote! {
        impl<#(#generics),*> ::dbsp::circuit::checkpointer::Checkpoint for #name<#(#generics),*>
        where
            #name<#(#generics),*>: ::rkyv::Serialize<::dbsp::storage::file::Serializer>
                + ::dbsp::storage::file::Deserializable,
        {
            fn checkpoint(&self) -> Result<Vec<u8>, ::dbsp::Error> {
                let mut s = ::dbsp::storage::file::Serializer::default();
                let _offset = ::rkyv::ser::Serializer::serialize_value(&mut s, self).unwrap();
                let data = s.into_serializer().into_inner().into_vec();
                Ok(data)
            }

            fn restore(&mut self, data: &[u8]) -> Result<(), ::dbsp::Error> {
                *self = ::dbsp::trace::unaligned_deserialize(data);
                Ok(())
            }
        }
    };

    expanded.extend(quote! {
        #struct_def
        #constructor
        #getter_setter
        #algebra_traits
        #conversion_traits
        #num_entries_impl
        #debug_impl
        #copy_impl
        #checkpoint_impl
        #not_an_option
    });

    expanded
}

pub(super) struct TupleDef {
    name: Ident,
    elements: Punctuated<Ident, Token![,]>,
}

impl syn::parse::Parse for TupleDef {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;

        let generics: syn::Generics = input.parse()?; // Parse `<T1, T2, ...>`
        let elements = generics
            .params
            .into_iter()
            .filter_map(|param| {
                if let syn::GenericParam::Type(ty) = param {
                    Some(ty.ident)
                } else {
                    None
                }
            })
            .collect();

        Ok(Self { name, elements })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    /// A simple test to verify the code of Tup<> types, for manual invocation run this:
    ///
    /// `cargo test -p feldera-macros -- --nocapture`
    fn dump_tup1_expansion() {
        let tuple: TupleDef = syn::parse2(quote!(Tup1<T0>)).expect("Failed to parse TupleDef");
        let expanded = declare_tuple_impl(tuple);
        let parsed_file: syn::File = syn::parse2(expanded).expect("Failed to parse output");
        let formatted = prettyplease::unparse(&parsed_file);

        println!("{formatted}");

        assert!(formatted.contains("pub struct Tup1"));
    }
}
