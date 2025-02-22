use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote};
use syn::{parse_macro_input, punctuated::Punctuated, Ident, Index, Token};

/// Parses input of the form: `declare_tuple!(Tup1<T0>);`
/// and generates dbsp tuple structs.
#[proc_macro]
pub fn declare_tuple(input: TokenStream) -> TokenStream {
    let tuple = parse_macro_input!(input as TupleDef);
    //eprintln!("{}", tuple.name);

    let found_crate = crate_name("dbsp").expect("dbsp needs to be included in `Cargo.toml`");
    let dbsp_crate = match found_crate {
        FoundCrate::Itself => quote!(crate),
        FoundCrate::Name(name) => {
            let ident = Ident::new(&name, Span::call_site());
            quote!( #ident )
        }
    };

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
        .map(|(idx, _e)| Index::from(idx)) // +1 because we have a hash field as the first field
        .collect::<Vec<_>>();
    let num_elements = elements.len();

    // Struct definition
    let struct_def = quote! {
        #[derive(
            Default, Clone,
            serde::Serialize, serde::Deserialize,
            size_of::SizeOf, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize
        )]
        #[archive_attr(
            derive(Ord, Eq, PartialEq, PartialOrd),
        )]
        #[archive(
            bound(archive = #archive_bounds)
        )]
        pub struct #name<#(#generics),*>( #(#generics),*, u64);
    };

    // Add derive_more::Neg,

    // Constructor
    let constructor = quote! {
    impl<#(#generics),*> #name<#(#generics),*> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(#(#fields: #generics),*) -> Self
    where
        #(#generics: core::hash::Hash,)*
    {
                let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
                #(
                    #fields.hash(&mut hasher);
                )*
                let hash = hasher.digest();
                Self(#(#fields),*, hash)
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
        impl<#(#generics),*, W> #dbsp_crate::algebra::MulByRef<W> for #name<#(#generics),*>
        where
            #(#generics: #dbsp_crate::algebra::MulByRef<W, Output = #generics>,)*
            W: #dbsp_crate::algebra::ZRingValue,
        {
            type Output = Self;
            fn mul_by_ref(&self, other: &W) -> Self::Output {
                let #name( #(#fields),*, _hash) = self;
                #name( #(#fields.mul_by_ref(other)),*, 0x0)
            }
        }

        impl<#(#generics),*> #dbsp_crate::algebra::HasZero for #name<#(#generics),*>
        where
            #(#generics: #dbsp_crate::algebra::HasZero,)*
        {
            fn zero() -> Self {
                #name( #(#generics::zero()),*, 0x0)
            }
            fn is_zero(&self) -> bool {
                let mut result = true;
                let #name( #(#fields),*, _hash) = self;
                #(result = result && #fields.is_zero();)*
                result
            }
        }

        impl<#(#generics),*> #dbsp_crate::algebra::AddByRef for #name<#(#generics),*>
        where
            #(#generics: #dbsp_crate::algebra::AddByRef,)*
        {
            fn add_by_ref(&self, other: &Self) -> Self {
                let #name( #(#fields),*, _hash) = self;
                let #name(  #(#fields_of_other),*, _hash) = other;
                #name( #(#fields.add_by_ref(#fields_of_other)),*, 0x0)
            }
        }

        impl<#(#generics),*> #dbsp_crate::algebra::AddAssignByRef for #name<#(#generics),*>
        where
            #(#generics: #dbsp_crate::algebra::AddAssignByRef,)*
        {
            fn add_assign_by_ref(&mut self, other: &Self) {
                let #name( #(ref mut #fields),*, ref mut hash) = self;
                let #name( #(ref #fields_of_other),*, _hash) = other;
                *hash = 0x0;

                #(#fields.add_assign_by_ref(#fields_of_other);)*
            }
        }

        impl<#(#generics),*> #dbsp_crate::algebra::NegByRef for #name<#(#generics),*>
        where
            #(#generics: #dbsp_crate::algebra::NegByRef,)*
        {
            fn neg_by_ref(&self) -> Self {
                let #name( #(#fields),*, _hash) = self;
                #name( #(#fields.neg_by_ref()),*, 0x0 )
            }
        }
    };

    let conversion_traits = quote! {
        impl<#(#generics),*> From<(#(#generics),*)> for #name<#(#generics),*> {
            fn from((#(#fields),*): (#(#generics),*)) -> Self {
                Self(#(#fields),*, 0x0)
            }
        }

        impl<'a, #(#generics),*> Into<(#(&'a #generics),*,)> for &'a #name<#(#generics),*> {
            #[allow(clippy::from_over_into)]
            fn into(self) -> (#(&'a #generics),*,) {
                let #name( #(#fields),*, _hash) = &self;
                (#(#fields),*,)
            }
        }

        impl<#(#generics),*> Into<(#(#generics),*,)> for #name<#(#generics),*> {
            #[allow(clippy::from_over_into)]
            fn into(self) -> (#(#generics),*,) {
                let #name( #(#fields),*, _hash) = self;
                (#(#fields),*,)
            }
        }
    };

    let debug_impl = quote! {
        impl<#(#generics: core::fmt::Debug),*> core::fmt::Debug for #name<#(#generics),*> {
            fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
                let #name( #(#fields),*, _hash ) = self;
                f.debug_tuple(stringify!(#name))
                    #( .field(&#fields) )*
                    .finish()
            }
        }
    };

    let num_entries_impl = quote! {
        impl<#(#generics),*> #dbsp_crate::NumEntries for #name<#(#generics),*>
        where
            #(#generics: #dbsp_crate::NumEntries,)*
        {
            const CONST_NUM_ENTRIES: Option<usize> = None;//Some(#num_elements);

            fn num_entries_shallow(&self) -> usize {
                #num_elements
            }

            fn num_entries_deep(&self) -> usize {
                let #name( #(#fields),*, _hash ) = self;
                0 #( + (#fields).num_entries_deep() )*
            }
        }
    };

    let copy_impl = quote! {
        impl<#(#generics: Copy),*> Copy for #name<#(#generics),*> {}
    };

    let checkpoint_impl = quote! {
        impl<#(#generics),*> #dbsp_crate::circuit::checkpointer::Checkpoint for #name<#(#generics),*>
        where
            #name<#(#generics),*>: ::rkyv::Serialize<#dbsp_crate::storage::file::Serializer>
                + #dbsp_crate::storage::file::Deserializable,
        {
            fn checkpoint(&self) -> Result<Vec<u8>, #dbsp_crate::Error> {
                let mut s = #dbsp_crate::storage::file::Serializer::default();
                let _offset = ::rkyv::ser::Serializer::serialize_value(&mut s, self).unwrap();
                let data = s.into_serializer().into_inner().into_vec();
                Ok(data)
            }

            fn restore(&mut self, data: &[u8]) -> Result<(), #dbsp_crate::Error> {
                *self = #dbsp_crate::trace::unaligned_deserialize(data);
                Ok(())
            }
        }
    };

    let comparison_impls = quote! {
        impl<#(#generics: Eq),*> Eq for #name<#(#generics),*> {}

        impl<#(#generics: PartialEq),*> PartialEq for #name<#(#generics),*>
        {
            fn eq(&self, other: &Self) -> bool {
                let #name( #(#fields),*, hash) = self;
                let #name( #(#fields_of_other),*, other_hash) = other;

                if *hash != 0x0 && *other_hash != 0x0 && *hash != *other_hash {
                    return false;
                }
                else {
                    #(#fields == #fields_of_other) &&*
                }
            }
        }

        impl<#(#generics: PartialOrd),*> PartialOrd for #name<#(#generics),*>
        {
            fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
                let #name( #(#fields),*, _hash) = self;
                let #name( #(#fields_of_other),*, _hash) = other;

                #(
                    match #fields.partial_cmp(#fields_of_other) {
                        Some(core::cmp::Ordering::Equal) => (),
                        not_equal => return not_equal
                    };
                )*

                Some(core::cmp::Ordering::Equal)
            }
        }

        impl<#(#generics: Ord),*> Ord for #name<#(#generics),*> {
            fn cmp(&self, other: &Self) -> core::cmp::Ordering {
                let #name( #(#fields),*, _hash) = self;
                let #name( #(#fields_of_other),*, _hash) = other;

                #(
                    match #fields.cmp(#fields_of_other) {
                        core::cmp::Ordering::Equal => (),
                        not_equal => return not_equal
                    };
                )*

                core::cmp::Ordering::Equal
            }
        }

        impl<#(#generics: std::hash::Hash),*> std::hash::Hash for #name<#(#generics),*> {
            fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
                let #name( #(#fields),*, hash_code ) = self;
                assert!(*hash_code != 0x0, "Hash code not set for tuple");
                hash_code.hash(state);
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
        #comparison_impls
    });

    // Debug print
    let expanded_pretty = expanded.clone();
    let parsed_file: syn::File = syn::parse2(expanded_pretty).expect("Failed to parse");
    let formatted = prettyplease::unparse(&parsed_file);
    eprintln!("{}", formatted);

    expanded.into()
}

struct TupleDef {
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