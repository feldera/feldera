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
    let self_indexes = elements
        .iter()
        .enumerate()
        .map(|(idx, _e)| Index::from(idx))
        .collect::<Vec<_>>();
    let num_elements = elements.len();
    let bitmap_words = num_elements.div_ceil(64);
    let bitmap_word_indexes = (0..bitmap_words).map(Index::from).collect::<Vec<_>>();
    let field_ptr_name = format_ident!("{}FieldPtr", name);
    let archived_name = format_ident!("Archived{}", name);
    let resolver_name = format_ident!("{}Resolver", name);
    let archived_none_ptr_name =
        format_ident!("__{}_archived_none_ptr", name.to_string().to_lowercase());

    // Struct definition
    let struct_def = quote! {
        #[derive(
            Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd,
            derive_more::Neg,
            serde::Serialize, serde::Deserialize,
            size_of::SizeOf
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

    let get_methods = fields
        .iter()
        .enumerate()
        .zip(generics.iter())
        .map(|((idx, _field), ty)| {
            let get_name = format_ident!("get_t{}", idx);
            let idx_lit = Index::from(idx);
            quote! {
                #[inline]
                pub fn #get_name(&self) -> &::rkyv::Archived<#ty> {
                    if self.none_bit_set(#idx_lit) {
                        // SAFETY: The bitmap only marks `None` for types whose archived
                        // representation is `ArchivedOption<...>`; a zeroed archived
                        // option is valid for the `None` variant.
                        unsafe { &*#archived_none_ptr_name::<#ty>() }
                    } else {
                        let ptr_idx = self.ptr_index(#idx_lit);
                        debug_assert!(ptr_idx < self.ptrs.len());
                        // SAFETY: `ptrs[ptr_idx]` points at the archived field when the bit is clear.
                        unsafe {
                            &*self
                                .ptrs
                                .as_slice()
                                .get_unchecked(ptr_idx)
                                .as_ptr()
                                .cast::<::rkyv::Archived<#ty>>()
                        }
                    }
                }
            }
        });

    let eq_checks = fields.iter().enumerate().map(|(idx, _)| {
        let get_name = format_ident!("get_t{}", idx);
        quote!(self.#get_name() == other.#get_name())
    });

    let cmp_checks = fields.iter().enumerate().map(|(idx, _)| {
        let get_name = format_ident!("get_t{}", idx);
        quote! {
            let cmp = self.#get_name().cmp(other.#get_name());
            if cmp != core::cmp::Ordering::Equal {
                return cmp;
            }
        }
    });

    let serialize_fields =
        fields
            .iter()
            .enumerate()
            .zip(self_indexes.iter())
            .map(|((idx, _), self_idx)| {
                let word_idx = Index::from(idx / 64);
                let bit_idx = idx % 64;
                quote! {
                    if ::dbsp::utils::IsNone::is_none(&self.#self_idx) {
                        bitmap[#word_idx] |= 1u64 << #bit_idx;
                    } else {
                        let pos = serializer.serialize_value(&self.#self_idx)?;
                        ptrs[ptrs_len] = #field_ptr_name { pos };
                        ptrs_len += 1;
                    }
                }
            });

    let deserialize_fields =
        fields
            .iter()
            .enumerate()
            .zip(generics.iter())
            .map(|((idx, _), ty)| {
                let field_name = format_ident!("t{}", idx);
                let idx_lit = Index::from(idx);
                quote! {
                    let #field_name = if self.none_bit_set(#idx_lit) {
                        #ty::default()
                    } else {
                        let archived = unsafe {
                            &*self
                                .ptrs
                                .as_slice()
                                .get_unchecked(ptr_idx)
                                .as_ptr()
                                .cast::<::rkyv::Archived<#ty>>()
                        };
                        ptr_idx += 1;
                        archived.deserialize(deserializer)?
                    };
                }
            });

    let rkyv_impls = quote! {
        #[derive(Copy, Clone)]
        struct #field_ptr_name {
            pos: usize,
        }

        impl ::rkyv::Archive for #field_ptr_name {
            type Archived = ::rkyv::RawRelPtr;
            type Resolver = usize;

            #[inline]
            unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
                ::rkyv::RawRelPtr::emplace(pos, resolver, out);
            }
        }

        impl<S> ::rkyv::Serialize<S> for #field_ptr_name
        where
            S: ::rkyv::ser::Serializer + ?Sized,
        {
            #[inline]
            fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                Ok(self.pos)
            }
        }

        #[inline]
        unsafe fn #archived_none_ptr_name<T: ::rkyv::Archive>() -> *const ::rkyv::Archived<T> {
            static NONE: ::std::sync::OnceLock<usize> = ::std::sync::OnceLock::new();
            let ptr = *NONE.get_or_init(|| {
                // This keeps a per-type, properly aligned `Archived<T>` that we can
                // reuse when the bitmap indicates a `None` value.
                // SAFETY: This is only valid when `Archived<T>` is `ArchivedOption<...>`.
                let boxed: Box<::rkyv::Archived<T>> = Box::new(unsafe { ::core::mem::zeroed() });
                Box::into_raw(boxed) as usize
            });
            ptr as *const ::rkyv::Archived<T>
        }

        #[repr(C)]
        pub struct #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive,)*
        {
            // Bitmap layout: each bit marks a field that was `None` (per `IsNone`).
            // `ptrs` stores only the non-`None` values in field order; `ptr_index`
            // computes the offset by counting unset bits before each field.
            bitmap: [u64; #bitmap_words],
            ptrs: ::rkyv::vec::ArchivedVec<::rkyv::RawRelPtr>,
            _phantom: core::marker::PhantomData<fn() -> (#(#generics),*)>,
        }

        impl<#(#generics),*> #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive,)*
        {
            #[inline]
            fn none_bit_set(&self, idx: usize) -> bool {
                debug_assert!(idx < #num_elements);
                let word = idx / 64;
                let bit = idx % 64;
                (self.bitmap[word] & (1u64 << bit)) != 0
            }

            #[inline]
            fn ptr_index(&self, field_idx: usize) -> usize {
                debug_assert!(field_idx < #num_elements);
                let word = field_idx / 64;
                let bit = field_idx % 64;
                let mut idx = 0usize;
                let mut w = 0usize;
                while w < word {
                    idx += 64usize - (self.bitmap[w].count_ones() as usize);
                    w += 1;
                }
                let mask = if bit == 0 { 0 } else { (1u64 << bit) - 1 };
                let none_before = (self.bitmap[word] & mask).count_ones() as usize;
                idx + bit - none_before
            }

            #(#get_methods)*
        }

        impl<#(#generics),*> core::cmp::PartialEq for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive,)*
            #(::rkyv::Archived<#generics>: core::cmp::PartialEq,)*
        {
            #[inline]
            fn eq(&self, other: &Self) -> bool {
                true #(&& #eq_checks)*
            }
        }

        impl<#(#generics),*> core::cmp::Eq for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive,)*
            #(::rkyv::Archived<#generics>: core::cmp::Eq,)*
        {}

        impl<#(#generics),*> core::cmp::PartialOrd for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive,)*
            #(::rkyv::Archived<#generics>: core::cmp::Ord,)*
        {
            #[inline]
            fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<#(#generics),*> core::cmp::Ord for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive,)*
            #(::rkyv::Archived<#generics>: core::cmp::Ord,)*
        {
            #[inline]
            fn cmp(&self, other: &Self) -> core::cmp::Ordering {
                #(#cmp_checks)*
                core::cmp::Ordering::Equal
            }
        }

        pub struct #resolver_name {
            bitmap: [u64; #bitmap_words],
            ptrs_resolver: ::rkyv::vec::VecResolver,
            ptrs_len: usize,
        }

        impl<#(#generics),*> ::rkyv::Archive for #name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive,)*
        {
            type Archived = #archived_name<#(#generics),*>;
            type Resolver = #resolver_name;

            #[inline]
            unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
                let (fp, fo) = ::rkyv::out_field!(out.bitmap);
                let bitmap_out = fo.cast::<u64>();
                #(
                    u64::resolve(
                        &resolver.bitmap[#bitmap_word_indexes],
                        pos + fp + (#bitmap_word_indexes * ::core::mem::size_of::<u64>()),
                        (),
                        bitmap_out.add(#bitmap_word_indexes),
                    );
                )*

                let (fp, fo) = ::rkyv::out_field!(out.ptrs);
                let vec_pos = pos + fp;
                ::rkyv::vec::ArchivedVec::<::rkyv::RawRelPtr>::resolve_from_len(
                    resolver.ptrs_len,
                    vec_pos,
                    resolver.ptrs_resolver,
                    fo,
                );

                let (_fp, fo) = ::rkyv::out_field!(out._phantom);
                fo.write(core::marker::PhantomData);
            }
        }

        impl<S, #(#generics),*> ::rkyv::Serialize<S> for #name<#(#generics),*>
        where
            S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
            #(#generics: ::rkyv::Archive + ::rkyv::Serialize<S> + ::dbsp::utils::IsNone,)*
        {
            #[inline]
            fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                let mut bitmap = [0u64; #bitmap_words];
                let mut ptrs: [#field_ptr_name; #num_elements] =
                    [#field_ptr_name { pos: 0 }; #num_elements];
                let mut ptrs_len = 0usize;

                #(#serialize_fields)*

                let ptrs_resolver = ::rkyv::vec::ArchivedVec::<::rkyv::RawRelPtr>::serialize_from_slice(
                    &ptrs[..ptrs_len],
                    serializer,
                )?;

                Ok(#resolver_name {
                    bitmap,
                    ptrs_resolver,
                    ptrs_len,
                })
            }
        }

        impl<D, #(#generics),*> ::rkyv::Deserialize<#name<#(#generics),*>, D> for #archived_name<#(#generics),*>
        where
            D: ::rkyv::Fallible + ?Sized,
            #(#generics: ::rkyv::Archive + Default,)*
            #(::rkyv::Archived<#generics>: ::rkyv::Deserialize<#generics, D>,)*
        {
            #[inline]
            fn deserialize(&self, deserializer: &mut D) -> Result<#name<#(#generics),*>, D::Error> {
                let mut ptr_idx = 0usize;
                #(#deserialize_fields)*
                Ok(#name( #(#fields),* ))
            }
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
        #rkyv_impls
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
