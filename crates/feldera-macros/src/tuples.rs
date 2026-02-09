//! Tuple code generation for Feldera storage formats.
//!
//! We support three tuple layouts:
//! - Legacy/default rkyv layout used in storage v3.
//! - Storage v4 dense layout (bitmap + inner values).
//! - Storage v4 sparse layout (bitmap + offsets to present fields).
//!
//! For small tuples we keep the legacy layout. Larger tuples use the v4
//! optimized dense/sparse format chosen at serialize time.

use proc_macro2::{Literal, TokenStream as TokenStream2};
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
    // Use the default rkyv format for Tup's that are small.
    //
    // The v4 format adds some overhead (to distinguish sparse/dense plus bitmaps)
    // which get amortized with larger tuples. For smaller tuples it seems often
    // better to just use the regular format.
    // (note that changing this is requires a new storage format version)
    let use_legacy = num_elements <= 8;
    let bitmap_bytes = num_elements.div_ceil(8);
    let field_ptr_name = format_ident!("{}FieldPtr", name);
    let bitmap_ty = quote!(::dbsp::utils::tuple::TupleBitmap<#bitmap_bytes>);
    let bitmap_new = quote!(::dbsp::utils::tuple::TupleBitmap::<#bitmap_bytes>::new());
    let format_ty = quote!(::dbsp::utils::tuple::TupleFormat);
    let archived_name = format_ident!("Archived{}", name);
    let archived_sparse_name = format_ident!("Archived{}Sparse", name);
    let archived_dense_name = format_ident!("Archived{}Dense", name);
    let resolver_name = format_ident!("{}Resolver", name);
    let sparse_data_name = format_ident!("{}SparseData", name);
    let sparse_resolver_name = format_ident!("{}SparseResolver", name);
    let dense_data_name = format_ident!("{}DenseData", name);
    let dense_resolver_name = format_ident!("{}DenseResolver", name);
    // Struct definition
    let struct_def = if use_legacy {
        quote! {
            #[derive(
                Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd,
                derive_more::Neg,
                serde::Serialize, serde::Deserialize,
                size_of::SizeOf,
                rkyv::Archive, rkyv::Serialize, rkyv::Deserialize
            )]
            pub struct #name<#(#generics),*>( #(pub #generics),* );
        }
    } else {
        quote! {
            #[derive(
                Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd,
                derive_more::Neg,
                serde::Serialize, serde::Deserialize,
                size_of::SizeOf
            )]
            pub struct #name<#(#generics),*>( #(pub #generics),* );
        }
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

    let num_elements_const = quote! {
        impl<#(#generics),*> #name<#(#generics),*> {
            pub const NUM_ELEMENTS: usize = #num_elements;
        }
    };

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
                Self::NUM_ELEMENTS
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
            type Inner = Self;

            fn is_none(&self) -> bool { false }

            fn unwrap_or_self(&self) -> &Self::Inner { self }

            fn from_inner(inner: Self::Inner) -> Self { inner }
        }
    };

    let sparse_get_methods = fields
        .iter()
        .enumerate()
        .zip(generics.iter())
        .map(|((idx, _field), ty)| {
            let get_name = format_ident!("get_t{}", idx);
            let idx_lit = Index::from(idx);
            quote! {
                    #[inline]
                    pub fn #get_name(
                        &self,
                    ) -> Option<&::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>> {
                        if self.none_bit_set(#idx_lit) {
                            None
                        } else {
                            let ptr_idx = self.idx_for_field(#idx_lit);
                            debug_assert!(ptr_idx < self.ptrs.len());
                            // SAFETY: `ptrs[ptr_idx]` points at the archived field when the bit is clear.
                            Some(unsafe {
                                &*self
                                    .ptrs
                                    .as_slice()
                                    .get_unchecked(ptr_idx)
                                    .as_ptr()
                                    .cast::<::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>>()
                            })
                        }
                }
            }
        });

    let dense_get_methods =
        fields
            .iter()
            .enumerate()
            .zip(generics.iter())
            .map(|((idx, _field), ty)| {
                let get_name = format_ident!("get_t{}", idx);
                let idx_lit = Index::from(idx);
                let field = format_ident!("t{}", idx);
                quote! {
                    #[inline]
                    pub fn #get_name(
                        &self,
                    ) -> Option<&::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>> {
                        if self.none_bit_set(#idx_lit) {
                            None
                        } else {
                            Some(unsafe { &*self.#field.as_ptr() })
                        }
                    }
                }
            });

    let archived_get_methods =
        fields
            .iter()
            .enumerate()
            .zip(generics.iter())
            .map(|((idx, _field), ty)| {
                let get_name = format_ident!("get_t{}", idx);
                quote! {
                    #[inline]
                    pub fn #get_name(
                        &self,
                    ) -> Option<&::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>> {
                        match self.format {
                            #format_ty::Sparse => self.sparse().#get_name(),
                            #format_ty::Dense => self.dense().#get_name(),
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
            let cmp = self.#get_name().cmp(&other.#get_name());
            if cmp != core::cmp::Ordering::Equal {
                return cmp;
            }
        }
    });

    let legacy_eq_checks = self_indexes
        .iter()
        .map(|idx| quote!(self.#idx == other.#idx));

    let legacy_cmp_checks = self_indexes.iter().map(|idx| {
        quote! {
            let cmp = self.#idx.cmp(&other.#idx);
            if cmp != core::cmp::Ordering::Equal {
                return cmp;
            }
        }
    });

    let legacy_archived_ord_impls = quote! {
        impl<#(#generics),*> core::cmp::PartialEq for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive,)*
            #(::rkyv::Archived<#generics>: core::cmp::PartialEq,)*
        {
            #[inline]
            fn eq(&self, other: &Self) -> bool {
                true #(&& #legacy_eq_checks)*
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
                #(#legacy_cmp_checks)*
                core::cmp::Ordering::Equal
            }
        }
    };

    let choose_format_fields =
        fields
            .iter()
            .enumerate()
            .zip(self_indexes.iter())
            .map(|((idx, _), self_idx)| {
                let idx_lit = Index::from(idx);
                quote! {
                    if ::dbsp::utils::IsNone::is_none(&self.#self_idx) {
                        bitmap.set_none(#idx_lit);
                    }
                }
            });

    let sparse_serialize_fields =
        fields
            .iter()
            .enumerate()
            .zip(self_indexes.iter())
            .map(|((idx, _), self_idx)| {
                let idx_lit = Index::from(idx);
                quote! {
                    if !self.bitmap.is_none(#idx_lit) {
                        let pos = serializer.serialize_value(
                            ::dbsp::utils::IsNone::unwrap_or_self(&self.value.#self_idx),
                        )?;
                        ptrs[ptrs_len] = #field_ptr_name { pos };
                        ptrs_len += 1;
                    }
                }
            });

    let dense_serialize_fields =
        fields
            .iter()
            .enumerate()
            .zip(self_indexes.iter())
            .map(|((idx, _), self_idx)| {
                let idx_lit = Index::from(idx);
                let field_name = format_ident!("t{}", idx);
                quote! {
                    let #field_name = if self.bitmap.is_none(#idx_lit) {
                        None
                    } else {
                        Some(::rkyv::Serialize::serialize(
                            ::dbsp::utils::IsNone::unwrap_or_self(&self.value.#self_idx),
                            serializer,
                        )?)
                    };
                }
            });

    let dense_resolve_fields =
        fields
            .iter()
            .enumerate()
            .zip(self_indexes.iter())
            .zip(generics.iter())
            .map(|(((idx, _), self_idx), ty)| {
                let field_name = format_ident!("t{}", idx);
                let field_out = format_ident!("{}_out", field_name);
                quote! {
                    let (fp, fo) = ::rkyv::out_field!(out.#field_name);
                    let #field_out = fo
                        .cast::<core::mem::MaybeUninit<::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>>>();
                    if let Some(resolver) = resolver.#field_name {
                        let inner = ::dbsp::utils::IsNone::unwrap_or_self(&self.value.#self_idx);
                        <<#ty as ::dbsp::utils::IsNone>::Inner as ::rkyv::Archive>::resolve(
                            inner,
                            pos + fp,
                            resolver,
                            #field_out
                                .cast::<::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>>(),
                        );
                    } else {
                        core::ptr::write(#field_out, core::mem::MaybeUninit::zeroed());
                    }
                }
            });

    let sparse_deserialize_fields =
        fields
            .iter()
            .enumerate()
            .zip(generics.iter())
            .map(|((idx, _), ty)| {
                let field_name = format_ident!("t{}", idx);
                let idx_lit = Index::from(idx);
                quote! {
                    let #field_name = if sparse.none_bit_set(#idx_lit) {
                        #ty::default()
                    } else {
                        let archived: &::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner> = unsafe {
                            &*sparse
                                .ptrs
                                .as_slice()
                                .get_unchecked(ptr_idx)
                                .as_ptr()
                                .cast::<::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>>()
                        };
                        ptr_idx += 1;
                        let inner = archived.deserialize(deserializer)?;
                        <#ty as ::dbsp::utils::IsNone>::from_inner(inner)
                    };
                }
            });

    let dense_deserialize_fields =
        fields
            .iter()
            .enumerate()
            .zip(generics.iter())
            .map(|((idx, _), ty)| {
                let field_name = format_ident!("t{}", idx);
                let idx_lit = Index::from(idx);
                let get_name = format_ident!("get_t{}", idx);
                let expect_msg =
                    Literal::string(&format!("{}: missing field {}", archived_dense_name, idx));
                quote! {
                    let #field_name = if dense.none_bit_set(#idx_lit) {
                        #ty::default()
                    } else {
                        let archived = dense.#get_name().expect(#expect_msg);
                        let inner = archived.deserialize(deserializer)?;
                        <#ty as ::dbsp::utils::IsNone>::from_inner(inner)
                    };
                }
            });

    let dense_resolver_fields =
        fields
            .iter()
            .enumerate()
            .zip(generics.iter())
            .map(|((idx, _), ty)| {
                let field_name = format_ident!("t{}", idx);
                quote! {
                    #field_name: Option<<<#ty as ::dbsp::utils::IsNone>::Inner as ::rkyv::Archive>::Resolver>,
                }
            });

    let dense_resolver_inits = fields.iter().enumerate().map(|(idx, _)| {
        let field_name = format_ident!("t{}", idx);
        quote!(#field_name)
    });

    let choose_format_impl = quote! {
        impl<#(#generics),*> #name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            #[inline]
            fn choose_format(&self) -> (#bitmap_ty, #format_ty) {
                let mut bitmap = #bitmap_new;
                #(#choose_format_fields)*

                let none_bits = bitmap.count_none(Self::NUM_ELEMENTS);
                // Choose sparse format if 40% is None
                if none_bits * 10 >= Self::NUM_ELEMENTS * 4 {
                    (bitmap, #format_ty::Sparse)
                } else {
                    (bitmap, #format_ty::Dense)
                }
            }
        }
    };

    let rkyv_impls = quote! {
        #[derive(Copy, Clone)]
        struct #field_ptr_name {
            pos: usize,
        }

        impl ::rkyv::Archive for #field_ptr_name {
            type Archived = ::rkyv::rel_ptr::RawRelPtrI32;
            type Resolver = usize;

            #[inline]
            unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
                ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos, resolver, out);
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

        #[repr(C)]
        pub struct #archived_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            bitmap: #bitmap_ty,
            ptrs: ::rkyv::vec::ArchivedVec<::rkyv::rel_ptr::RawRelPtrI32>,
            _phantom: core::marker::PhantomData<fn() -> (#(#generics),*)>,
        }

        #[repr(C)]
        pub struct #archived_dense_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            bitmap: #bitmap_ty,
            #(
                #fields: core::mem::MaybeUninit<::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>>,
            )*
            _phantom: core::marker::PhantomData<fn() -> (#(#generics),*)>,
        }

        #[repr(C)]
        pub struct #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            format: #format_ty,
            data: ::rkyv::rel_ptr::RawRelPtrI32,
            _phantom: core::marker::PhantomData<fn() -> (#(#generics),*)>,
        }

        impl<#(#generics),*> #archived_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            #[inline]
            fn none_bit_set(&self, idx: usize) -> bool {
                debug_assert!(idx < #num_elements);
                self.bitmap.is_none(idx)
            }

            #[inline]
            fn idx_for_field(&self, field_idx: usize) -> usize {
                debug_assert!(field_idx < #num_elements);
                field_idx - self.bitmap.count_none_before(field_idx)
            }

            #(#sparse_get_methods)*
        }

        impl<#(#generics),*> #archived_dense_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            #[inline]
            fn none_bit_set(&self, idx: usize) -> bool {
                debug_assert!(idx < #num_elements);
                self.bitmap.is_none(idx)
            }

            #(#dense_get_methods)*
        }

        impl<#(#generics),*> #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            #[inline]
            fn sparse(&self) -> &#archived_sparse_name<#(#generics),*> {
                unsafe { &*self.data.as_ptr().cast::<#archived_sparse_name<#(#generics),*>>() }
            }

            #[inline]
            fn dense(&self) -> &#archived_dense_name<#(#generics),*> {
                unsafe { &*self.data.as_ptr().cast::<#archived_dense_name<#(#generics),*>>() }
            }

            #(#archived_get_methods)*
        }

        impl<#(#generics),*> core::cmp::PartialEq for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,)*
        {
            #[inline]
            fn eq(&self, other: &Self) -> bool {
                true #(&& #eq_checks)*
            }
        }

        impl<#(#generics),*> core::cmp::Eq for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Eq,)*
        {}

        impl<#(#generics),*> core::cmp::PartialOrd for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,)*
        {
            #[inline]
            fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<#(#generics),*> core::cmp::Ord for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,)*
        {
            #[inline]
            fn cmp(&self, other: &Self) -> core::cmp::Ordering {
                #(#cmp_checks)*
                core::cmp::Ordering::Equal
            }
        }

        struct #sparse_data_name<'a, #(#generics),*> {
            bitmap: #bitmap_ty,
            value: &'a #name<#(#generics),*>,
        }

        struct #sparse_resolver_name {
            bitmap: #bitmap_ty,
            ptrs_resolver: ::rkyv::vec::VecResolver,
            ptrs_len: usize,
        }

        impl<'a, #(#generics),*> ::rkyv::Archive for #sparse_data_name<'a, #(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            type Archived = #archived_sparse_name<#(#generics),*>;
            type Resolver = #sparse_resolver_name;

            #[inline]
            unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
                let (_fp, fo) = ::rkyv::out_field!(out.bitmap);
                fo.write(resolver.bitmap);

                let (fp, fo) = ::rkyv::out_field!(out.ptrs);
                let vec_pos = pos + fp;
                ::rkyv::vec::ArchivedVec::<::rkyv::rel_ptr::RawRelPtrI32>::resolve_from_len(
                    resolver.ptrs_len,
                    vec_pos,
                    resolver.ptrs_resolver,
                    fo,
                );

                let (_fp, fo) = ::rkyv::out_field!(out._phantom);
                fo.write(core::marker::PhantomData);
            }
        }

        impl<'a, S, #(#generics),*> ::rkyv::Serialize<S> for #sparse_data_name<'a, #(#generics),*>
        where
            S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,)*
        {
            #[inline]
            fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                let mut ptrs: [#field_ptr_name; #num_elements] =
                    [#field_ptr_name { pos: 0 }; #num_elements];
                let mut ptrs_len = 0usize;

                #(#sparse_serialize_fields)*

                let ptrs_resolver =
                    ::rkyv::vec::ArchivedVec::<::rkyv::rel_ptr::RawRelPtrI32>::serialize_from_slice(
                        &ptrs[..ptrs_len],
                        serializer,
                    )?;

                Ok(#sparse_resolver_name {
                    bitmap: self.bitmap,
                    ptrs_resolver,
                    ptrs_len,
                })
            }
        }

        struct #dense_data_name<'a, #(#generics),*> {
            bitmap: #bitmap_ty,
            value: &'a #name<#(#generics),*>,
        }

        struct #dense_resolver_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            bitmap: #bitmap_ty,
            #(#dense_resolver_fields)*
        }

        impl<'a, #(#generics),*> ::rkyv::Archive for #dense_data_name<'a, #(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            type Archived = #archived_dense_name<#(#generics),*>;
            type Resolver = #dense_resolver_name<#(#generics),*>;

            #[inline]
            unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
                let (_fp, fo) = ::rkyv::out_field!(out.bitmap);
                fo.write(resolver.bitmap);

                #(#dense_resolve_fields)*

                let (_fp, fo) = ::rkyv::out_field!(out._phantom);
                fo.write(core::marker::PhantomData);
            }
        }

        impl<'a, S, #(#generics),*> ::rkyv::Serialize<S> for #dense_data_name<'a, #(#generics),*>
        where
            S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,)*
        {
            #[inline]
            fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                #(#dense_serialize_fields)*
                Ok(#dense_resolver_name {
                    bitmap: self.bitmap,
                    #(#dense_resolver_inits),*
                })
            }
        }

        pub enum #resolver_name<#(#generics),*> {
            Sparse { data_pos: usize },
            Dense { data_pos: usize },
            _Phantom(core::marker::PhantomData<(#(#generics),*)>),
        }

        impl<#(#generics),*> ::rkyv::Archive for #name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            type Archived = #archived_name<#(#generics),*>;
            type Resolver = #resolver_name<#(#generics),*>;

            #[inline]
            unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
                let (_fp, format_out) = ::rkyv::out_field!(out.format);
                let (fp, data_out) = ::rkyv::out_field!(out.data);
                let data_out = data_out.cast::<::rkyv::rel_ptr::RawRelPtrI32>();

                match resolver {
                    #resolver_name::Sparse { data_pos } => {
                        format_out.write(#format_ty::Sparse);
                        ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos + fp, data_pos, data_out);
                    }
                    #resolver_name::Dense { data_pos } => {
                        format_out.write(#format_ty::Dense);
                        ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos + fp, data_pos, data_out);
                    }
                    #resolver_name::_Phantom(_) => unreachable!(),
                }

                let (_fp, fo) = ::rkyv::out_field!(out._phantom);
                fo.write(core::marker::PhantomData);
            }
        }

        impl<S, #(#generics),*> ::rkyv::Serialize<S> for #name<#(#generics),*>
        where
            S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,)*
        {
            #[inline]
            fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                let (bitmap, format) = self.choose_format();
                match format {
                    #format_ty::Dense => {
                        let data = #dense_data_name {
                            bitmap,
                            value: self,
                        };
                        let data_pos = serializer.serialize_value(&data)?;
                        Ok(#resolver_name::Dense { data_pos })
                    }
                    #format_ty::Sparse => {
                        let data = #sparse_data_name {
                            bitmap,
                            value: self,
                        };
                        let data_pos = serializer.serialize_value(&data)?;
                        Ok(#resolver_name::Sparse { data_pos })
                    }
                }
            }
        }

        impl<D, #(#generics),*> ::rkyv::Deserialize<#name<#(#generics),*>, D>
            for #archived_name<#(#generics),*>
        where
            D: ::rkyv::Fallible + ::core::any::Any,
            #(#generics: ::rkyv::Archive + Default + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<#generics>: ::rkyv::Deserialize<#generics, D>,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>:
                ::rkyv::Deserialize<<#generics as ::dbsp::utils::IsNone>::Inner, D>,)*
        {
            #[inline]
            fn deserialize(&self, deserializer: &mut D) -> Result<#name<#(#generics),*>, D::Error> {
                match self.format {
                    #format_ty::Sparse => {
                        let sparse = self.sparse();
                        let mut ptr_idx = 0usize;
                        #(#sparse_deserialize_fields)*
                        Ok(#name( #(#fields),* ))
                    }
                    #format_ty::Dense => {
                        let dense = self.dense();
                        #(#dense_deserialize_fields)*
                        Ok(#name( #(#fields),* ))
                    }
                }
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

    let rkyv_blocks = if use_legacy {
        quote! {
            #legacy_archived_ord_impls
        }
    } else {
        quote! {
            #choose_format_impl
            #rkyv_impls
        }
    };

    expanded.extend(quote! {
        #struct_def
        #constructor
        #getter_setter
        #num_elements_const
        #rkyv_blocks
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
    /// `cargo test -p feldera-macros -- dump_tup_expansions --nocapture`
    fn dump_tup_expansions() {
        let tuple: TupleDef = syn::parse2(quote!(Tup2<T0, T1>)).expect("Failed to parse TupleDef");
        let expanded = declare_tuple_impl(tuple);
        let parsed_file: syn::File = syn::parse2(expanded).expect("Failed to parse output");
        let formatted = prettyplease::unparse(&parsed_file);
        println!("{formatted}");
        assert!(formatted.contains("pub struct Tup2"));

        let tuple: TupleDef = syn::parse2(quote!(Tup8<T0, T1, T2, T3, T4, T5, T6, T7>))
            .expect("Failed to parse TupleDef");
        let expanded = declare_tuple_impl(tuple);
        let parsed_file: syn::File = syn::parse2(expanded).expect("Failed to parse output");
        let formatted = prettyplease::unparse(&parsed_file);
        println!("{formatted}");
        assert!(formatted.contains("pub struct Tup8"));
    }
}
