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
    let archived_inline_sparse_name = format_ident!("Archived{}InlineSparse", name);
    let inline_sparse_data_name = format_ident!("{}InlineSparseData", name);
    let inline_sparse_resolver_name = format_ident!("{}InlineSparseResolver", name);
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

    // Generate get methods for the InlineSparse format.
    // Each accessor walks the inline body to find the field's offset.
    // Fields are aligned to INLINE_ALIGN within the blob (which itself
    // starts at INLINE_SPARSE_MAX_ALIGN alignment).
    let inline_sparse_get_methods = fields
        .iter()
        .enumerate()
        .zip(generics.iter())
        .map(|((idx, _field), ty)| {
            let get_name = format_ident!("get_t{}", idx);
            let idx_lit = Index::from(idx);

            // Generate code to compute the byte offset of field `idx` by
            // summing alignment padding + inline sizes of preceding non-NULL
            // fields.
            let offset_computation: Vec<_> = (0..idx)
                .map(|prev| {
                    let prev_lit = Index::from(prev);
                    let prev_ty = &generics[prev];
                    quote! {
                        if !self.bitmap.is_none(#prev_lit) {
                            let align = <<#prev_ty as ::dbsp::utils::IsNone>::Inner
                                as ::dbsp::utils::ArchiveLayout>::INLINE_ALIGN;
                            if align > 1 { off = (off + align - 1) & !(align - 1); }
                            off += <<#prev_ty as ::dbsp::utils::IsNone>::Inner
                                as ::dbsp::utils::ArchiveLayout>::INLINE_SIZE;
                        }
                    }
                })
                .collect();

            quote! {
                #[inline]
                pub fn #get_name(
                    &self,
                ) -> Option<&::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>> {
                    if self.bitmap.is_none(#idx_lit) {
                        return None;
                    }
                    let base = self as *const Self as *const u8;
                    // Inline body starts right after the bitmap.
                    let mut off = #bitmap_bytes;
                    #(#offset_computation)*
                    // Align for the current field.
                    let align = <<#ty as ::dbsp::utils::IsNone>::Inner
                        as ::dbsp::utils::ArchiveLayout>::INLINE_ALIGN;
                    if align > 1 { off = (off + align - 1) & !(align - 1); }
                    if <<#ty as ::dbsp::utils::IsNone>::Inner
                        as ::dbsp::utils::ArchiveLayout>::IS_FIXED
                    {
                        // Fixed-size: archived value is stored inline.
                        Some(unsafe { &*base.add(off).cast() })
                    } else {
                        // Variable-size: an i32 relative pointer is stored
                        // inline, pointing to the archived value elsewhere
                        // in the rkyv buffer.
                        unsafe {
                            let ptr_addr = base.add(off);
                            let rel_offset = core::ptr::read_unaligned(
                                ptr_addr as *const i32
                            );
                            let target = ptr_addr.offset(rel_offset as isize);
                            Some(&*target.cast())
                        }
                    }
                }
            }
        });

    // Walking-read method names shared across formats.
    let walking_read_names: Vec<_> = (0..num_elements)
        .map(|idx| format_ident!("walking_read_t{}", idx))
        .collect();

    // Generate walking-read methods for the Sparse format.
    // Maintains a running `ptr_idx` that increments for each non-NULL field,
    // avoiding the O(n²) cost of count_none_before() per random access.
    // Must be called in field order starting from ptr_idx = 0.
    let sparse_walking_methods = fields
        .iter()
        .enumerate()
        .zip(generics.iter())
        .map(|((idx, _field), ty)| {
            let method_name = &walking_read_names[idx];
            let idx_lit = Index::from(idx);
            quote! {
                #[inline]
                fn #method_name(
                    &self,
                    ptr_idx: &mut usize,
                ) -> Option<&::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>> {
                    if self.bitmap.is_none(#idx_lit) {
                        return None;
                    }
                    let idx = *ptr_idx;
                    *ptr_idx = idx + 1;
                    debug_assert!(idx < self.ptrs.len());
                    Some(unsafe {
                        &*self
                            .ptrs
                            .as_slice()
                            .get_unchecked(idx)
                            .as_ptr()
                            .cast::<::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>>()
                    })
                }
            }
        });

    // Generate walking-read methods for the Dense format.
    // Dense has O(1) field access so walking just delegates to get_tN,
    // ignoring the offset parameter. This allows the envelope to use
    // a uniform walking interface across all formats.
    let dense_walking_methods = fields
        .iter()
        .enumerate()
        .zip(generics.iter())
        .map(|((idx, _field), ty)| {
            let method_name = &walking_read_names[idx];
            let get_name = format_ident!("get_t{}", idx);
            quote! {
                #[inline]
                fn #method_name(
                    &self,
                    _off: &mut usize,
                ) -> Option<&::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>> {
                    self.#get_name()
                }
            }
        });

    // Generate walking-read methods for the InlineSparse format.
    // Each method reads the current field and advances `off` to the next.
    // Must be called in field order starting from off = bitmap_bytes.
    let inline_sparse_walking_methods = fields
        .iter()
        .enumerate()
        .zip(generics.iter())
        .map(|((idx, _field), ty)| {
            let method_name = &walking_read_names[idx];
            let idx_lit = Index::from(idx);
            quote! {
                #[inline]
                unsafe fn #method_name(
                    &self,
                    off: &mut usize,
                ) -> Option<&::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>> {
                    if self.bitmap.is_none(#idx_lit) {
                        return None;
                    }
                    let base = self as *const Self as *const u8;
                    let align = <<#ty as ::dbsp::utils::IsNone>::Inner
                        as ::dbsp::utils::ArchiveLayout>::INLINE_ALIGN;
                    if align > 1 { *off = (*off + align - 1) & !(align - 1); }
                    let result = if <<#ty as ::dbsp::utils::IsNone>::Inner
                        as ::dbsp::utils::ArchiveLayout>::IS_FIXED
                    {
                        &*base.add(*off).cast()
                    } else {
                        let ptr_addr = base.add(*off);
                        let rel_offset = core::ptr::read_unaligned(ptr_addr as *const i32);
                        &*ptr_addr.offset(rel_offset as isize).cast()
                    };
                    *off += <<#ty as ::dbsp::utils::IsNone>::Inner
                        as ::dbsp::utils::ArchiveLayout>::INLINE_SIZE;
                    Some(result)
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
                            #format_ty::InlineSparse => self.inline_sparse().#get_name(),
                        }
                    }
                }
            });

    // ---------------------------------------------------------------------------
    // Comparison generators for the archived envelope type.
    //
    // The envelope's walking_read_tN dispatches per format: InlineSparse
    // uses sequential walking with a running offset (O(1) per field),
    // while Dense/Sparse delegate to O(1) random-access get_tN (ignoring
    // the offset). This uniform interface lets all format combinations
    // share a single comparison codepath — two running offsets, one per
    // side, with walking_read_tN on both.
    //
    // An additional fast-path exists for both-InlineSparse with equal
    // bitmaps: identical layouts allow a single shared offset.
    //
    // `gen_eq` and `gen_cmp` produce per-field check sequences from any
    // pair of field-access closures.
    // ---------------------------------------------------------------------------

    // Field-access expression via walking_read_tN (advancing offset).
    let walking_field = |idx: usize, obj: &TokenStream2, off: &Ident| -> TokenStream2 {
        let method = &walking_read_names[idx];
        quote!(unsafe { #obj.#method(&mut #off) })
    };

    let self_tok: TokenStream2 = quote!(self);
    let other_tok: TokenStream2 = quote!(other);
    let off_self_id = format_ident!("off_self");
    let off_other_id = format_ident!("off_other");

    // Generate eq checks: `if left != right { return false; }` per field.
    let gen_eq = |left: &dyn Fn(usize) -> TokenStream2,
                  right: &dyn Fn(usize) -> TokenStream2| -> Vec<TokenStream2> {
        (0..num_elements).map(|idx| {
            let l = left(idx); let r = right(idx);
            quote! { if #l != #r { return false; } }
        }).collect()
    };

    // Generate cmp checks: compare and return early if not Equal, per field.
    let gen_cmp = |left: &dyn Fn(usize) -> TokenStream2,
                   right: &dyn Fn(usize) -> TokenStream2| -> Vec<TokenStream2> {
        (0..num_elements).map(|idx| {
            let l = left(idx); let r = right(idx);
            quote! {{
                let cmp = (#l).cmp(&(#r));
                if cmp != core::cmp::Ordering::Equal { return cmp; }
            }}
        }).collect()
    };

    // General two-offset walk via walking_read_tN on both sides.
    // Used both at envelope level (dispatches per format) and inside
    // InlineSparse's walking_cmp (different-bitmap fallback).
    // Caller must initialize off_self/off_other appropriately.
    let general_eq_checks = gen_eq(
        &|idx| walking_field(idx, &self_tok, &off_self_id),
        &|idx| walking_field(idx, &other_tok, &off_other_id),
    );
    let general_cmp_checks = gen_cmp(
        &|idx| walking_field(idx, &self_tok, &off_self_id),
        &|idx| walking_field(idx, &other_tok, &off_other_id),
    );

    // Both InlineSparse with equal bitmaps: single-offset walk.
    // Equal bitmaps guarantee identical inline body layout, so we
    // advance one shared offset and compare values at the same position.
    let same_bitmap_eq_checks: Vec<TokenStream2> = (0..num_elements).map(|idx| {
        let method = &walking_read_names[idx];
        let idx_lit = Index::from(idx);
        // walking_read_tN only advances `off` when the field is present.
        // With equal bitmaps both sides have the same present set, so
        // calling walking_read on `self` advances the shared offset
        // while `other` reads from the same position.
        quote! {
            if !self.bitmap.is_none(#idx_lit) {
                let saved_off = off;
                let left = self.#method(&mut off);
                let mut off2 = saved_off;
                let right = other.#method(&mut off2);
                if left != right { return false; }
            }
        }
    }).collect();

    let same_bitmap_cmp_checks: Vec<TokenStream2> = (0..num_elements).map(|idx| {
        let method = &walking_read_names[idx];
        let idx_lit = Index::from(idx);
        quote! {
            if !self.bitmap.is_none(#idx_lit) {
                let saved_off = off;
                let left = self.#method(&mut off);
                let mut off2 = saved_off;
                let right = other.#method(&mut off2);
                let cmp = left.cmp(&right);
                if cmp != core::cmp::Ordering::Equal { return cmp; }
            }
        }
    }).collect();

    // Sparse same-format walking comparisons.
    // Uses walking_read_tN with running ptr_idx instead of O(n²) get_tN.
    let sparse_self_tok: TokenStream2 = quote!(self);
    let sparse_other_tok: TokenStream2 = quote!(other);
    let sparse_off_self = format_ident!("pi_self");
    let sparse_off_other = format_ident!("pi_other");
    let sparse_eq_checks = gen_eq(
        &|idx| { let m = &walking_read_names[idx]; quote!(#sparse_self_tok.#m(&mut #sparse_off_self)) },
        &|idx| { let m = &walking_read_names[idx]; quote!(#sparse_other_tok.#m(&mut #sparse_off_other)) },
    );
    let sparse_cmp_checks = gen_cmp(
        &|idx| { let m = &walking_read_names[idx]; quote!(#sparse_self_tok.#m(&mut #sparse_off_self)) },
        &|idx| { let m = &walking_read_names[idx]; quote!(#sparse_other_tok.#m(&mut #sparse_off_other)) },
    );

    // Dense same-format comparisons via get_tN (already O(1), no match dispatch).
    let dense_self_tok: TokenStream2 = quote!(self);
    let dense_other_tok: TokenStream2 = quote!(other);
    let dense_eq_checks = gen_eq(
        &|idx| { let g = format_ident!("get_t{}", idx); quote!(#dense_self_tok.#g()) },
        &|idx| { let g = format_ident!("get_t{}", idx); quote!(#dense_other_tok.#g()) },
    );
    let dense_cmp_checks = gen_cmp(
        &|idx| { let g = format_ident!("get_t{}", idx); quote!(#dense_self_tok.#g()) },
        &|idx| { let g = format_ident!("get_t{}", idx); quote!(#dense_other_tok.#g()) },
    );

    // IS↔Dense cross-format comparisons. IS walking_read on `self`, Dense
    // get_tN on `other`. Implemented as methods on the IS struct; the
    // envelope normalizes IS to the left and reverses cmp if swapped.
    let is_dense_off = format_ident!("is_off");
    let is_dense_self: TokenStream2 = quote!(self);
    let is_dense_other: TokenStream2 = quote!(other);
    let is_dense_eq_checks = gen_eq(
        &|idx| { let m = &walking_read_names[idx]; quote!(unsafe { #is_dense_self.#m(&mut #is_dense_off) }) },
        &|idx| { let g = format_ident!("get_t{}", idx); quote!(#is_dense_other.#g()) },
    );
    let is_dense_cmp_checks = gen_cmp(
        &|idx| { let m = &walking_read_names[idx]; quote!(unsafe { #is_dense_self.#m(&mut #is_dense_off) }) },
        &|idx| { let g = format_ident!("get_t{}", idx); quote!(#is_dense_other.#g()) },
    );

    // Envelope walking_read_tN methods: dispatch to each format's walking
    // reader. All three formats accept `off: &mut usize` but interpret it
    // differently:
    //   - InlineSparse: byte offset into inline body (start: bitmap_bytes)
    //   - Sparse: index into ptrs array (start: 0)
    //   - Dense: ignored (O(1) field access)
    // The caller initializes `off` via `walking_offset_init()`.
    let envelope_walking_methods = fields
        .iter()
        .enumerate()
        .zip(generics.iter())
        .map(|((idx, _field), ty)| {
            let walking_name = &walking_read_names[idx];
            quote! {
                #[inline]
                fn #walking_name(
                    &self,
                    off: &mut usize,
                ) -> Option<&::rkyv::Archived<<#ty as ::dbsp::utils::IsNone>::Inner>> {
                    match self.format {
                        #format_ty::InlineSparse => unsafe {
                            self.inline_sparse().#walking_name(off)
                        },
                        #format_ty::Sparse => self.sparse().#walking_name(off),
                        #format_ty::Dense => self.dense().#walking_name(off),
                    }
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
                // Choose inline sparse format if 40% is None
                if none_bits * 10 >= Self::NUM_ELEMENTS * 4 {
                    (bitmap, #format_ty::InlineSparse)
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
            #(#sparse_walking_methods)*
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
            #(#dense_walking_methods)*
        }

        // InlineSparse archived struct.
        // Layout: bitmap | inline body
        // The inline body contains fixed-size archived values stored directly
        // and i32 relative pointers for variable-size fields, only for non-NULL
        // fields. The struct is repr(C) but only the bitmap has known layout;
        // the inline body is accessed via computed byte offsets.
        #[repr(C)]
        pub struct #archived_inline_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
        {
            bitmap: #bitmap_ty,
            _phantom: core::marker::PhantomData<fn() -> (#(#generics),*)>,
        }

        impl<#(#generics),*> #archived_inline_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::dbsp::utils::ArchiveLayout,)*
        {
            #(#inline_sparse_get_methods)*
            #(#inline_sparse_walking_methods)*
        }

        // Walking equality comparison for InlineSparse (PartialEq bound).
        impl<#(#generics),*> #archived_inline_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,)*
        {
            #[inline]
            fn walking_eq(&self, other: &Self) -> bool {
                // Fast path: different NULL patterns → tuples differ
                // (None != Some(_) for any field).
                if self.bitmap != other.bitmap {
                    return false;
                }
                // Same bitmap → identical inline body layout. Walk with
                // a single offset; skip None fields entirely.
                let mut off = #bitmap_bytes;
                unsafe {
                    #(#same_bitmap_eq_checks)*
                }
                true
            }
        }

        // Walking ordering comparison for InlineSparse (Ord bound).
        impl<#(#generics),*> #archived_inline_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,)*
        {
            #[inline]
            fn walking_cmp(&self, other: &Self) -> core::cmp::Ordering {
                if self.bitmap == other.bitmap {
                    // Same bitmap → single-offset walk.
                    let mut off = #bitmap_bytes;
                    unsafe {
                        #(#same_bitmap_cmp_checks)*
                    }
                    return core::cmp::Ordering::Equal;
                }
                // Different bitmaps: two-offset walk needed.
                let mut off_self = #bitmap_bytes;
                let mut off_other = #bitmap_bytes;
                unsafe {
                    #(#general_cmp_checks)*
                }
                core::cmp::Ordering::Equal
            }
        }

        // Cross-format IS↔Dense comparisons.
        // IS is always `self`; the envelope normalizes and reverses cmp if needed.
        impl<#(#generics),*> #archived_inline_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,)*
        {
            #[inline]
            fn eq_with_dense(&self, other: &#archived_dense_name<#(#generics),*>) -> bool {
                let mut is_off = #bitmap_bytes;
                #(#is_dense_eq_checks)*
                true
            }
        }

        impl<#(#generics),*> #archived_inline_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,)*
        {
            #[inline]
            fn cmp_with_dense(&self, other: &#archived_dense_name<#(#generics),*>) -> core::cmp::Ordering {
                let mut is_off = #bitmap_bytes;
                #(#is_dense_cmp_checks)*
                core::cmp::Ordering::Equal
            }
        }

        // Walking equality comparison for Sparse.
        impl<#(#generics),*> #archived_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,)*
        {
            #[inline]
            fn walking_eq(&self, other: &Self) -> bool {
                if self.bitmap != other.bitmap {
                    return false;
                }
                let mut pi_self = 0usize;
                let mut pi_other = 0usize;
                #(#sparse_eq_checks)*
                true
            }
        }

        // Walking ordering comparison for Sparse.
        impl<#(#generics),*> #archived_sparse_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,)*
        {
            #[inline]
            fn walking_cmp(&self, other: &Self) -> core::cmp::Ordering {
                let mut pi_self = 0usize;
                let mut pi_other = 0usize;
                #(#sparse_cmp_checks)*
                core::cmp::Ordering::Equal
            }
        }

        // Walking equality comparison for Dense.
        impl<#(#generics),*> #archived_dense_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,)*
        {
            #[inline]
            fn walking_eq(&self, other: &Self) -> bool {
                if self.bitmap != other.bitmap {
                    return false;
                }
                #(#dense_eq_checks)*
                true
            }
        }

        // Walking ordering comparison for Dense.
        impl<#(#generics),*> #archived_dense_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,)*
        {
            #[inline]
            fn walking_cmp(&self, other: &Self) -> core::cmp::Ordering {
                #(#dense_cmp_checks)*
                core::cmp::Ordering::Equal
            }
        }

        impl<#(#generics),*> #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
        {
            #[inline]
            fn sparse(&self) -> &#archived_sparse_name<#(#generics),*> {
                unsafe { &*self.data.as_ptr().cast::<#archived_sparse_name<#(#generics),*>>() }
            }

            #[inline]
            fn dense(&self) -> &#archived_dense_name<#(#generics),*> {
                unsafe { &*self.data.as_ptr().cast::<#archived_dense_name<#(#generics),*>>() }
            }

            #[inline]
            fn inline_sparse(&self) -> &#archived_inline_sparse_name<#(#generics),*> {
                unsafe { &*self.data.as_ptr().cast::<#archived_inline_sparse_name<#(#generics),*>>() }
            }

            /// Returns the initial walking offset for this tuple's format.
            /// InlineSparse starts walking at byte offset `bitmap_bytes`;
            /// Sparse starts at ptr_idx 0; Dense ignores the offset.
            #[inline]
            fn walking_offset_init(&self) -> usize {
                match self.format {
                    #format_ty::InlineSparse => #bitmap_bytes,
                    _ => 0,
                }
            }

            #(#archived_get_methods)*
            #(#envelope_walking_methods)*
        }

        impl<#(#generics),*> core::cmp::PartialEq for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,)*
        {
            #[inline]
            fn eq(&self, other: &Self) -> bool {
                // Each comparison branch below is a separate function call whose
                // body contains one field-access + compare per tuple field (N
                // iterations for an N-field tuple). The general cross-format
                // fallback at the bottom inlines a 3-arm format match per field
                // per side, producing ~70 instructions × N fields of machine
                // code. For wide tuples (e.g. 700 fields) that exceeds L1
                // icache (32 KB) and causes thrashing on every comparison.
                //
                // The same-format and IS↔Dense fast paths avoid the per-field
                // match by calling format-specific methods directly, cutting
                // per-field code size by 2-4×. The general fallback is only
                // reached for rare cross-format pairs during format migration.

                // Same-format: dispatch once, then compare without per-field match.
                if self.format == other.format {
                    return match self.format {
                        #format_ty::InlineSparse =>
                            self.inline_sparse().walking_eq(other.inline_sparse()),
                        #format_ty::Sparse =>
                            self.sparse().walking_eq(other.sparse()),
                        #format_ty::Dense =>
                            self.dense().walking_eq(other.dense()),
                    };
                }
                // IS↔Dense: one implementation, normalize IS to the left.
                if (self.format == #format_ty::InlineSparse && other.format == #format_ty::Dense)
                    || (self.format == #format_ty::Dense && other.format == #format_ty::InlineSparse)
                {
                    let (is_ref, dense_ref) = if self.format == #format_ty::InlineSparse {
                        (self.inline_sparse(), other.dense())
                    } else {
                        (other.inline_sparse(), self.dense())
                    };
                    return is_ref.eq_with_dense(dense_ref);
                }
                // General cross-format fallback (per-field match dispatch).
                let mut off_self = self.walking_offset_init();
                let mut off_other = other.walking_offset_init();
                #(#general_eq_checks)*
                true
            }
        }

        impl<#(#generics),*> core::cmp::Eq for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Eq,)*
        {}

        impl<#(#generics),*> core::cmp::PartialOrd for #archived_name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
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
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
            #(::rkyv::Archived<<#generics as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,)*
        {
            #[inline]
            fn cmp(&self, other: &Self) -> core::cmp::Ordering {
                // See the comment in `eq` above: format-specific branches
                // reduce machine code size to avoid L1 icache thrashing on
                // wide tuples.

                // Same-format: dispatch once, then compare without per-field match.
                if self.format == other.format {
                    return match self.format {
                        #format_ty::InlineSparse =>
                            self.inline_sparse().walking_cmp(other.inline_sparse()),
                        #format_ty::Sparse =>
                            self.sparse().walking_cmp(other.sparse()),
                        #format_ty::Dense =>
                            self.dense().walking_cmp(other.dense()),
                    };
                }
                // IS↔Dense: normalize IS to left, reverse result if swapped.
                if (self.format == #format_ty::InlineSparse && other.format == #format_ty::Dense)
                    || (self.format == #format_ty::Dense && other.format == #format_ty::InlineSparse)
                {
                    return if self.format == #format_ty::InlineSparse {
                        self.inline_sparse().cmp_with_dense(other.dense())
                    } else {
                        other.inline_sparse().cmp_with_dense(self.dense()).reverse()
                    };
                }
                // General cross-format fallback (per-field match dispatch).
                let mut off_self = self.walking_offset_init();
                let mut off_other = other.walking_offset_init();
                #(#general_cmp_checks)*
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

        // InlineSparse data wrapper for serialization.
        struct #inline_sparse_data_name<'a, #(#generics),*> {
            bitmap: #bitmap_ty,
            value: &'a #name<#(#generics),*>,
        }

        // InlineSparse resolver: carries the blob start position.
        struct #inline_sparse_resolver_name {
            // Position in the serializer buffer where the inline blob starts.
            blob_start: usize,
        }

        impl<'a, #(#generics),*> ::rkyv::Archive for #inline_sparse_data_name<'a, #(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
        {
            type Archived = #archived_inline_sparse_name<#(#generics),*>;
            type Resolver = #inline_sparse_resolver_name;

            #[inline]
            unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
                // All data is written directly in serialize() via raw writes.
                // The resolve step is a no-op because we already wrote all
                // bytes in the correct layout during serialize.
            }
        }

        impl<'a, S, #(#generics),*> ::rkyv::Serialize<S> for #inline_sparse_data_name<'a, #(#generics),*>
        where
            S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>
                + ::dbsp::utils::ArchiveLayout,)*
        {
            #[inline]
            fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                use ::rkyv::ser::Serializer as _;

                // Phase 1: serialize all non-NULL variable-size fields into the
                // rkyv buffer first. We record their positions so we can write
                // i32 relative pointers in the inline body.
                let mut var_positions: [usize; #num_elements] =
                    [0usize; #num_elements];

                #(
                {
                    let field_idx = #self_indexes;
                    if !self.bitmap.is_none(field_idx) {
                        if !<<#generics as ::dbsp::utils::IsNone>::Inner
                            as ::dbsp::utils::ArchiveLayout>::IS_FIXED
                        {
                            let inner = ::dbsp::utils::IsNone::unwrap_or_self(
                                &self.value.#self_indexes,
                            );
                            var_positions[field_idx] = serializer.serialize_value(inner)?;
                        }
                    }
                }
                )*

                // Phase 2: write inline blob directly to serializer.
                // Fixed-size fields have their archived bytes written directly;
                // variable-size fields get an i32 relative pointer.
                // Fields are padded to their INLINE_ALIGN within the blob.

                // Align serializer to INLINE_SPARSE_MAX_ALIGN so that field
                // alignment within the blob translates to absolute alignment.
                {
                    let max_align = ::dbsp::utils::INLINE_SPARSE_MAX_ALIGN;
                    let cur = serializer.pos();
                    let padding = cur.wrapping_neg() & (max_align - 1);
                    if padding > 0 {
                        serializer.pad(padding)?;
                    }
                }
                let blob_start = serializer.pos();

                // Write bitmap directly to serializer.
                serializer.write(unsafe {
                    core::slice::from_raw_parts(
                        &self.bitmap as *const #bitmap_ty as *const u8,
                        #bitmap_bytes,
                    )
                })?;

                // Track offset within the blob for alignment calculations.
                let mut blob_off: usize = #bitmap_bytes;

                // Write inline body for each non-NULL field.
                #(
                {
                    let field_idx = #self_indexes;
                    if !self.bitmap.is_none(field_idx) {
                        // Pad to field alignment.
                        let align = <<#generics as ::dbsp::utils::IsNone>::Inner
                            as ::dbsp::utils::ArchiveLayout>::INLINE_ALIGN;
                        if align > 1 {
                            let padded = (blob_off + align - 1) & !(align - 1);
                            let padding = padded - blob_off;
                            if padding > 0 {
                                serializer.pad(padding)?;
                                blob_off = padded;
                            }
                        }

                        if <<#generics as ::dbsp::utils::IsNone>::Inner
                            as ::dbsp::utils::ArchiveLayout>::IS_FIXED
                        {
                            // Fixed-size: copy the raw bytes of the value.
                            let inner = ::dbsp::utils::IsNone::unwrap_or_self(
                                &self.value.#self_indexes,
                            );
                            let size = <<#generics as ::dbsp::utils::IsNone>::Inner
                                as ::dbsp::utils::ArchiveLayout>::ARCHIVED_SIZE;
                            let bytes = unsafe {
                                core::slice::from_raw_parts(
                                    inner as *const <#generics as ::dbsp::utils::IsNone>::Inner
                                        as *const u8,
                                    size,
                                )
                            };
                            serializer.write(bytes)?;
                            blob_off += size;
                        } else {
                            // Variable-size: write an i32 relative pointer.
                            let ptr_pos = blob_start + blob_off;
                            let target_pos = var_positions[field_idx];
                            let rel_offset =
                                (target_pos as isize - ptr_pos as isize) as i32;
                            serializer.write(&rel_offset.to_le_bytes())?;
                            blob_off += 4;
                        }
                    }
                }
                )*

                Ok(#inline_sparse_resolver_name {
                    blob_start,
                })
            }
        }

        pub enum #resolver_name<#(#generics),*> {
            Sparse { data_pos: usize },
            Dense { data_pos: usize },
            InlineSparse { data_pos: usize },
            _Phantom(core::marker::PhantomData<(#(#generics),*)>),
        }

        impl<#(#generics),*> ::rkyv::Archive for #name<#(#generics),*>
        where
            #(#generics: ::rkyv::Archive + ::dbsp::utils::IsNone,)*
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::dbsp::utils::ArchiveLayout,)*
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
                    #resolver_name::InlineSparse { data_pos } => {
                        format_out.write(#format_ty::InlineSparse);
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
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>
                + ::dbsp::utils::ArchiveLayout,)*
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
                    #format_ty::InlineSparse => {
                        let data = #inline_sparse_data_name {
                            bitmap,
                            value: self,
                        };
                        // serialize() writes var-size fields first, then the
                        // inline blob. blob_start is the position of the blob.
                        let resolver = ::rkyv::Serialize::serialize(&data, serializer)?;
                        Ok(#resolver_name::InlineSparse { data_pos: resolver.blob_start })
                    }
                    _ => {
                        // Old Sparse format is no longer written, but handle
                        // gracefully in case choose_format is changed.
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
            #(<#generics as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive
                + ::dbsp::utils::ArchiveLayout,)*
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
                    #format_ty::InlineSparse => {
                        let is = self.inline_sparse();
                        let mut off = #bitmap_bytes;
                        #(
                        let #fields = match unsafe { is.#walking_read_names(&mut off) } {
                            None => <#generics>::default(),
                            Some(archived) => {
                                let inner: <#generics as ::dbsp::utils::IsNone>::Inner =
                                    archived.deserialize(deserializer)?;
                                <#generics as ::dbsp::utils::IsNone>::from_inner(inner)
                            }
                        };
                        )*
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
