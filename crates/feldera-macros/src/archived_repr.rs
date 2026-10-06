//! Code generation for `#[derive(ArchivedRepr)]`.
//!
//! The derive implements, for the archived form `ArchivedFoo` of a struct or
//! enum `Foo`, the three traits storage needs to work on it without decoding
//! it: `OrdRepr<Foo>` and `HashRepr`, generated exactly as their own derives
//! generate them, and `ArchivedRepr<Foo>`, whose `MAX_ALIGN` is the strictest
//! alignment of the archived value's root and of everything its fields hold.
//!
//! A field with `#[omit_bounds]`, rkyv's marker for a recursive type, cannot
//! name its own alignment, which is defined in terms of the type being
//! derived; it counts as the strictest alignment of a primitive instead.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{parse_quote, Data, DeriveInput};

use crate::archive_attrs::{all_fields, archive_bounds, omits_bounds};
use crate::hash_repr::derive_hash_repr_impl;
use crate::ord_repr::derive_ord_repr_impl;

pub(super) fn derive_archived_repr_impl(input: DeriveInput) -> syn::Result<TokenStream2> {
    if let Data::Union(_) = &input.data {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "ArchivedRepr cannot be derived for a union",
        ));
    }
    let ord_repr = derive_ord_repr_impl(input.clone())?;
    let hash_repr = derive_hash_repr_impl(input.clone())?;
    let archived_repr = archived_repr_impl(&input)?;
    Ok(quote! {
        #ord_repr
        #hash_repr
        #archived_repr
    })
}

/// Generates `impl ArchivedRepr<Foo> for ArchivedFoo`.
///
/// The archived value's root is `ArchivedFoo` itself, and each field adds
/// whatever it keeps out of line, so `MAX_ALIGN` is the larger of the root's
/// alignment and every field's `MAX_ALIGN`.  The bounds are those of the
/// `OrdRepr` derive, with each field's archived form required to implement
/// `ArchivedRepr` rather than `OrdRepr`, which it extends.
///
/// # Arguments
///
/// * `input` - the struct or enum being derived.
///
/// # Returns
///
/// The impl, or an error for an attribute the bounds cannot parse.
fn archived_repr_impl(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let ident = &input.ident;
    let archived = format_ident!("Archived{}", ident);

    let mut generics = input.generics.clone();
    let mut alignments = vec![quote!(::core::mem::align_of::<Self>())];
    {
        let where_clause = generics.make_where_clause();
        where_clause.predicates.extend(archive_bounds(input)?);
        for field in all_fields(&input.data) {
            if omits_bounds(field) {
                alignments.push(quote!(::dbsp::dynamic::MAX_PRIMITIVE_ALIGN));
                continue;
            }
            let ty = &field.ty;
            where_clause
                .predicates
                .push(parse_quote!(#ty: ::rkyv::Archive));
            where_clause.predicates.push(parse_quote!(
                <#ty as ::rkyv::Archive>::Archived: ::dbsp::dynamic::ArchivedRepr<#ty>
            ));
            alignments.push(quote!(
                <<#ty as ::rkyv::Archive>::Archived as ::dbsp::dynamic::ArchivedRepr<#ty>>::MAX_ALIGN
            ));
        }
    }
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::dbsp::dynamic::ArchivedRepr<#ident #ty_generics>
            for #archived #ty_generics #where_clause
        {
            const MAX_ALIGN: usize = {
                let mut max = 1usize;
                #(max = ::dbsp::dynamic::max_align(max, #alignments);)*
                max
            };
        }
    })
}
