//! Code generation for `#[derive(HashRepr)]`.
//!
//! The derive produces `impl HashRepr for ArchivedFoo`, writing to a hasher
//! what hashing a `Foo` would write. It follows `#[derive(Hash)]`, which
//! hashes a struct's fields in declaration order and writes nothing else, so
//! a type whose `Hash` is derived gets a `HashRepr` that agrees with it. A
//! type with a hand-written `Hash` needs a hand-written `HashRepr`.
//!
//! Three shapes have no faithful answer, and decline rather than guess. The
//! impl is still generated, because `ArchivedDBData` requires the trait of
//! every type; declining costs its caller a deserialization, which is slow
//! rather than wrong.
//!
//! - An enum. `#[derive(Hash)]` writes `mem::discriminant`, and rkyv gives
//!   the archived enum the narrowest repr that fits its variants, so the two
//!   forms write discriminants of different widths.
//! - A field with a `#[with]` wrapper, whose archived form is the wrapper's
//!   rather than the field's, and hashes as the wrapper sees fit.
//! - A field with `#[omit_bounds]`, rkyv's marker for a recursive type.
//!   Whether such a type is faithful is defined in terms of itself, which is
//!   a cycle rather than an answer.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{parse_quote, Data, DeriveInput, Index, Member};

use crate::archive_attrs::{all_fields, archive_bounds, omits_bounds, with_wrapper};

pub(super) fn derive_hash_repr_impl(input: DeriveInput) -> syn::Result<TokenStream2> {
    let ident = &input.ident;
    let archived = format_ident!("Archived{}", ident);

    let struct_fields = match &input.data {
        Data::Struct(data) => Some(&data.fields),
        Data::Enum(_) => None,
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                ident,
                "HashRepr cannot be derived for a union",
            ));
        }
    };

    // Only a struct whose every field keeps its own archived form can say
    // what the decoded value writes; see this module's introduction for the
    // three shapes that cannot.
    let faithful = struct_fields.is_some_and(|fields| {
        fields
            .iter()
            .all(|field| with_wrapper(field).is_none() && !omits_bounds(field))
    });

    // The impl needs the archived type's own bounds, which rkyv's derive
    // builds from the input's where clause, `#[archive(bound(archive = ..))]`
    // and `T: Archive` for every field type `T`. A faithful impl asks for one
    // bound more, that each field's archived form hash like the field, in the
    // same per-field way that rkyv bounds field types rather than parameters.
    let mut generics = input.generics.clone();
    {
        let where_clause = generics.make_where_clause();
        where_clause.predicates.extend(archive_bounds(&input)?);
        for field in all_fields(&input.data) {
            if omits_bounds(field) {
                continue;
            }
            let ty = &field.ty;
            where_clause
                .predicates
                .push(parse_quote!(#ty: ::rkyv::Archive));
            if faithful {
                where_clause.predicates.push(parse_quote!(
                    <#ty as ::rkyv::Archive>::Archived: ::dbsp::dynamic::HashRepr
                ));
            }
        }
    }
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();

    // A composite is faithful only if everything it holds is, and hashes by
    // hashing its fields in declaration order.
    let (is_faithful, body) = match struct_fields.filter(|_| faithful) {
        Some(fields) => {
            let types = fields.iter().map(|field| &field.ty);
            let members = fields
                .iter()
                .enumerate()
                .map(|(index, field)| match &field.ident {
                    Some(name) => Member::Named(name.clone()),
                    None => Member::Unnamed(Index::from(index)),
                });
            (
                quote!(true #(&& <<#types as ::rkyv::Archive>::Archived
                    as ::dbsp::dynamic::HashRepr>::FAITHFUL)*),
                quote!(#(::dbsp::dynamic::HashRepr::hash_repr(&self.#members, state);)*),
            )
        }
        None => (quote!(false), quote!()),
    };
    // A declining impl writes nothing, and neither does a faithful one for a
    // struct with no fields.
    let state = if body.is_empty() {
        format_ident!("_state")
    } else {
        format_ident!("state")
    };

    Ok(quote! {
        impl #impl_generics ::dbsp::dynamic::HashRepr
            for #archived #ty_generics #where_clause
        {
            const FAITHFUL: bool = #is_faithful;

            #[inline]
            fn hash_repr<H: ::core::hash::Hasher>(&self, #state: &mut H) {
                #body
            }
        }
    })
}
