//! Code generation for `#[derive(OrdRepr)]`.
//!
//! The derive produces `impl OrdRepr<Foo> for ArchivedFoo`, comparing the
//! archived form of a struct or enum with an unarchived value of the same
//! type. It follows `#[derive(Ord)]`: struct fields compare in declaration
//! order, and enum variants compare by position before their payloads. A
//! type whose `Ord` is derived therefore gets an `OrdRepr` that agrees with
//! it. A type with a hand-written `Ord` needs a hand-written `OrdRepr`, and
//! so does an enum that sets a discriminant, since `#[derive(Ord)]` orders
//! its variants by discriminant value: the derive refuses such an enum
//! rather than compare by position.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{parse_quote, Data, DeriveInput, Fields, Ident, Index, Member};

use crate::archive_attrs::{all_fields, archive_bounds, omits_bounds, with_wrapper};

pub(super) fn derive_ord_repr_impl(input: DeriveInput) -> syn::Result<TokenStream2> {
    let ident = &input.ident;
    let archived = format_ident!("Archived{}", ident);

    let body = match &input.data {
        Data::Struct(data) => struct_body(&data.fields),
        Data::Enum(data) => {
            // `#[derive(Ord)]` orders variants by discriminant value, which
            // is their declaration position only while no variant sets one.
            if let Some(variant) = data.variants.iter().find(|v| v.discriminant.is_some()) {
                return Err(syn::Error::new_spanned(
                    variant,
                    "OrdRepr cannot be derived for an enum with an explicit discriminant, \
                     whose `Ord` orders variants by discriminant value rather than by \
                     declaration position; implement it by hand",
                ));
            }
            enum_body(ident, &archived, data.variants.iter().collect())
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                ident,
                "OrdRepr cannot be derived for a union",
            ));
        }
    };

    // The impl needs the archived type's own bounds, which rkyv's derive
    // builds from the input's where clause, `#[archive(bound(archive = ..))]`
    // and `T: Archive` for every field type `T`. On top of those, the impl
    // bounds each field type rather than each type parameter, as rkyv does:
    // `ArchivedFoo<T>` compares against `Foo<T>` whenever every field's
    // archived form compares against the field. A field marked
    // `#[omit_bounds]` (rkyv's attribute for recursive fields) contributes no
    // bound, which keeps a recursive type from requiring itself.
    let mut generics = input.generics.clone();
    {
        let where_clause = generics.make_where_clause();
        where_clause.predicates.extend(archive_bounds(&input)?);
        for field in all_fields(&input.data) {
            if let Some(with) = with_wrapper(field) {
                return Err(syn::Error::new_spanned(
                    with,
                    "OrdRepr cannot be derived for a field with a `#[with]` wrapper; \
                     implement it by hand",
                ));
            }
            if omits_bounds(field) {
                continue;
            }
            let ty = &field.ty;
            where_clause
                .predicates
                .push(parse_quote!(#ty: ::rkyv::Archive));
            where_clause.predicates.push(parse_quote!(
                <#ty as ::rkyv::Archive>::Archived: ::dbsp::dynamic::OrdRepr<#ty>
            ));
        }
    }
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::dbsp::dynamic::OrdRepr<#ident #ty_generics>
            for #archived #ty_generics #where_clause
        {
            // A single-variant enum leaves the cross-variant arm unreachable.
            #[allow(unreachable_patterns)]
            fn ord_cmp(&self, other: &#ident #ty_generics) -> ::core::cmp::Ordering {
                #body
            }
        }
    })
}

/// Compares `self.<field>` with `other.<field>` for each field in turn, and
/// returns the first difference.
fn compare_fields(lhs: &[TokenStream2], rhs: &[TokenStream2]) -> TokenStream2 {
    quote! {
        #(
            match ::dbsp::dynamic::OrdRepr::ord_cmp(#lhs, #rhs) {
                ::core::cmp::Ordering::Equal => {}
                unequal => return unequal,
            }
        )*
        ::core::cmp::Ordering::Equal
    }
}

fn struct_body(fields: &Fields) -> TokenStream2 {
    let members: Vec<Member> = fields
        .iter()
        .enumerate()
        .map(|(index, field)| match &field.ident {
            Some(name) => Member::Named(name.clone()),
            None => Member::Unnamed(Index::from(index)),
        })
        .collect();
    let lhs: Vec<_> = members.iter().map(|m| quote!(&self.#m)).collect();
    let rhs: Vec<_> = members.iter().map(|m| quote!(&other.#m)).collect();
    compare_fields(&lhs, &rhs)
}

fn enum_body(ident: &Ident, archived: &Ident, variants: Vec<&syn::Variant>) -> TokenStream2 {
    // Matching variants compare their payloads; otherwise the variant declared
    // earlier is the lesser, as with `#[derive(Ord)]`.
    let payload_arms = variants.iter().map(|variant| {
        let name = &variant.ident;
        match &variant.fields {
            Fields::Unit => quote! {
                (#archived::#name, #ident::#name) => ::core::cmp::Ordering::Equal,
            },
            Fields::Unnamed(fields) => {
                let lhs: Vec<Ident> = (0..fields.unnamed.len())
                    .map(|index| format_ident!("lhs{}", index))
                    .collect();
                let rhs: Vec<Ident> = (0..fields.unnamed.len())
                    .map(|index| format_ident!("rhs{}", index))
                    .collect();
                let body = compare_fields(
                    &lhs.iter().map(|b| quote!(#b)).collect::<Vec<_>>(),
                    &rhs.iter().map(|b| quote!(#b)).collect::<Vec<_>>(),
                );
                quote! {
                    (#archived::#name(#(#lhs),*), #ident::#name(#(#rhs),*)) => { #body }
                }
            }
            Fields::Named(fields) => {
                let names: Vec<&Ident> = fields
                    .named
                    .iter()
                    .map(|field| field.ident.as_ref().expect("named field"))
                    .collect();
                let lhs: Vec<Ident> = names.iter().map(|n| format_ident!("lhs_{}", n)).collect();
                let rhs: Vec<Ident> = names.iter().map(|n| format_ident!("rhs_{}", n)).collect();
                let body = compare_fields(
                    &lhs.iter().map(|b| quote!(#b)).collect::<Vec<_>>(),
                    &rhs.iter().map(|b| quote!(#b)).collect::<Vec<_>>(),
                );
                quote! {
                    (
                        #archived::#name { #(#names: #lhs),* },
                        #ident::#name { #(#names: #rhs),* },
                    ) => { #body }
                }
            }
        }
    });

    let self_positions = variants.iter().enumerate().map(|(position, variant)| {
        let name = &variant.ident;
        quote!(#archived::#name { .. } => #position,)
    });
    let other_positions = variants.iter().enumerate().map(|(position, variant)| {
        let name = &variant.ident;
        quote!(#ident::#name { .. } => #position,)
    });

    quote! {
        match (self, other) {
            #(#payload_arms)*
            _ => {
                let lhs: usize = match self { #(#self_positions)* };
                let rhs: usize = match other { #(#other_positions)* };
                lhs.cmp(&rhs)
            }
        }
    }
}
