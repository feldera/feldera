//! Reading the rkyv attributes that `#[derive(OrdRepr)]` and
//! `#[derive(HashRepr)]` both have to account for.
//!
//! Each of those derives writes an impl for a type that rkyv's own derive
//! generated, so each has to repeat the bounds rkyv put on that type -- the
//! input's where clause and `#[archive(bound(archive = ..))]` -- and to spot
//! the two field attributes that change what the archived field is:
//! `#[with]`, which replaces its type with a wrapper's, and
//! `#[omit_bounds]`, which rkyv uses to stop a recursive type requiring
//! itself.

use proc_macro2::TokenStream as TokenStream2;
use syn::{meta::ParseNestedMeta, Data, DeriveInput, Field, LitStr, WherePredicate};

/// The predicates of `#[archive(bound(archive = "..."))]`, which rkyv adds to
/// the archived type's where clause.
pub(super) fn archive_bounds(input: &DeriveInput) -> syn::Result<Vec<WherePredicate>> {
    let mut predicates = Vec::new();
    for attr in &input.attrs {
        if !attr.path().is_ident("archive") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("bound") {
                return skip_meta_value(&meta);
            }
            // `bound(..)` may be empty, which `parse_nested_meta` rejects.
            let content;
            syn::parenthesized!(content in meta.input);
            let bounds: TokenStream2 = content.parse()?;
            let parser = syn::meta::parser(|meta| {
                let value: LitStr = meta.value()?.parse()?;
                if meta.path.is_ident("archive") {
                    let clause: syn::WhereClause =
                        syn::parse_str(&format!("where {}", value.value()))?;
                    predicates.extend(clause.predicates);
                }
                Ok(())
            });
            syn::parse::Parser::parse2(parser, bounds)
        })?;
    }
    Ok(predicates)
}

/// Consumes whatever follows an `#[archive(..)]` key that these derives do
/// not use, such as `compare(PartialEq)` or `as = "Self"`.
fn skip_meta_value(meta: &ParseNestedMeta) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) {
        meta.value()?.parse::<syn::Expr>()?;
    } else if meta.input.peek(syn::token::Paren) {
        let content;
        syn::parenthesized!(content in meta.input);
        content.parse::<TokenStream2>()?;
    }
    Ok(())
}

/// Every field of a struct, or of every variant of an enum.
pub(super) fn all_fields(data: &Data) -> Vec<&Field> {
    match data {
        Data::Struct(data) => data.fields.iter().collect(),
        Data::Enum(data) => data
            .variants
            .iter()
            .flat_map(|variant| variant.fields.iter())
            .collect(),
        Data::Union(_) => Vec::new(),
    }
}

/// Whether the field carries rkyv's `#[omit_bounds]`, which it uses to keep a
/// recursive type from requiring itself.
pub(super) fn omits_bounds(field: &Field) -> bool {
    field
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("omit_bounds"))
}

/// The field's `#[with = ..]` wrapper, if it has one.  A wrapper replaces the
/// field's archived form with another type, so neither derive can tell what
/// the archived field holds.
pub(super) fn with_wrapper(field: &Field) -> Option<&syn::Attribute> {
    field.attrs.iter().find(|attr| attr.path().is_ident("with"))
}
