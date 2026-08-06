//! Procedural macros for Feldera tuple types and utility traits.
//!
//! The `declare_tuple!` macro decides which layout to use based on tuple size
//! and the active storage format rules.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

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

#[proc_macro_derive(IsNone, attributes(interned))]
pub fn derive_not_none(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let interned = derive_interned_body(&input);
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

        #interned
    };

    TokenStream::from(expanded)
}

/// Forward `Interned` to every field, so a row reaches whichever of its leaves
/// keeps its strings in a shared side table.
///
/// `MAY_INTERN` is the disjunction over the fields, which lets a batch builder
/// skip the visit entirely for the overwhelming majority of key types.
fn derive_interned_body(input: &DeriveInput) -> proc_macro2::TokenStream {
    let ident = &input.ident;

    // Two escape hatches from the field-forwarding implementation:
    //
    // - `#[interned(opaque)]`: this type holds nothing that could reference a
    //   shared side table. For types whose fields come from other crates,
    //   which no crate here is allowed to implement the trait for.
    // - `#[interned(manual)]`: this type writes its own implementation, which
    //   is how a leaf that really does keep a side table declares itself.
    let mut opaque = false;
    let mut manual = false;
    for attr in &input.attrs {
        if attr.path().is_ident("interned") {
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("opaque") {
                    opaque = true;
                } else if meta.path.is_ident("manual") {
                    manual = true;
                }
                Ok(())
            });
        }
    }
    if manual {
        return quote! {};
    }
    if opaque {
        let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
        return quote! {
            impl #impl_generics ::dbsp::dynamic::Interned
                for #ident #ty_generics #where_clause {}
        };
    }

    let fields: Vec<syn::Member> = match &input.data {
        syn::Data::Struct(data) => data
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| match &field.ident {
                Some(name) => syn::Member::Named(name.clone()),
                None => syn::Member::Unnamed(syn::Index::from(index)),
            })
            .collect(),
        // Only structs get this derive today; anything else keeps the
        // do-nothing default rather than silently skipping its contents.
        _ => {
            let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
            return quote! {
                impl #impl_generics ::dbsp::dynamic::Interned
                    for #ident #ty_generics #where_clause {}
            };
        }
    };

    let types: Vec<&syn::Type> = match &input.data {
        syn::Data::Struct(data) => data.fields.iter().map(|field| &field.ty).collect(),
        _ => unreachable!("handled above"),
    };

    // Bound the field types rather than the type parameters. A parameter that
    // only reaches a `PhantomData` field would otherwise pick up a bound its
    // callers cannot satisfy, which is the usual trap with derived bounds.
    let mut generics = input.generics.clone();
    {
        let where_clause = generics.make_where_clause();
        for ty in &types {
            where_clause
                .predicates
                .push(syn::parse_quote!(#ty: ::dbsp::dynamic::Interned));
        }
    }
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::dbsp::dynamic::Interned for #ident #ty_generics #where_clause {
            const MAY_INTERN: bool =
                #(<#types as ::dbsp::dynamic::Interned>::MAY_INTERN ||)* false;

            fn new_intern_session()
                -> Option<Box<dyn ::dbsp::dynamic::InternSession>>
            {
                None #(.or_else(||
                    if <#types as ::dbsp::dynamic::Interned>::MAY_INTERN {
                        <#types as ::dbsp::dynamic::Interned>::new_intern_session()
                    } else {
                        None
                    }
                ))*
            }

            fn reintern(&mut self, session: &mut dyn ::dbsp::dynamic::InternSession) {
                #(::dbsp::dynamic::Interned::reintern(&mut self.#fields, session);)*
            }
        }
    }
}
