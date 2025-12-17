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

#[proc_macro_derive(IsNone)]
pub fn derive_not_none(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics ::dbsp::utils::IsNone for #ident #ty_generics #where_clause {
            #[inline]
            fn is_none(&self) -> bool {
                false
            }
        }
    };

    TokenStream::from(expanded)
}
