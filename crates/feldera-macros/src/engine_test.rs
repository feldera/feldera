//! The `#[engine_test]` attribute, which registers a connector suite case so that the test
//! harness of every engine runs it.

use proc_macro2::{TokenStream, TokenTree};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, FnArg, ItemFn, Lit, LitStr, Meta, MetaList, Token, Type};

/// What the attribute's arguments ask for.
#[derive(Default)]
struct CaseOptions {
    /// `Serial` or `Parallel`, and the key of the lock.
    lock: Option<(proc_macro2::Ident, String)>,
    ignore: bool,
    ignore_reason: Option<LitStr>,
    ignore_if: Option<TokenStream>,
    should_panic: Option<LitStr>,
    cfg: Option<TokenStream>,
    /// The capabilities that `requires(...)` names.
    requires: Vec<proc_macro2::Ident>,
    /// The services that `services(...)` names.
    services: Vec<proc_macro2::Ident>,
}

/// Returns the string literal of a `name = "text"` argument.
fn string_value(value: &Expr) -> syn::Result<LitStr> {
    match value {
        Expr::Lit(ExprLit {
            lit: Lit::Str(text),
            ..
        }) => Ok(text.clone()),
        other => Err(syn::Error::new(other.span(), "expected a string literal")),
    }
}

/// Splits `predicate, "reason"` into the predicate and the reason, if the last
/// top-level item is a string literal.
fn split_reason(tokens: TokenStream) -> syn::Result<(TokenStream, Option<LitStr>)> {
    let items: Vec<TokenTree> = tokens.into_iter().collect();
    let last_comma = items
        .iter()
        .rposition(|item| matches!(item, TokenTree::Punct(punct) if punct.as_char() == ','));
    if let Some(comma) = last_comma {
        let tail: TokenStream = items[comma + 1..].iter().cloned().collect();
        if let Ok(reason) = syn::parse2::<LitStr>(tail) {
            return Ok((items[..comma].iter().cloned().collect(), Some(reason)));
        }
    }
    Ok((items.into_iter().collect(), None))
}

/// Returns the names in `list`, such as `Storage` and `Checkpoints` in
/// `requires(Storage, Checkpoints)`.
fn names(list: &MetaList) -> syn::Result<Punctuated<proc_macro2::Ident, Token![,]>> {
    Punctuated::parse_terminated.parse2(list.tokens.clone())
}

impl CaseOptions {
    fn parse(args: TokenStream) -> syn::Result<Self> {
        let mut options = CaseOptions::default();
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(args)?;
        for meta in metas {
            match &meta {
                Meta::Path(path) if path.is_ident("serial") => {
                    options.lock = Some((format_ident!("Serial"), String::new()));
                }
                Meta::List(list)
                    if list.path.is_ident("serial") || list.path.is_ident("parallel") =>
                {
                    let key: proc_macro2::Ident = syn::parse2(list.tokens.clone())?;
                    let kind = if list.path.is_ident("serial") {
                        format_ident!("Serial")
                    } else {
                        format_ident!("Parallel")
                    };
                    options.lock = Some((kind, key.to_string()));
                }
                Meta::Path(path) if path.is_ident("ignore") => options.ignore = true,
                Meta::NameValue(pair) if pair.path.is_ident("ignore") => {
                    options.ignore = true;
                    options.ignore_reason = Some(string_value(&pair.value)?);
                }
                Meta::NameValue(pair) if pair.path.is_ident("should_panic") => {
                    options.should_panic = Some(string_value(&pair.value)?);
                }
                Meta::List(list) if list.path.is_ident("ignore_if") => {
                    let (predicate, reason) = split_reason(list.tokens.clone())?;
                    options.ignore_if = Some(predicate);
                    options.ignore_reason = reason;
                }
                Meta::List(list) if list.path.is_ident("cfg") => {
                    options.cfg = Some(list.tokens.clone());
                }
                Meta::List(list) if list.path.is_ident("requires") => {
                    options.requires.extend(names(list)?);
                }
                Meta::List(list) if list.path.is_ident("services") => {
                    options.services.extend(names(list)?);
                }
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "expected `serial`, `serial(key)`, `parallel(key)`, `ignore`, \
                         `ignore = \"reason\"`, `ignore_if(predicate)`, \
                         `should_panic = \"text\"`, `requires(Capability, ...)`, \
                         `services(Service, ...)`, or `cfg(predicate)`",
                    ));
                }
            }
        }
        Ok(options)
    }
}

/// Returns `Connector` or `Pipeline`, by the engine trait that the case's only argument
/// names.
fn engine_kind(function: &ItemFn) -> syn::Result<proc_macro2::Ident> {
    let error = || {
        syn::Error::new(
            function.sig.span(),
            "an engine test takes one argument, `&dyn ConnectorTestEngine` or \
             `&dyn PipelineTestEngine`",
        )
    };
    let mut inputs = function.sig.inputs.iter();
    let (Some(FnArg::Typed(argument)), None) = (inputs.next(), inputs.next()) else {
        return Err(error());
    };
    let Type::Reference(reference) = argument.ty.as_ref() else {
        return Err(error());
    };
    let Type::TraitObject(object) = reference.elem.as_ref() else {
        return Err(error());
    };
    for bound in &object.bounds {
        let syn::TypeParamBound::Trait(bound) = bound else {
            continue;
        };
        match bound.path.segments.last() {
            Some(segment) if segment.ident == "ConnectorTestEngine" => {
                return Ok(format_ident!("Connector"));
            }
            Some(segment) if segment.ident == "PipelineTestEngine" => {
                return Ok(format_ident!("Pipeline"));
            }
            _ => {}
        }
    }
    Err(error())
}

/// Keeps `item`, a suite case, and registers it with `inventory` for the harness of every
/// engine. The registration names items of the crate that defines the case, `dbsp_adapters`.
pub fn engine_test(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let options = CaseOptions::parse(args)?;
    let function: ItemFn = syn::parse2(item)?;
    let kind = engine_kind(&function)?;
    let name = &function.sig.ident;
    let name_text = name.to_string();
    // The case's own `cfg` attributes, so that the registration goes away with the case.
    let case_cfgs = function
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("cfg"));
    let registration_cfg = options
        .cfg
        .iter()
        .map(|predicate| quote!(#[cfg(#predicate)]));
    let lock = match &options.lock {
        Some((kind, key)) => {
            quote!(::core::option::Option::Some(crate::test::CaseLock::#kind(#key)))
        }
        None => quote!(::core::option::Option::None),
    };
    let ignore = match (&options.ignore_if, options.ignore) {
        (_, true) => quote!(true),
        (Some(predicate), false) => quote!(::core::cfg!(#predicate)),
        (None, false) => quote!(false),
    };
    let ignore_reason = match &options.ignore_reason {
        Some(reason) => quote!(::core::option::Option::Some(#reason)),
        None => quote!(::core::option::Option::None),
    };
    let should_panic = match &options.should_panic {
        Some(text) => quote!(::core::option::Option::Some(#text)),
        None => quote!(::core::option::Option::None),
    };
    let requires = &options.requires;
    let services = &options.services;
    Ok(quote! {
        #function

        #(#case_cfgs)*
        #(#registration_cfg)*
        ::inventory::submit! {
            crate::test::EngineCase {
                module: ::core::module_path!(),
                name: #name_text,
                run: crate::test::CaseFn::#kind(#name),
                lock: #lock,
                ignore: #ignore,
                ignore_reason: #ignore_reason,
                should_panic: #should_panic,
                requires: &[#(crate::test::Capability::#requires),*],
                services: &[#(crate::test::Service::#services),*],
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::{engine_test, CaseOptions};

    /// Returns the text of `tokens` without spaces, which `quote!` puts between tokens.
    fn compact(tokens: impl ToString) -> String {
        tokens.to_string().replace(' ', "")
    }

    /// `requires` and `services` collect their names, beside the other options and over
    /// several lists.
    #[test]
    fn requires_and_services_collect_their_names() {
        let options = CaseOptions::parse(quote!(
            serial,
            requires(Storage, Checkpoints),
            services(Kafka),
            requires(Bootstrap)
        ))
        .unwrap();
        let names = |names: &[proc_macro2::Ident]| names.iter().map(compact).collect::<Vec<_>>();
        assert_eq!(
            names(&options.requires),
            ["Storage", "Checkpoints", "Bootstrap"]
        );
        assert_eq!(names(&options.services), ["Kafka"]);
        assert!(options.lock.is_some());
    }

    /// The registration names each capability and each service as a variant of the
    /// harness's enums, and a case without them registers empty lists.
    #[test]
    fn the_registration_names_capabilities_and_services() {
        let case = quote!(
            pub fn case(engine: &dyn PipelineTestEngine) {}
        );
        let registration = compact(
            engine_test(
                quote!(requires(Storage), services(Kafka, Postgres)),
                case.clone(),
            )
            .unwrap(),
        );
        assert!(
            registration.contains("requires:&[crate::test::Capability::Storage],"),
            "{registration}"
        );
        assert!(
            registration.contains(
                "services:&[crate::test::Service::Kafka,crate::test::Service::Postgres],"
            ),
            "{registration}"
        );
        let plain = compact(engine_test(quote!(serial), case).unwrap());
        assert!(plain.contains("requires:&[],services:&[],"), "{plain}");
    }

    /// A capability is a name, so a string or a path in its place is an error.
    #[test]
    fn a_capability_must_be_a_name() {
        assert!(CaseOptions::parse(quote!(requires("storage"))).is_err());
        assert!(CaseOptions::parse(quote!(services(crate::Kafka))).is_err());
    }
}
