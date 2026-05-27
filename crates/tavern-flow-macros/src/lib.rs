//! tavern-flow-macros — proc-macro DSL for method-level event-driven orchestration.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, punctuated::Punctuated, spanned::Spanned, token::Comma, Attribute,
    DeriveInput, FnArg, ImplItem, ItemImpl, LitStr, Pat, Type,
};

// ── Helpers ──

fn extract_flow_attr(attrs: &[Attribute]) -> Option<FlowMethodAttr> {
    for attr in attrs {
        if attr.path().is_ident("start") {
            return Some(FlowMethodAttr::Start);
        }
        if attr.path().is_ident("listen") {
            return parse_listen_attr(attr);
        }
        if attr.path().is_ident("router") {
            if let Ok(lit) = attr.parse_args::<syn::LitStr>() {
                return Some(FlowMethodAttr::Router(lit.value()));
            }
        }
    }
    None
}

enum FlowMethodAttr {
    Start,
    Listen(ListenTarget),
    Router(String),
}

enum ListenTarget {
    Single(String),
    Or(Vec<String>),
    And(Vec<String>),
}

/// Parse `#[listen("name")]`, `#[listen(or("a", "b"))]`, or `#[listen(and("a", "b"))]`.
fn parse_listen_attr(attr: &Attribute) -> Option<FlowMethodAttr> {
    if !attr.path().is_ident("listen") {
        return None;
    }

    // Try to parse as a simple string: #[listen("name")]
    if let Ok(lit) = attr.parse_args::<syn::LitStr>() {
        return Some(FlowMethodAttr::Listen(ListenTarget::Single(lit.value())));
    }

    // Try to parse as or("a", "b", ...) or and("a", "b", ...)
    let content: proc_macro2::TokenStream = attr.meta.require_list().ok()?.tokens.clone();

    // Parse: ident ( string_lit , string_lit , ... )
    let parsed = syn::parse2::<ListenCall>(content).ok()?;
    match parsed.func {
        Func::Or => Some(FlowMethodAttr::Listen(ListenTarget::Or(parsed.args))),
        Func::And => Some(FlowMethodAttr::Listen(ListenTarget::And(parsed.args))),
    }
}

struct ListenCall {
    func: Func,
    args: Vec<String>,
}

enum Func {
    Or,
    And,
}

impl syn::parse::Parse for ListenCall {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let func_ident: syn::Ident = input.parse()?;
        let func = if func_ident == "or" {
            Func::Or
        } else {
            Func::And
        };

        let content;
        syn::parenthesized!(content in input);

        let args: Punctuated<syn::LitStr, syn::Token![,]> = Punctuated::parse_terminated(&content)?;
        let args: Vec<String> = args.iter().map(|s: &syn::LitStr| s.value()).collect();

        Ok(ListenCall { func, args })
    }
}

/// Strip `#[start]` and `#[listen]` attributes from a method.
fn strip_flow_attrs(attrs: &[Attribute]) -> Vec<Attribute> {
    attrs
        .iter()
        .filter(|a| !a.path().is_ident("start") && !a.path().is_ident("listen"))
        .cloned()
        .collect()
}

// ── Proc Macros ──

#[proc_macro_derive(Flow, attributes(flow))]
pub fn derive_flow(input: TokenStream) -> TokenStream {
    let _input = parse_macro_input!(input as DeriveInput);
    TokenStream::new()
}

#[proc_macro_attribute]
pub fn start(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn listen(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn router(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// `#[flow_impl(crate = "path")]` — generate FlowDispatch + Flow trait impls.
#[proc_macro_attribute]
pub fn flow_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated::<syn::Meta, Comma>::parse_terminated);
    let args_vec: Vec<_> = args.into_iter().collect();
    let crate_path = extract_crate_path(&args_vec);

    let input = parse_macro_input!(item as ItemImpl);
    let struct_name = match &*input.self_ty {
        Type::Path(tp) => tp.path.segments.last().unwrap().ident.clone(),
        _ => {
            return syn::Error::new(input.self_ty.span(), "expected simple struct type")
                .to_compile_error()
                .into();
        }
    };

    let mut methods_info: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut dispatch_arms: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut pass_through: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut wrappers: Vec<proc_macro2::TokenStream> = Vec::new();

    for (idx, item) in input.items.iter().enumerate() {
        if let ImplItem::Fn(method) = item {
            let flow_attr = extract_flow_attr(&method.attrs);

            match flow_attr {
                Some(FlowMethodAttr::Start)
                | Some(FlowMethodAttr::Listen(_))
                | Some(FlowMethodAttr::Router(_)) => {
                    let method_name = &method.sig.ident;
                    let name_str = method_name.to_string();
                    let wrapper_name = format_ident!("__flow_wrapper_{}", name_str);

                    let is_start = matches!(flow_attr, Some(FlowMethodAttr::Start));
                    let is_router = matches!(flow_attr, Some(FlowMethodAttr::Router(_)));

                    // Build listen_type from flow_attr
                    let listen_type_tokens = match &flow_attr {
                        Some(FlowMethodAttr::Listen(ListenTarget::Single(name))) => {
                            let name = name.clone();
                            quote! { #crate_path::ListenType::Single(#name.to_string()) }
                        }
                        Some(FlowMethodAttr::Listen(ListenTarget::Or(names))) => {
                            let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
                            quote! { #crate_path::ListenType::Or(vec![#(#names.to_string()),*]) }
                        }
                        Some(FlowMethodAttr::Listen(ListenTarget::And(names))) => {
                            let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
                            quote! { #crate_path::ListenType::And(vec![#(#names.to_string()),*]) }
                        }
                        _ => {
                            quote! { #crate_path::ListenType::Single(String::new()) }
                        }
                    };

                    let router_upstream = match &flow_attr {
                        Some(FlowMethodAttr::Router(upstream)) => upstream.clone(),
                        _ => String::new(),
                    };

                    methods_info.push(quote! {
                        #crate_path::MethodInfo {
                            name: #name_str.to_string(),
                            is_start: #is_start,
                            is_router: #is_router,
                            router_for: if #is_router { Some(#router_upstream.to_string()) } else { None },
                            listen_type: #listen_type_tokens,
                        }
                    });

                    // Build wrapper method: async fn wrapper(&mut self, args) -> Result<Value, FlowError>
                    let mut wrapper_inputs: Vec<FnArg> = Vec::new();
                    let mut wrapper_args: Vec<proc_macro2::TokenStream> = Vec::new();
                    let mut has_input = false;

                    for arg in &method.sig.inputs {
                        if let FnArg::Typed(pat_ty) = arg {
                            if let Pat::Ident(pi) = &*pat_ty.pat {
                                if pi.ident != "self" {
                                    has_input = true;
                                    wrapper_inputs.push(FnArg::Typed(pat_ty.clone()));
                                    wrapper_args.push(quote! { #pi });
                                }
                            }
                        }
                    }

                    let call = if has_input {
                        quote! { self.#method_name(#(#wrapper_args),*) }
                    } else {
                        quote! { self.#method_name() }
                    };

                    // Generate wrapper (router vs normal)
                    let wrapper = if is_router {
                        if has_input {
                            quote! {
                                async fn #wrapper_name(&mut self, #(#wrapper_inputs),*) -> std::result::Result<serde_json::Value, #crate_path::FlowError> {
                                    let label = #call.await;
                                    Ok(serde_json::Value::String(label))
                                }
                            }
                        } else {
                            quote! {
                                async fn #wrapper_name(&mut self) -> std::result::Result<serde_json::Value, #crate_path::FlowError> {
                                    let label = #call.await;
                                    Ok(serde_json::Value::String(label))
                                }
                            }
                        }
                    } else {
                        if has_input {
                            quote! {
                                async fn #wrapper_name(&mut self, #(#wrapper_inputs),*) -> std::result::Result<serde_json::Value, #crate_path::FlowError> {
                                    let result = #call.await?;
                                    Ok(serde_json::to_value(result).map_err(|e| #crate_path::FlowError::Serialization(e.to_string()))?)
                                }
                            }
                        } else {
                            quote! {
                                async fn #wrapper_name(&mut self) -> std::result::Result<serde_json::Value, #crate_path::FlowError> {
                                    let result = #call.await?;
                                    Ok(serde_json::to_value(result).map_err(|e| #crate_path::FlowError::Serialization(e.to_string()))?)
                                }
                            }
                        }
                    };
                    wrappers.push(wrapper);

                    // Generate dispatch arm (call wrapper)
                    let dispatch_arm = if has_input {
                        quote! {
                            #name_str => {
                                let parsed: std::result::Result<_, _> = serde_json::from_value(input);
                                match parsed {
                                    Ok(val) => Box::pin(self.#wrapper_name(val)),
                                    Err(e) => Box::pin(std::future::ready(Err(
                                        #crate_path::FlowError::Serialization(e.to_string())
                                    ))),
                                }
                            }
                        }
                    } else {
                        quote! {
                            #name_str => Box::pin(self.#wrapper_name())
                        }
                    };
                    dispatch_arms.push(dispatch_arm);

                    // Pass through original method (strip flow attrs)
                    let mut clean_method = method.clone();
                    clean_method.attrs = strip_flow_attrs(&method.attrs);
                    pass_through.push(quote! { #clean_method });
                }
                None => {
                    pass_through.push(quote! { #method });
                }
            }
        } else {
            pass_through.push(quote! { #item });
        }
    }

    let expanded = quote! {
        impl #struct_name {
            #(#pass_through)*
            #(#wrappers)*
        }

        impl #crate_path::FlowDispatch for #struct_name {
            fn dispatch(
                &mut self,
                method: &str,
                input: serde_json::Value,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<serde_json::Value, #crate_path::FlowError>> + Send + '_>> {
                match method {
                    #(#dispatch_arms),*,
                    _ => Box::pin(std::future::ready(Err(#crate_path::FlowError::MethodNotFound {
                        name: method.to_string(),
                    }))),
                }
            }
        }

        impl #crate_path::Flow for #struct_name {
            fn metadata() -> #crate_path::FlowMetadata {
                #crate_path::FlowMetadata {
                    methods: vec![#(#methods_info),*],
                }
            }
        }
    };

    expanded.into()
}

/// 从 `#[flow_impl(crate = "...")]` 中提取 crate 路径。
fn extract_crate_path(args: &[syn::Meta]) -> syn::Path {
    for meta in args {
        if let syn::Meta::NameValue(nv) = meta {
            if nv.path.is_ident("crate") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) = &nv.value
                {
                    return syn::parse_str::<syn::Path>(&s.value()).unwrap();
                }
            }
        }
    }
    syn::parse_str::<syn::Path>("tavern_flow").unwrap()
}
