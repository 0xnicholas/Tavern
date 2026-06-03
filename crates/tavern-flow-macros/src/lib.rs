//! tavern-flow-macros — proc-macro DSL for method-level event-driven orchestration.
//! V0.4: Expands to `tavern_comp::Workflow` + `tavern_comp::FlowStepExecutor`.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::{
    Attribute, DeriveInput, FnArg, ImplItem, ItemImpl, Pat, Type, parse_macro_input,
    punctuated::Punctuated, spanned::Spanned, token::Comma,
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
        if attr.path().is_ident("router")
            && let Ok(lit) = attr.parse_args::<syn::LitStr>()
        {
            return Some(FlowMethodAttr::Router(lit.value()));
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
        let func = match func_ident.to_string().as_str() {
            "or" => Func::Or,
            "and" => Func::And,
            other => {
                return Err(syn::Error::new(
                    func_ident.span(),
                    format!("expected `or` or `and`, got `{}`", other),
                ));
            }
        };

        let content;
        syn::parenthesized!(content in input);

        let args: Punctuated<syn::LitStr, syn::Token![,]> = Punctuated::parse_terminated(&content)?;
        let args: Vec<String> = args.iter().map(|s: &syn::LitStr| s.value()).collect();

        Ok(ListenCall { func, args })
    }
}

/// Strip `#[start]`, `#[listen]`, and `#[router]` attributes from a method.
fn strip_flow_attrs(attrs: &[Attribute]) -> Vec<Attribute> {
    attrs
        .iter()
        .filter(|a| {
            !a.path().is_ident("start")
                && !a.path().is_ident("listen")
                && !a.path().is_ident("router")
        })
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

/// `#[flow_impl(crate = "path")]` — generate FlowStepExecutor + __workflow_definition + run().
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

    // Collect all method names for prefix auto-detection
    let method_names: HashSet<String> = input
        .items
        .iter()
        .filter_map(|item| {
            if let ImplItem::Fn(method) = item {
                Some(method.sig.ident.to_string())
            } else {
                None
            }
        })
        .collect();

    let mut last_output_step_id: Option<String> = None;
    let mut workflow_steps: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut dispatch_arms: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut pass_through: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut wrappers: Vec<proc_macro2::TokenStream> = Vec::new();

    for item in &input.items {
        if let ImplItem::Fn(method) = item {
            let flow_attr = extract_flow_attr(&method.attrs);

            match flow_attr {
                Some(FlowMethodAttr::Start)
                | Some(FlowMethodAttr::Listen(_))
                | Some(FlowMethodAttr::Router(_)) => {
                    let method_name = &method.sig.ident;
                    let name_str = method_name.to_string();
                    let wrapper_name = format_ident!("__flow_wrapper_{}", name_str);

                    let is_router = matches!(flow_attr, Some(FlowMethodAttr::Router(_)));

                    // ── Build Step for __workflow_definition ──
                    if !is_router {
                        last_output_step_id = Some(name_str.clone());
                    }

                    let step_id = if is_router {
                        format!("__router__{}", name_str)
                    } else {
                        name_str.clone()
                    };

                    let (depends_on, or_depends_on, router_config) = match &flow_attr {
                        Some(FlowMethodAttr::Start) => (vec![], vec![], None),
                        Some(FlowMethodAttr::Listen(ListenTarget::Single(name))) => {
                            if method_names.contains(name) {
                                // Direct method dependency → OR with no prefix
                                (vec![], vec![name.clone()], None)
                            } else {
                                // Router label → OR with __label__ prefix
                                (vec![], vec![format!("__label__{}", name)], None)
                            }
                        }
                        Some(FlowMethodAttr::Listen(ListenTarget::Or(names))) => {
                            // OR with prefix detection: method names → no prefix, labels → __label__ prefix
                            let prefixed: Vec<String> = names
                                .iter()
                                .map(|n| {
                                    if method_names.contains(n) {
                                        n.clone()
                                    } else {
                                        format!("__label__{}", n)
                                    }
                                })
                                .collect();
                            (vec![], prefixed, None)
                        }
                        Some(FlowMethodAttr::Listen(ListenTarget::And(names))) => {
                            // AND with prefix detection: method names → no prefix, labels → __label__ prefix
                            let prefixed: Vec<String> = names
                                .iter()
                                .map(|n| {
                                    if method_names.contains(n) {
                                        n.clone()
                                    } else {
                                        format!("__label__{}", n)
                                    }
                                })
                                .collect();
                            (prefixed, vec![], None)
                        }
                        Some(FlowMethodAttr::Router(upstream)) => (
                            vec![upstream.clone()],
                            vec![],
                            Some(
                                quote! { Some(tavern_comp::RouterConfig { upstream: #upstream.to_string() }) },
                            ),
                        ),
                        None => unreachable!(),
                    };

                    let router_config_tokens = router_config.unwrap_or(quote! { None });
                    let output_key_tokens = if is_router {
                        quote! { None }
                    } else {
                        quote! { Some(#step_id.to_string()) }
                    };

                    let depends_on_tokens: Vec<proc_macro2::TokenStream> = depends_on
                        .iter()
                        .map(|d| quote! { #d.to_string() })
                        .collect();
                    let or_depends_on_tokens: Vec<proc_macro2::TokenStream> = or_depends_on
                        .iter()
                        .map(|d| quote! { #d.to_string() })
                        .collect();

                    workflow_steps.push(quote! {
                        tavern_comp::Step {
                            id: #step_id.to_string(),
                            agent_id: tavern_comp::FLOW_AGENT_ID.to_string(),
                            task: #name_str.to_string(),
                            depends_on: vec![#(#depends_on_tokens),*],
                            or_depends_on: vec![#(#or_depends_on_tokens),*],
                            output_key: #output_key_tokens,
                            router: #router_config_tokens,
                            ..tavern_comp::Step::default()
                        }
                    });

                    // ── Build wrapper inputs ──
                    let mut wrapper_inputs: Vec<FnArg> = Vec::new();
                    let mut wrapper_args: Vec<proc_macro2::TokenStream> = Vec::new();
                    let mut has_input = false;

                    for arg in &method.sig.inputs {
                        if let FnArg::Typed(pat_ty) = arg
                            && let Pat::Ident(pi) = &*pat_ty.pat
                            && pi.ident != "self"
                        {
                            has_input = true;
                            wrapper_inputs.push(FnArg::Typed(pat_ty.clone()));
                            wrapper_args.push(quote! { #pi });
                        }
                    }

                    let call = if has_input {
                        quote! { self.#method_name(#(#wrapper_args),*) }
                    } else {
                        quote! { self.#method_name() }
                    };

                    // Detect router return type (Vec<String> vs String)
                    let router_returns_vec = is_router && {
                        if let syn::ReturnType::Type(_, ty) = &method.sig.output {
                            if let Type::Path(tp) = ty.as_ref() {
                                let s = quote! { #tp }.to_string();
                                s.contains("Vec") && s.contains("String")
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    };

                    // Generate wrapper (returns Result<Value, String> for FlowStepExecutor)
                    let wrapper = if is_router {
                        let body = if router_returns_vec {
                            quote! { let labels: Vec<String> = #call.await; Ok(serde_json::Value::Array(labels.into_iter().map(serde_json::Value::String).collect())) }
                        } else {
                            quote! { let label = #call.await; Ok(serde_json::Value::String(label)) }
                        };
                        if has_input {
                            quote! { async fn #wrapper_name(&mut self, #(#wrapper_inputs),*) -> std::result::Result<serde_json::Value, String> { #body } }
                        } else {
                            quote! { async fn #wrapper_name(&mut self) -> std::result::Result<serde_json::Value, String> { #body } }
                        }
                    } else if has_input {
                        quote! {
                            async fn #wrapper_name(&mut self, #(#wrapper_inputs),*) -> std::result::Result<serde_json::Value, String> {
                                let result = #call.await.map_err(|e| e.to_string())?;
                                Ok(serde_json::to_value(result).map_err(|e| e.to_string())?)
                            }
                        }
                    } else {
                        quote! {
                            async fn #wrapper_name(&mut self) -> std::result::Result<serde_json::Value, String> {
                                let result = #call.await.map_err(|e| e.to_string())?;
                                Ok(serde_json::to_value(result).map_err(|e| e.to_string())?)
                            }
                        }
                    };
                    wrappers.push(wrapper);

                    // ── Generate dispatch arm for FlowStepExecutor ──
                    let dispatch_step_id = if is_router {
                        format!("__router__{}", name_str)
                    } else {
                        name_str.clone()
                    };

                    let dispatch_arm = if has_input {
                        quote! {
                            #dispatch_step_id => {
                                let parsed: std::result::Result<_, _> = serde_json::from_value(input);
                                match parsed {
                                    Ok(val) => Box::pin(self.#wrapper_name(val)),
                                    Err(e) => Box::pin(std::future::ready(Err(e.to_string()))),
                                }
                            }
                        }
                    } else {
                        quote! {
                            #dispatch_step_id => Box::pin(self.#wrapper_name())
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

    let last_step_str = last_output_step_id.as_deref().unwrap_or("");

    let expanded = quote! {
        impl #struct_name {
            #(#pass_through)*
            #(#wrappers)*

            /// V0.4: Build the workflow definition from proc-macro attributes.
            fn __workflow_definition() -> tavern_comp::Workflow {
                tavern_comp::Workflow {
                    id: stringify!(#struct_name).to_string(),
                    name: stringify!(#struct_name).to_string(),
                    description: None,
                    steps: vec![#(#workflow_steps),*],
                    inputs: vec![],
                    outputs: vec![],
                    process: tavern_comp::Process::Sequential,
                    planning: None,
                    webhook: None,
                    schedule: None,
                    schedule_inputs: serde_json::Value::Null,
                }
            }

            /// V0.4: Run the flow synchronously via the Comp engine.
            pub async fn run(self, inputs: serde_json::Value) -> std::result::Result<serde_json::Value, #crate_path::FlowError> {
                let workflow = Self::__workflow_definition();
                let executor = std::sync::Arc::new(tokio::sync::Mutex::new(self));
                let engine = tavern_comp::WorkflowEngine::new_with_flow_executor(executor);
                let result = engine.run(&workflow, inputs).await
                    .map_err(|e| #crate_path::FlowError::Other(e.to_string()))?;
                // Return terminal step output (last non-router step)
                let last_step_id: &str = #last_step_str;
                if let Some(step_result) = result.step_results.get(last_step_id) {
                    if let Some(ref output) = step_result.output {
                        return Ok(output.clone());
                    }
                }
                Ok(result.outputs)
            }
        }

        impl tavern_comp::FlowStepExecutor for #struct_name {
            fn execute_step(
                &mut self,
                step_id: &str,
                input: serde_json::Value,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<serde_json::Value, String>> + Send + '_>> {
                match step_id {
                    #(#dispatch_arms),*,
                    _ => Box::pin(std::future::ready(Err(format!("method not found: {}", step_id)))),
                }
            }
        }
    };

    expanded.into()
}

/// Extract crate path from `#[flow_impl(crate = "...")]`.
fn extract_crate_path(args: &[syn::Meta]) -> syn::Path {
    for meta in args {
        if let syn::Meta::NameValue(nv) = meta
            && nv.path.is_ident("crate")
            && let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) = &nv.value
        {
            return syn::parse_str::<syn::Path>(&s.value()).unwrap();
        }
    }
    syn::parse_str::<syn::Path>("tavern_flow").unwrap()
}
