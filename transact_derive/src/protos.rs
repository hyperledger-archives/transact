/*
* Copyright 2018 Bitwise IO, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*

*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
* -----------------------------------------------------------------------------
*/

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, ToTokens};
use syn::{
    Data, DeriveInput, Error as SynError, Field, Fields, GenericArgument, Ident, Lit, Meta,
    NestedMeta, PathArguments, Type, Variant,
};

pub fn generate_from_proto(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    match derive_input.clone().data {
        Data::Struct(_) => generate_from_proto_for_struct(derive_input),
        Data::Enum(_) => generate_from_proto_for_enum(derive_input),
        _ => Err(SynError::new_spanned(
            derive_input.clone().into_token_stream(),
            "Deriving from_proto is incompatible with this structure",
        )),
    }
}

pub fn generate_from_native(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    match derive_input.clone().data {
        Data::Struct(_) => generate_from_native_for_struct(derive_input),
        Data::Enum(_) => generate_from_native_for_enum(derive_input),
        _ => Err(SynError::new_spanned(
            derive_input.clone().into_token_stream(),
            "Deriving from_native is incompatible with this structure",
        )),
    }
}

pub fn generate_into_proto(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    let data_name = derive_input.ident.clone();
    let proto_type = extract_proto_type(derive_input)?;

    Ok(quote! {
        impl IntoProto<#(#proto_type)*> for #data_name {}
    })
}

pub fn generate_into_native(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    let data_name = derive_input.ident.clone();
    let proto_type = extract_proto_type(derive_input)?;

    Ok(quote! {
        impl IntoNative<#data_name> for #(#proto_type)* {}
    })
}

pub fn generate_into_bytes(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    let struct_name = derive_input.ident.clone();

    Ok(quote! {
        impl IntoBytes for #struct_name {
            fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
                let proto = self.into_proto()?;
                let bytes = proto.write_to_bytes().map_err(|_| {
                    ProtoConversionError::SerializationError(format!(
                        "Unable to get {} from Bytes",
                        stringify!(#struct_name)
                    ))
                })?;
                Ok(bytes)
            }
        }
    })
}

pub fn generate_from_bytes(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    let struct_name = derive_input.ident.clone();
    let proto_type = extract_proto_type(derive_input)?;

    Ok(quote! {
        impl FromBytes<#struct_name> for #struct_name {
            fn from_bytes(bytes: &[u8]) -> Result<#struct_name, ProtoConversionError> {
                let proto: #(#proto_type)* = protobuf::parse_from_bytes(bytes).map_err(|_| {
                    ProtoConversionError::SerializationError(format!(
                        "Unable to get {} from bytes",
                        stringify!(#struct_name)
                    ))
                })?;

                proto.into_native()
            }
        }
    })
}

fn generate_from_native_for_struct(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    let struct_name = derive_input.ident.clone();
    let signature_type = extract_proto_type(derive_input.clone())?;
    let init_type = signature_type.clone();
    let fields = extract_native_fields(derive_input.clone())?;

    Ok(quote! {
        impl FromNative<#struct_name> for #(#signature_type)* {
            fn from_native(native: #struct_name) -> Result<Self, ProtoConversionError> {
                let mut proto =  #(#init_type)*::new();
                #(#fields)*
                Ok(proto)
            }
        }
    })
}

fn generate_from_native_for_enum(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    let enum_name = derive_input.ident.clone();
    let generic_type = extract_proto_type(derive_input.clone())?;
    let signature_type = generic_type.clone();

    let variants = get_enum_variants(derive_input.clone())?
        .iter()
        .map(|v| {
            let ident = v.ident.clone();
            quote!(#enum_name::#ident)
        })
        .collect::<Vec<TokenStream2>>();

    let proto_variants = extract_proto_enum_variants(derive_input.clone())?
        .iter()
        .map(|variant| {
            let generic_type = extract_proto_type(derive_input.clone())?;
            Ok(quote!(#(#generic_type)*::#variant))
        })
        .collect::<Result<Vec<TokenStream2>, SynError>>()?;

    Ok(quote! {
        impl FromNative<#enum_name> for #(#signature_type)* {
            fn from_native(native: #enum_name) -> Result<Self, ProtoConversionError> {
                match native {
                    #(#variants => Ok(#proto_variants)),*
                }
            }
        }
    })
}

fn generate_from_proto_for_struct(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    let struct_name = derive_input.ident.clone();
    let generic_type = extract_proto_type(derive_input.clone())?;
    let signature_type = generic_type.clone();
    let fields = extract_proto_fields(derive_input.clone())?;

    Ok(quote! {
        impl FromProto<#(#generic_type)*> for #struct_name {
            fn from_proto(proto: #(#signature_type)*) -> Result<Self, ProtoConversionError> {
                Ok(#struct_name {
                    #(#fields),*
                })
            }
        }
    })
}

fn generate_from_proto_for_enum(derive_input: DeriveInput) -> Result<TokenStream2, SynError> {
    let enum_name = derive_input.ident.clone();
    let generic_type = extract_proto_type(derive_input.clone())?;
    let signature_type = generic_type.clone();
    let variants = get_enum_variants(derive_input.clone())?
        .iter()
        .map(|v| {
            let ident = v.ident.clone();
            quote!(#enum_name::#ident)
        })
        .collect::<Vec<TokenStream2>>();

    let proto_variants = extract_proto_enum_variants(derive_input.clone())?
        .iter()
        .map(|variant| {
            let generic_type = extract_proto_type(derive_input.clone())?;
            Ok(quote!(#(#generic_type)*::#variant))
        })
        .collect::<Result<Vec<TokenStream2>, SynError>>()?;

    Ok(quote! {
        impl FromProto<#(#generic_type)*> for #enum_name {
            fn from_proto(proto: #(#signature_type)*) -> Result<Self, ProtoConversionError> {
                match proto {
                    #(#proto_variants => Ok(#variants)),*
                }
            }
        }
    })
}

fn extract_proto_type(derive_input: DeriveInput) -> Result<Vec<TokenStream2>, SynError> {
    for attr in derive_input.clone().attrs {
        let segment = if let Some(segment) = attr.path.segments.first() {
            segment
        } else {
            continue;
        };

        if segment.into_value().ident != Ident::new("proto_type", Span::call_site()) {
            continue;
        }

        let meta_name_value = if let Ok(Meta::NameValue(nv)) = attr.parse_meta() {
            nv
        } else {
            continue;
        };

        if let Lit::Str(s) = meta_name_value.lit {
            return Ok(path_to_token_stream(&s.value()));
        } else {
            continue;
        }
    }

    Err(SynError::new_spanned(
        derive_input.clone().into_token_stream(),
        "A protobuf message type is required",
    ))
}

fn extract_native_fields(derive_input: DeriveInput) -> Result<Vec<TokenStream2>, SynError> {
    let fields = get_struct_fields(derive_input.clone())?;

    fields
        .iter()
        .map(make_native_field_token_stream)
        .collect::<Result<Vec<TokenStream2>, SynError>>()
}

fn extract_proto_fields(derive_input: DeriveInput) -> Result<Vec<TokenStream2>, SynError> {
    let fields = get_struct_fields(derive_input.clone())?;

    fields
        .iter()
        .map(make_proto_field_token_stream)
        .collect::<Result<Vec<TokenStream2>, SynError>>()
}

fn extract_proto_enum_variants(derive_input: DeriveInput) -> Result<Vec<Ident>, SynError> {
    let variants = get_enum_variants(derive_input.clone())?;

    variants
        .iter()
        .map(make_proto_enum_variant)
        .collect::<Result<Vec<Ident>, SynError>>()
}

fn make_proto_field_token_stream(field: &Field) -> Result<TokenStream2, SynError> {
    for attr in field.attrs.clone() {
        let meta = if let Ok(meta) = attr.parse_meta() {
            meta
        } else {
            continue;
        };

        if meta.name() == Ident::new("from_proto_impl", Span::call_site()) {
            return parse_proto_meta(field, &meta);
        }
    }

    let field_name = field.ident.clone().unwrap();
    let proto_getter_name = Ident::new(
        &format!("get_{}", field_name.to_string()),
        Span::call_site(),
    );

    if is_vec(&field.ty) {
        Ok(quote! {
            #field_name: proto.#proto_getter_name().to_vec()
        })
    } else {
        Ok(quote! {
            #field_name: proto.#proto_getter_name()
        })
    }
}

fn make_native_field_token_stream(field: &Field) -> Result<TokenStream2, SynError> {
    for attr in field.attrs.clone() {
        let meta = if let Ok(meta) = attr.parse_meta() {
            meta
        } else {
            continue;
        };

        if meta.name() == Ident::new("from_native_impl", Span::call_site()) {
            return parse_native_meta(field, &meta);
        }
    }

    let field_name = field.ident.clone().unwrap();
    let proto_setter_name = Ident::new(
        &format!("set_{}", field_name.to_string()),
        Span::call_site(),
    );

    Ok(quote! {
        proto.#proto_setter_name(native.#field_name());
    })
}

fn make_proto_enum_variant(variant: &Variant) -> Result<Ident, SynError> {
    for attr in variant.attrs.clone() {
        let meta = if let Ok(meta) = attr.parse_meta() {
            meta
        } else {
            continue;
        };

        if meta.name() != Ident::new("proto_enum", Span::call_site()) {
            continue;
        }

        let meta_list = if let Meta::List(l) = meta.clone() {
            l.nested
        } else {
            return Err(SynError::new_spanned(
                meta.into_token_stream(),
                "Malformed Meta",
            ));
        };

        let nested_meta = if let Some(pair) = meta_list.first() {
            pair.into_value()
        } else {
            return Err(SynError::new_spanned(
                meta.clone().into_token_stream(),
                "Malformed Meta",
            ));
        };

        if let NestedMeta::Meta(Meta::Word(i)) = nested_meta {
            return Ok(i.clone());
        } else {
            return Err(SynError::new_spanned(
                meta.clone().into_token_stream(),
                "Malformed Meta",
            ));
        }
    }

    Err(SynError::new_spanned(
        variant.clone().into_token_stream(),
        "No protobuf vairant specified",
    ))
}

fn parse_proto_meta(field: &Field, meta: &Meta) -> Result<TokenStream2, SynError> {
    let meta_list = if let Meta::List(l) = meta {
        l.nested.first()
    } else {
        return Err(SynError::new_spanned(
            meta.into_token_stream(),
            "Malformed Meta",
        ));
    };

    let nested_meta = if let Some(pair) = meta_list {
        pair.into_value()
    } else {
        return Err(SynError::new_spanned(
            meta.into_token_stream(),
            "Malformed Meta",
        ));
    };

    let ident = if let NestedMeta::Meta(Meta::Word(i)) = nested_meta {
        i.clone()
    } else {
        return Err(SynError::new_spanned(
            meta.into_token_stream(),
            "Malformed Meta",
        ));
    };

    let field_name = field.ident.clone().unwrap();
    let proto_getter_name = Ident::new(
        &format!("get_{}", field_name.to_string()),
        Span::call_site(),
    );

    let token = if ident == Ident::new("to_string", Span::call_site()) {
        quote! {
            #field_name: proto.#proto_getter_name().to_string()
        }
    } else if ident == Ident::new("clone", Span::call_site()) {
        quote! {
            #field_name: proto.#proto_getter_name().clone()
        }
    } else if ident == Ident::new("from_proto", Span::call_site()) {
        let ty = field.ty.clone();
        quote! {
            #field_name: #ty::from_proto(proto.#proto_getter_name().clone())?
        }
    } else if ident == Ident::new("Vec", Span::call_site()) {
        let vec_ty = field.ty.clone();
        let ty = extract_type_from_generic(&vec_ty)?;

        if is_string(&ty) {
            quote! {
                #field_name: proto
                .#proto_getter_name()
                .to_vec()
                .into_iter()
                .map(String::from)
                .collect()
            }
        } else {
            quote! {
                #field_name: proto
                .#proto_getter_name()
                .to_vec()
                .into_iter()
                .map(#ty::from_proto)
                .collect::<Result<#vec_ty, ProtoConversionError>>()?
            }
        }
    } else {
        return Err(SynError::new_spanned(
            meta.into_token_stream(),
            "Unknown from_impl directive",
        ));
    };

    Ok(token)
}

fn parse_native_meta(field: &Field, meta: &Meta) -> Result<TokenStream2, SynError> {
    let meta_list = if let Meta::List(l) = meta {
        l.nested.first()
    } else {
        return Err(SynError::new_spanned(
            meta.into_token_stream(),
            "Malformed Meta",
        ));
    };

    let nested_meta = if let Some(pair) = meta_list {
        pair.into_value()
    } else {
        return Err(SynError::new_spanned(
            meta.into_token_stream(),
            "Malformed Meta",
        ));
    };

    let ident = if let NestedMeta::Meta(Meta::Word(i)) = nested_meta {
        i.clone()
    } else {
        return Err(SynError::new_spanned(
            meta.into_token_stream(),
            "Malformed Meta",
        ));
    };

    let field_name = field.ident.clone().unwrap();
    let proto_setter_name = Ident::new(
        &format!("set_{}", field_name.to_string()),
        Span::call_site(),
    );

    let token = if ident == Ident::new("to_string", Span::call_site()) {
        quote! {
            proto.#proto_setter_name(native.#field_name().to_string());
        }
    } else if ident == Ident::new("clone", Span::call_site()) {
        quote! {
            proto.#proto_setter_name(native.#field_name().clone());
        }
    } else if ident == Ident::new("into_proto", Span::call_site()) {
        quote! {
            proto.#proto_setter_name(native.#field_name().clone().into_proto()?);
        }
    } else if ident == Ident::new("deref", Span::call_site()) {
        quote! {
            proto.#proto_setter_name(*native.#field_name());
        }
    } else if ident == Ident::new("Vec", Span::call_site()) {
        let vec_ty = field.ty.clone();
        let ty = extract_type_from_generic(&vec_ty)?;

        if is_string(&ty) {
            quote! {
                proto.#proto_setter_name(RepeatedField::from_vec(native.#field_name().to_vec()));
            }
        } else {
            let vec_variable = Ident::new(
                &format!("{}_vec", field_name.to_string()),
                Span::call_site(),
            );
            quote! {
                let #vec_variable = native
                    .#field_name()
                    .to_vec()
                    .into_iter()
                    .map(#ty::into_proto)
                    .collect::<Result<#vec_ty, ProtoConversionError>>()?;

                proto.#proto_setter_name(RepeatedField::from_vec(#vec_variable));
            }
        }
    } else {
        return Err(SynError::new_spanned(
            meta.into_token_stream(),
            "Unknown from_impl directive",
        ));
    };

    Ok(token)
}

fn is_string(ty: &Type) -> bool {
    is_type(Ident::new("String", Span::call_site()), ty)
}

fn is_vec(ty: &Type) -> bool {
    is_type(Ident::new("Vec", Span::call_site()), ty)
}

fn is_type(ident: Ident, ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        type_path.path.segments.iter().any(|x| x.ident == ident)
    } else {
        false
    }
}

fn get_struct_fields(derive_input: DeriveInput) -> Result<Fields, SynError> {
    if let Data::Struct(d) = derive_input.data {
        Ok(d.fields)
    } else {
        Err(SynError::new_spanned(
            derive_input.into_token_stream(),
            "macro is only compatible with stucts",
        ))
    }
}

fn get_enum_variants(derive_input: DeriveInput) -> Result<Vec<Variant>, SynError> {
    let enm = if let Data::Enum(e) = derive_input.data {
        e
    } else {
        return Err(SynError::new_spanned(
            derive_input.into_token_stream(),
            "macro is only compatible with enums",
        ));
    };

    Ok(enm.variants.into_iter().collect::<Vec<Variant>>())
}

fn extract_type_from_generic(ty: &Type) -> Result<Type, SynError> {
    let segment = if let Type::Path(type_path) = ty {
        type_path.path.segments.first()
    } else {
        return Err(SynError::new_spanned(
            ty.into_token_stream(),
            "Type does not have generic",
        ));
    };

    let args = if let Some(seg) = segment {
        seg.into_value().arguments.clone()
    } else {
        return Err(SynError::new_spanned(
            ty.into_token_stream(),
            "Type does not have generic",
        ));
    };

    let angled_bracket_args = if let PathArguments::AngleBracketed(args) = args {
        if let Some(angled_bracket_args) = args.args.first() {
            angled_bracket_args.into_value().clone()
        } else {
            return Err(SynError::new_spanned(
                ty.into_token_stream(),
                "Type does not have generic",
            ));
        }
    } else {
        return Err(SynError::new_spanned(
            ty.into_token_stream(),
            "Type does not have generic",
        ));
    };

    if let GenericArgument::Type(t) = angled_bracket_args {
        Ok(t)
    } else {
        return Err(SynError::new_spanned(
            ty.into_token_stream(),
            "Type does not have generic",
        ));
    }
}

fn path_to_token_stream(path: &str) -> Vec<TokenStream2> {
    let mut tokens = Vec::new();

    for seg in path.split("::") {
        let ident = Ident::new(seg, Span::call_site());

        tokens.push(quote!(#ident));
        tokens.push(quote!(::));
    }

    // Remove extra "::"
    if !tokens.is_empty() {
        tokens.pop();
    }

    tokens
}
