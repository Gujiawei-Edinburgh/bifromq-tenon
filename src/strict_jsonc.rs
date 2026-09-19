/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Shared strict JSON-with-comments parsing boundary.

use jsonc_parser::ast::Value as JsoncValue;
use jsonc_parser::errors::ParseError;
use jsonc_parser::{CollectOptions, ParseOptions, parse_to_ast};
use serde_json::{Map, Number, Value};
use std::error::Error;
use std::fmt;
use std::str::{FromStr, Utf8Error, from_utf8};

const STRICT_JSONC_OPTIONS: ParseOptions = ParseOptions {
    allow_comments: true,
    allow_loose_object_property_names: false,
    allow_trailing_commas: false,
    allow_missing_commas: false,
    allow_single_quoted_strings: false,
    allow_hexadecimal_numbers: false,
    allow_unary_plus_numbers: false,
};

const STRICT_JSON_OPTIONS: ParseOptions = ParseOptions {
    allow_comments: false,
    allow_loose_object_property_names: false,
    allow_trailing_commas: false,
    allow_missing_commas: false,
    allow_single_quoted_strings: false,
    allow_hexadecimal_numbers: false,
    allow_unary_plus_numbers: false,
};

#[derive(Debug)]
pub(crate) enum StrictJsonError {
    Utf8Invalid {
        source: Utf8Error,
    },
    JsonSyntaxInvalid {
        source: ParseError,
    },
    JsonValueMissing,
    ObjectFieldDuplicate {
        field: String,
    },
    JsonNumberInvalid {
        value: String,
        source: serde_json::Error,
    },
}

impl fmt::Display for StrictJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Utf8Invalid { .. } => "input is not valid UTF-8",
            Self::JsonSyntaxInvalid { .. } => "JSON syntax is invalid",
            Self::JsonValueMissing => "input contains no JSON value",
            Self::ObjectFieldDuplicate { .. } => "JSON object field is duplicated",
            Self::JsonNumberInvalid { .. } => "JSON number is invalid",
        })
    }
}

impl Error for StrictJsonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Utf8Invalid { source } => Some(source),
            Self::JsonSyntaxInvalid { source } => Some(source),
            Self::JsonNumberInvalid { source, .. } => Some(source),
            Self::JsonValueMissing | Self::ObjectFieldDuplicate { .. } => None,
        }
    }
}

pub(crate) fn parse_jsonc(source: &[u8]) -> Result<Value, StrictJsonError> {
    parse(source, &STRICT_JSONC_OPTIONS)
}

pub(crate) fn parse_json(source: &[u8]) -> Result<Value, StrictJsonError> {
    parse(source, &STRICT_JSON_OPTIONS)
}

fn parse(source: &[u8], options: &ParseOptions) -> Result<Value, StrictJsonError> {
    let text = from_utf8(source).map_err(|source| StrictJsonError::Utf8Invalid { source })?;
    let parsed = parse_to_ast(text, &CollectOptions::default(), options)
        .map_err(|source| StrictJsonError::JsonSyntaxInvalid { source })?;
    let value = parsed.value.ok_or(StrictJsonError::JsonValueMissing)?;

    into_json(value)
}

fn into_json(value: JsoncValue<'_>) -> Result<Value, StrictJsonError> {
    match value {
        JsoncValue::StringLit(value) => Ok(Value::String(value.value.into_owned())),
        JsoncValue::NumberLit(value) => {
            let number = Number::from_str(value.value).map_err(|source| {
                StrictJsonError::JsonNumberInvalid {
                    value: value.value.to_owned(),
                    source,
                }
            })?;
            Ok(Value::Number(number))
        }
        JsoncValue::BooleanLit(value) => Ok(Value::Bool(value.value)),
        JsoncValue::NullKeyword(_) => Ok(Value::Null),
        JsoncValue::Array(value) => value
            .elements
            .into_iter()
            .map(into_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        JsoncValue::Object(value) => {
            let mut entries = Map::new();
            for property in value.properties {
                let field = property.name.into_string();
                if entries.contains_key(&field) {
                    return Err(StrictJsonError::ObjectFieldDuplicate { field });
                }
                entries.insert(field, into_json(property.value)?);
            }
            Ok(Value::Object(entries))
        }
    }
}
