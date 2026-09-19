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

//! Strict external JSON parsing, preserving number tokens and rejecting duplicate keys.

use jsonc_parser::{CollectOptions, ParseOptions, ast, parse_to_ast};
use serde_json::{Map, Value, map::Entry};
use std::{error::Error, fmt};

/// Parses strict UTF-8 JSON without losing number precision.
///
/// # Errors
/// Rejects malformed JSON, extensions, and duplicate object properties.
pub(crate) fn parse_json(bytes: &[u8]) -> Result<Value, JsonError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| JsonError::caused_by("contract is not UTF-8", error))?;
    let parsed = parse_to_ast(
        text,
        &CollectOptions::default(),
        &ParseOptions {
            allow_comments: false,
            allow_loose_object_property_names: false,
            allow_trailing_commas: false,
            allow_missing_commas: false,
            allow_single_quoted_strings: false,
            allow_hexadecimal_numbers: false,
            allow_unary_plus_numbers: false,
        },
    )
    .map_err(|error| JsonError::caused_by("contract is not strict JSON", error))?;
    let value = parsed
        .value
        .ok_or_else(|| JsonError::new("contract contains no JSON value"))?;
    convert(value)
}

/// A strict JSON decoding failure with its original parse cause.
#[derive(Debug)]
pub(crate) struct JsonError {
    message: &'static str,
    cause: Option<Box<dyn Error + Send + Sync>>,
}

impl JsonError {
    fn new(message: &'static str) -> Self {
        Self {
            message,
            cause: None,
        }
    }

    fn caused_by(message: &'static str, cause: impl Error + Send + Sync + 'static) -> Self {
        Self {
            message,
            cause: Some(Box::new(cause)),
        }
    }
}

impl fmt::Display for JsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)?;
        if let Some(cause) = &self.cause {
            write!(formatter, ": {cause}")?;
        }
        Ok(())
    }
}

impl Error for JsonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause.as_deref().map(|cause| cause as &dyn Error)
    }
}

fn convert(value: ast::Value<'_>) -> Result<Value, JsonError> {
    match value {
        ast::Value::NullKeyword(_) => Ok(Value::Null),
        ast::Value::BooleanLit(value) => Ok(Value::Bool(value.value)),
        ast::Value::StringLit(value) => Ok(Value::String(value.value.into_owned())),
        ast::Value::NumberLit(value) => value
            .value
            .parse()
            .map(Value::Number)
            .map_err(|error| JsonError::caused_by("invalid JSON number", error)),
        ast::Value::Array(array) => array
            .elements
            .into_iter()
            .map(|item| convert(item))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        ast::Value::Object(object) => {
            let mut entries = Map::new();
            for property in object.properties {
                match entries.entry(property.name.into_string()) {
                    Entry::Vacant(entry) => {
                        entry.insert(convert(property.value)?);
                    }
                    Entry::Occupied(_) => {
                        return Err(JsonError::new("duplicate JSON object property"));
                    }
                }
            }
            Ok(Value::Object(entries))
        }
    }
}

#[cfg(test)]
mod tests;
