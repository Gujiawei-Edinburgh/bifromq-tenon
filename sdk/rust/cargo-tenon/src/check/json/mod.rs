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

//! Strict package-file JSON parsing, independent of the Plugin runtime SDK.
//!
//! The parser owns syntax, decoded-key uniqueness and exact number values;
//! manifest and Config Schema validation remain with their respective callers.

use jsonc_parser::{CollectOptions, ParseOptions, ast, parse_to_ast};
use serde_json::Value;
use std::collections::HashSet;
use std::io;

pub(super) fn parse(bytes: &[u8]) -> io::Result<Value> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
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
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let value = parsed.value.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Contract contains no JSON value",
        )
    })?;
    reject_duplicate_keys(&value)?;
    // Strict parsing establishes valid decimal tokens; arbitrary_precision
    // preserves their values in the dependency's AST conversion.
    Ok(Value::from(value))
}

fn reject_duplicate_keys(value: &ast::Value<'_>) -> io::Result<()> {
    match value {
        ast::Value::Object(object) => {
            let mut names = HashSet::with_capacity(object.properties.len());
            for property in &object.properties {
                if !names.insert(property.name.as_str()) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Duplicate JSON object property",
                    ));
                }
                reject_duplicate_keys(&property.value)?;
            }
        }
        ast::Value::Array(array) => {
            for element in &array.elements {
                reject_duplicate_keys(element)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests;
