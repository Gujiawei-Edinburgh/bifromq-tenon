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

//! Parsing boundary and internal verification for Tenon Documents.
//!
//! The physical boundary checks UTF-8, the exact
//! JSON-with-comments grammar, duplicate object fields, and conversion into an
//! owned, unverified JSON tree. Schema validation, semantic projection, and Lua
//! compilation are implementation details of one verifier
//! rather than caller-visible states.
//! Runtime material resolution is a separate boundary after verification.

use jsonc_parser::errors::ParseError;
use serde_json::Value;
use std::error::Error;
use std::fmt;
use std::str::Utf8Error;

use crate::strict_jsonc::{StrictJsonError, parse_jsonc};

mod static_validation;
pub(crate) mod verified;

pub use static_validation::{
    TenonDocumentVerificationError, TenonDocumentVerifierInitializationError,
};
pub use verified::{ResourceLimits, VerifiedTenonDocument};

/// The acknowledgement boundary selected for one Flow's Source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum SourceDelivery {
    /// Complete Source sends before Lua processing starts.
    AtMostOnce,
    /// Complete Source sends only when Lua reaches an explicit emit boundary.
    AtLeastOnce,
}

/// An owned Tenon Document that has been parsed but not verified.
///
/// The JSON tree is owned and does not borrow from the input byte slice.
/// Successful construction does not imply that the value is a JSON object or
/// that it satisfies the Tenon Document Schema or any domain rule.
#[derive(PartialEq)]
pub struct UnverifiedTenonDocument {
    json: Value,
}

impl UnverifiedTenonDocument {
    /// Parses one raw Tenon Document into an owned JSON tree.
    ///
    /// JSON comments are accepted, while trailing commas, duplicate object fields, loose keys,
    /// missing commas, single-quoted strings, hexadecimal numbers, unary plus,
    /// empty input, and additional root values are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`TenonDocumentSyntaxError`] when the raw bytes are not UTF-8,
    /// violate the strict JSONC grammar, contain a
    /// duplicate object field at any depth, or cannot be represented by the
    /// lossless JSON number type.
    pub fn parse(source: &[u8]) -> Result<Self, TenonDocumentSyntaxError> {
        let json = parse_jsonc(source).map_err(TenonDocumentSyntaxError::from)?;

        Ok(Self { json })
    }

    /// Returns the owned JSON tree by shared reference.
    #[must_use]
    pub const fn as_json(&self) -> &Value {
        &self.json
    }

    fn into_json(self) -> Value {
        self.json
    }
}

impl From<StrictJsonError> for TenonDocumentSyntaxError {
    fn from(error: StrictJsonError) -> Self {
        match error {
            StrictJsonError::Utf8Invalid { source } => Self::Utf8Invalid { source },
            StrictJsonError::JsonSyntaxInvalid { source } => Self::JsonSyntaxInvalid { source },
            StrictJsonError::JsonValueMissing => Self::JsonValueMissing,
            StrictJsonError::ObjectFieldDuplicate { field } => Self::ObjectFieldDuplicate { field },
            StrictJsonError::JsonNumberInvalid { value, source } => {
                Self::JsonNumberInvalid { value, source }
            }
        }
    }
}

impl fmt::Debug for UnverifiedTenonDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnverifiedTenonDocument")
            .field("json", &"[REDACTED]")
            .finish()
    }
}

/// A stable failure from the Tenon Document physical syntax boundary.
#[derive(Debug)]
#[non_exhaustive]
pub enum TenonDocumentSyntaxError {
    /// The raw document is not UTF-8.
    Utf8Invalid {
        /// The standard UTF-8 decoding failure.
        source: Utf8Error,
    },
    /// The text violates Tenon's strict JSON-with-comments grammar.
    JsonSyntaxInvalid {
        /// The JSONC parser failure with source position.
        source: ParseError,
    },
    /// The document contains only whitespace or comments.
    JsonValueMissing,
    /// One JSON object contains the same decoded field name more than once.
    ObjectFieldDuplicate {
        /// The decoded field name that appears more than once.
        field: String,
    },
    /// A syntactically valid JSON number cannot enter the lossless JSON tree.
    JsonNumberInvalid {
        /// The original JSON number text.
        value: String,
        /// The JSON number conversion failure.
        source: serde_json::Error,
    },
}

impl TenonDocumentSyntaxError {
    /// Returns the stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Utf8Invalid { .. } => "tenon_document.utf8_invalid",
            Self::JsonSyntaxInvalid { .. } | Self::JsonValueMissing => {
                "tenon_document.json_syntax_invalid"
            }
            Self::ObjectFieldDuplicate { .. } => "tenon_document.object_field_duplicate",
            Self::JsonNumberInvalid { .. } => "tenon_document.json_syntax_invalid",
        }
    }
}

impl fmt::Display for TenonDocumentSyntaxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Utf8Invalid { .. } => formatter.write_str("Tenon Document is not valid UTF-8"),
            Self::JsonSyntaxInvalid { .. } => {
                formatter.write_str("Tenon Document JSONC syntax is invalid")
            }
            Self::JsonValueMissing => formatter.write_str("Tenon Document contains no JSON value"),
            Self::ObjectFieldDuplicate { field } => {
                write!(
                    formatter,
                    "Tenon Document object field is duplicated: {field}"
                )
            }
            Self::JsonNumberInvalid { value, .. } => {
                write!(formatter, "Tenon Document JSON number is invalid: {value}")
            }
        }
    }
}

impl Error for TenonDocumentSyntaxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Utf8Invalid { source } => Some(source),
            Self::JsonSyntaxInvalid { source } => Some(source),
            Self::JsonNumberInvalid { source, .. } => Some(source),
            Self::JsonValueMissing | Self::ObjectFieldDuplicate { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;
