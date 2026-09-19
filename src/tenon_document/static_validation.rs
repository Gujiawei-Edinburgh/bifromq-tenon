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

//! Shared Schema execution and redacted verification failure types.

use super::UnverifiedTenonDocument;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub(super) struct TenonDocumentV1SchemaValidator {
    validator: jsonschema::Validator,
}

impl TenonDocumentV1SchemaValidator {
    pub(super) fn compile_schema(
        schema_bytes: &[u8],
    ) -> Result<Self, TenonDocumentSchemaInitializationError> {
        let schema = serde_json::from_slice(schema_bytes).map_err(|source| {
            TenonDocumentSchemaInitializationError::SchemaJsonInvalid { source }
        })?;
        let validator = jsonschema::draft202012::new(&schema).map_err(|source| {
            TenonDocumentSchemaInitializationError::SchemaCompilationFailed { source }
        })?;

        Ok(Self { validator })
    }

    pub(super) fn validate(
        &self,
        document: &UnverifiedTenonDocument,
    ) -> Result<(), Vec<TenonDocumentVerificationIssue>> {
        let issues = self
            .validator
            .iter_errors(document.as_json())
            .map(|error| {
                TenonDocumentVerificationIssue::new(
                    "tenon_document.schema_invalid",
                    error.instance_path().as_str(),
                )
            })
            .collect::<Vec<_>>();

        if issues.is_empty() {
            Ok(())
        } else {
            Err(issues)
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum TenonDocumentSchemaInitializationError {
    SchemaJsonInvalid {
        source: serde_json::Error,
    },
    SchemaCompilationFailed {
        source: jsonschema::ValidationError<'static>,
    },
}

impl fmt::Display for TenonDocumentSchemaInitializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaJsonInvalid { .. } => {
                formatter.write_str("Embedded Tenon Document v1 Schema is not valid JSON")
            }
            Self::SchemaCompilationFailed { .. } => {
                formatter.write_str("Embedded Tenon Document v1 Schema cannot be compiled")
            }
        }
    }
}

impl Error for TenonDocumentSchemaInitializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SchemaJsonInvalid { source } => Some(source),
            Self::SchemaCompilationFailed { source } => Some(source),
        }
    }
}

/// One stable, redacted verification issue and its original JSON location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenonDocumentVerificationIssue {
    code: &'static str,
    instance_path: Box<str>,
}

impl TenonDocumentVerificationIssue {
    /// Returns the stable machine-readable issue code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Returns the RFC 6901 JSON Pointer to the invalid value.
    #[must_use]
    pub fn instance_path(&self) -> &str {
        &self.instance_path
    }

    pub(super) fn new(code: &'static str, instance_path: impl Into<Box<str>>) -> Self {
        Self {
            code,
            instance_path: instance_path.into(),
        }
    }
}

/// A stable, redacted failure from complete Tenon Document verification.
pub struct TenonDocumentVerificationError {
    issues: Box<[TenonDocumentVerificationIssue]>,
}

impl TenonDocumentVerificationError {
    /// Returns the stable machine-readable error code.
    #[must_use]
    #[cfg_attr(
        not(any(test, feature = "repository-test-support")),
        expect(
            dead_code,
            reason = "Shared contract implementation exercised by repository tests"
        )
    )]
    pub const fn code(&self) -> &'static str {
        "tenon_document.verification_failed"
    }

    /// Returns every issue in deterministic validation order.
    #[must_use]
    pub const fn issues(&self) -> &[TenonDocumentVerificationIssue] {
        &self.issues
    }

    pub(super) fn from_issues(issues: Box<[TenonDocumentVerificationIssue]>) -> Self {
        Self { issues }
    }
}

impl fmt::Debug for TenonDocumentVerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenonDocumentVerificationError")
            .field("issues", &self.issues)
            .finish()
    }
}

impl fmt::Display for TenonDocumentVerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Tenon Document verification failed")
    }
}

impl Error for TenonDocumentVerificationError {}

/// A stable, redacted failure to initialize the static verifier.
pub struct TenonDocumentVerifierInitializationError {
    source: TenonDocumentSchemaInitializationError,
}

impl TenonDocumentVerifierInitializationError {
    /// Returns the stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        "tenon_document_verifier.schema_initialization_failed"
    }

    pub(super) fn schema(source: TenonDocumentSchemaInitializationError) -> Self {
        Self { source }
    }
}

impl fmt::Debug for TenonDocumentVerifierInitializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenonDocumentVerifierInitializationError")
            .field("code", &self.code())
            .finish()
    }
}

impl fmt::Display for TenonDocumentVerifierInitializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Tenon Document verifier Schema initialization failed")
    }
}

impl Error for TenonDocumentVerifierInitializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
