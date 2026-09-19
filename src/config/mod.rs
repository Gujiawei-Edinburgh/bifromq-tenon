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

//! Immutable Runner startup configuration loaded from one strict JSONC file.

mod http;
mod http_tls;
mod metrics;
mod pipeline;
mod retry_backoff;
mod script_vm_limits;

use crate::contracts::runner::config_schema_bytes;
use crate::strict_jsonc::{StrictJsonError, parse_jsonc};
use crate::tenon_document::TenonDocumentVerifierInitializationError;
use crate::tenon_document::verified::TenonDocumentVerifier;
use http::{HttpConfig, RawHttpConfig};
pub use http_tls::HttpTlsConfig;
pub(crate) use metrics::{MetricsConfig, RawMetricsConfig};
use pipeline::{PipelineConfig, RawPipelineConfig};
use script_vm_limits::RawLuaConfig;
pub use script_vm_limits::{ScriptVmLimits, ScriptVmLimitsError};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::error::Error;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::Utf8Error;
use std::time::Duration;
use std::{fmt, fs, io};

/// One fully validated and frozen Runner configuration snapshot.
pub struct RunnerConfig {
    state_directory: PathBuf,
    http: HttpConfig,
    pipeline: PipelineConfig,
    script_vm_limits: ScriptVmLimits,
    metrics: MetricsConfig,
    extra: Map<String, Value>,
}

impl RunnerConfig {
    /// Reads, validates, and freezes one Runner configuration file.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerConfigError`] when `path` is not absolute, the file
    /// cannot be read, or its complete contents violate the Runner
    /// configuration contract.
    pub(crate) fn load(path: &Path) -> Result<Self, RunnerConfigError> {
        if !path.is_absolute() {
            return Err(RunnerConfigError::PathNotAbsolute { path: "" });
        }
        let source = fs::read(path).map_err(|source| RunnerConfigError::ReadFailed {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&source)
    }

    /// Returns the private state root without creating or opening it.
    #[must_use]
    pub fn state_directory(&self) -> &Path {
        &self.state_directory
    }

    /// Returns the exact IP address and port used by the future HTTP server.
    #[must_use]
    pub const fn http_listen_address(&self) -> SocketAddr {
        self.http.listen_address
    }

    /// Returns optional HTTPS settings from this frozen configuration.
    pub fn http_tls(&self) -> Option<&HttpTlsConfig> {
        self.http.tls.as_ref()
    }

    /// Returns the deadline from Pipeline child creation through its first
    /// structurally applied status.
    #[must_use]
    pub const fn pipeline_startup_timeout(&self) -> Duration {
        self.pipeline.startup_timeout
    }

    /// Returns the total planned Pipeline shutdown deadline.
    #[must_use]
    pub const fn pipeline_shutdown_timeout(&self) -> Duration {
        self.pipeline.shutdown_timeout
    }

    /// Returns the single deadline for a subsequent Pipeline reconfiguration.
    #[must_use]
    pub const fn pipeline_reconfigure_timeout(&self) -> Duration {
        self.pipeline.reconfigure_timeout
    }

    /// Returns the initial shared Source and process retry delay.
    #[must_use]
    pub const fn retry_initial_delay(&self) -> Duration {
        self.pipeline.retry_backoff.initial_delay
    }

    /// Returns the maximum shared Source and process retry delay.
    #[must_use]
    pub const fn retry_maximum_delay(&self) -> Duration {
        self.pipeline.retry_backoff.maximum_delay
    }

    /// Returns the CPU time limit for Lua initialization and each `main` call.
    #[must_use]
    pub const fn lua_cpu_time_limit(&self) -> Duration {
        self.script_vm_limits.cpu_time()
    }

    /// Returns the complete Lua VM state-space memory limit.
    #[must_use]
    pub const fn lua_memory_limit_bytes(&self) -> NonZeroUsize {
        self.script_vm_limits.memory_bytes()
    }

    /// Builds the static verifier from this immutable configuration snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the embedded Tenon Document Schema cannot be
    /// compiled into a validator.
    pub(crate) fn tenon_document_verifier(
        &self,
    ) -> Result<TenonDocumentVerifier, TenonDocumentVerifierInitializationError> {
        TenonDocumentVerifier::try_new(self.script_vm_limits)
    }

    /// Returns the shared script compilation and execution limits.
    #[must_use]
    pub const fn script_vm_limits(&self) -> ScriptVmLimits {
        self.script_vm_limits
    }

    /// Returns distribution-specific values from the same frozen configuration.
    #[must_use]
    pub fn extra(&self) -> &Map<String, Value> {
        &self.extra
    }

    pub(crate) fn metrics(&self) -> &MetricsConfig {
        &self.metrics
    }

    /// Validates and freezes one complete Runner JSONC value from memory.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerConfigError`] when the source is not strict UTF-8 JSONC
    /// or any structural or domain constraint is violated.
    fn parse(source: &[u8]) -> Result<Self, RunnerConfigError> {
        let value = parse_jsonc(source).map_err(RunnerConfigError::from)?;
        validate_schema(&value)?;
        let raw: RawRunnerConfig = serde_json::from_value(value)
            .map_err(|source| RunnerConfigError::InternalContractMismatch { source })?;
        Self::try_from(raw)
    }
}

impl fmt::Debug for RunnerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerConfig")
            .field("state_directory", &self.state_directory)
            .field("http_listen_address", &self.http.listen_address)
            .field("pipeline_startup_timeout", &self.pipeline.startup_timeout)
            .field("pipeline_shutdown_timeout", &self.pipeline.shutdown_timeout)
            .field(
                "pipeline_reconfigure_timeout",
                &self.pipeline.reconfigure_timeout,
            )
            .field(
                "retry_initial_delay",
                &self.pipeline.retry_backoff.initial_delay,
            )
            .field(
                "retry_maximum_delay",
                &self.pipeline.retry_backoff.maximum_delay,
            )
            .field("script_vm_limits", &self.script_vm_limits)
            .finish()
    }
}

impl TryFrom<RawRunnerConfig> for RunnerConfig {
    type Error = RunnerConfigError;

    fn try_from(raw: RawRunnerConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            state_directory: state_directory_path(raw.state_directory)?,
            http: HttpConfig::try_from(raw.http)?,
            pipeline: PipelineConfig::try_from(raw.pipeline)?,
            script_vm_limits: ScriptVmLimits::try_from(raw.lua)?,
            metrics: MetricsConfig::try_from(raw.metrics.unwrap_or_default())
                .map_err(RunnerConfigError::value_invalid)?,
            extra: raw.extra,
        })
    }
}

/// Stable failure while loading or validating Runner configuration.
#[derive(Debug)]
#[non_exhaustive]
pub(super) enum RunnerConfigError {
    /// The only startup path is not absolute.
    PathNotAbsolute {
        /// Empty for the CLI config path, otherwise the invalid config field.
        path: &'static str,
    },
    /// The absolute configuration file cannot be read completely.
    ReadFailed {
        /// The exact path supplied by the process command.
        path: PathBuf,
        /// The operating-system read failure.
        source: io::Error,
    },
    /// The configuration bytes are not UTF-8.
    Utf8Invalid {
        /// The standard UTF-8 decoding failure.
        source: Utf8Error,
    },
    /// The text violates Tenon's strict JSON-with-comments grammar.
    JsonSyntaxInvalid {
        /// The JSONC parser failure with source position.
        source: jsonc_parser::errors::ParseError,
    },
    /// The configuration contains only whitespace or comments.
    JsonValueMissing,
    /// One configuration object contains the same decoded field more than once.
    ObjectFieldDuplicate,
    /// A syntactically valid JSON number cannot enter the lossless JSON tree.
    JsonNumberInvalid {
        /// The lossless JSON number conversion failure.
        source: serde_json::Error,
    },
    /// The embedded Runner configuration Schema is not valid JSON.
    EmbeddedSchemaInvalid {
        /// The embedded JSON decoding failure.
        source: serde_json::Error,
    },
    /// The embedded Schema cannot be compiled as Draft 2020-12.
    EmbeddedSchemaCompilationFailed {
        /// The Schema compilation failure.
        source: jsonschema::ValidationError<'static>,
    },
    /// The parsed value violates the closed Runner configuration Schema.
    SchemaInvalid {
        /// The JSON Pointer of the first structural failure.
        path: Box<str>,
    },
    /// A structurally valid value violates a Runner domain rule.
    ValueInvalid {
        /// The fixed JSON Pointer of the invalid value.
        path: &'static str,
    },
    /// The retry maximum is smaller than the initial delay.
    RetryBackoffInvalid,
    /// The embedded Schema and private Rust projection disagree.
    InternalContractMismatch {
        /// The defensive Serde projection failure.
        source: serde_json::Error,
    },
}

impl RunnerConfigError {
    /// Returns the stable machine-readable failure category.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::PathNotAbsolute { .. } => "runner_config.path_not_absolute",
            Self::ReadFailed { .. } => "runner_config.read_failed",
            Self::Utf8Invalid { .. } => "runner_config.utf8_invalid",
            Self::JsonSyntaxInvalid { .. }
            | Self::JsonValueMissing
            | Self::JsonNumberInvalid { .. } => "runner_config.json_syntax_invalid",
            Self::ObjectFieldDuplicate => "runner_config.object_field_duplicate",
            Self::EmbeddedSchemaInvalid { .. }
            | Self::EmbeddedSchemaCompilationFailed { .. }
            | Self::InternalContractMismatch { .. } => "runner_config.internal_contract_invalid",
            Self::SchemaInvalid { .. } => "runner_config.schema_invalid",
            Self::ValueInvalid { .. } => "runner_config.value_invalid",
            Self::RetryBackoffInvalid => "runner_config.retry_backoff_invalid",
        }
    }

    fn value_invalid(path: &'static str) -> Self {
        Self::ValueInvalid { path }
    }
}

impl fmt::Display for RunnerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PathNotAbsolute { path: "" } => {
                formatter.write_str("Runner config path must be absolute")
            }
            Self::PathNotAbsolute { path } => {
                write!(formatter, "Runner config path must be absolute at {path}")
            }
            Self::ReadFailed { path, .. } => {
                write!(
                    formatter,
                    "Runner config cannot be read: {}",
                    path.display(),
                )
            }
            Self::Utf8Invalid { .. } => formatter.write_str("Runner config is not valid UTF-8"),
            Self::JsonSyntaxInvalid { .. } => {
                formatter.write_str("Runner config JSONC syntax is invalid")
            }
            Self::JsonValueMissing => formatter.write_str("Runner config contains no JSON value"),
            Self::ObjectFieldDuplicate => {
                formatter.write_str("Runner config contains a duplicate object field")
            }
            Self::JsonNumberInvalid { .. } => {
                formatter.write_str("Runner config contains an invalid JSON number")
            }
            Self::EmbeddedSchemaInvalid { .. }
            | Self::EmbeddedSchemaCompilationFailed { .. }
            | Self::InternalContractMismatch { .. } => {
                formatter.write_str("Embedded Runner configuration contract is invalid")
            }
            Self::SchemaInvalid { path } if path.is_empty() => {
                formatter.write_str("Runner config structure is invalid at the document root")
            }
            Self::SchemaInvalid { path } => {
                write!(formatter, "Runner config structure is invalid at {path}")
            }
            Self::ValueInvalid { path } => {
                write!(formatter, "Runner config value is invalid at {path}")
            }
            Self::RetryBackoffInvalid => {
                formatter.write_str("Runner config retry backoff range is invalid")
            }
        }
    }
}

impl Error for RunnerConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadFailed { source, .. } => Some(source),
            Self::Utf8Invalid { source } => Some(source),
            Self::JsonSyntaxInvalid { source } => Some(source),
            Self::JsonNumberInvalid { source } => Some(source),
            Self::EmbeddedSchemaInvalid { source } => Some(source),
            Self::EmbeddedSchemaCompilationFailed { source } => Some(source),
            Self::InternalContractMismatch { source } => Some(source),
            Self::PathNotAbsolute { .. }
            | Self::JsonValueMissing
            | Self::ObjectFieldDuplicate
            | Self::SchemaInvalid { .. }
            | Self::ValueInvalid { .. }
            | Self::RetryBackoffInvalid => None,
        }
    }
}

impl From<StrictJsonError> for RunnerConfigError {
    fn from(error: StrictJsonError) -> Self {
        match error {
            StrictJsonError::Utf8Invalid { source } => Self::Utf8Invalid { source },
            StrictJsonError::JsonSyntaxInvalid { source } => Self::JsonSyntaxInvalid { source },
            StrictJsonError::JsonValueMissing => Self::JsonValueMissing,
            StrictJsonError::ObjectFieldDuplicate { .. } => Self::ObjectFieldDuplicate,
            StrictJsonError::JsonNumberInvalid { source, .. } => Self::JsonNumberInvalid { source },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawRunnerConfig {
    state_directory: String,
    http: RawHttpConfig,
    pipeline: RawPipelineConfig,
    lua: RawLuaConfig,
    metrics: Option<RawMetricsConfig>,
    #[serde(default)]
    extra: Map<String, Value>,
}

fn validate_schema(value: &Value) -> Result<(), RunnerConfigError> {
    let schema = serde_json::from_slice(config_schema_bytes())
        .map_err(|source| RunnerConfigError::EmbeddedSchemaInvalid { source })?;
    let validator = jsonschema::draft202012::new(&schema)
        .map_err(|source| RunnerConfigError::EmbeddedSchemaCompilationFailed { source })?;
    if let Some(error) = validator.iter_errors(value).next() {
        return Err(RunnerConfigError::SchemaInvalid {
            path: error.instance_path().as_str().into(),
        });
    }
    Ok(())
}

fn state_directory_path(value: String) -> Result<PathBuf, RunnerConfigError> {
    if value.contains('\0') {
        return Err(RunnerConfigError::value_invalid("/stateDirectory"));
    }
    let value = PathBuf::from(value);
    if value.is_absolute() {
        Ok(value)
    } else {
        Err(RunnerConfigError::PathNotAbsolute {
            path: "/stateDirectory",
        })
    }
}

#[cfg(test)]
mod tests;
