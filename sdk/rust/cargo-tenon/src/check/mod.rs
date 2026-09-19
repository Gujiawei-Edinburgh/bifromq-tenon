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

//! Validates external package contracts before any build or packaging action.
//!
//! This module borrows input bytes only for one synchronous check. Its result
//! owns the checked identity and root names for the CLI report. It does not
//! represent an installed Program, retain raw material, or certify executable
//! dependencies. The binary alone reports errors and chooses its exit status.

mod config_schema;
mod json;
mod payload;

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Path;

/// Checks the three fixed contract files without executing or changing them.
///
/// # Errors
/// Returns the file and failure category for unreadable or invalid contracts.
pub(super) fn package(directory: &Path) -> Result<CheckedPackage, CheckError> {
    let manifest = manifest(&read_file(directory, "manifest.json")?)?;
    manifest.check(
        &read_file(directory, "config.schema.json")?,
        &read_file(directory, "payload.descriptor.pb")?,
    )
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CheckedPackage {
    program_name: String,
    exact_version: String,
    display_name: String,
    description: String,
    interface: PluginInterface,
    platforms: Vec<Platform>,
    source_root: Option<String>,
    sink_root: Option<String>,
}

/// One failure category, contextual explanation, and original external cause.
#[derive(Debug)]
pub(super) struct CheckError {
    code: &'static str,
    message: String,
    cause: Option<Box<dyn Error + Send + Sync>>,
}

impl CheckError {
    pub(super) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            cause: None,
        }
    }

    pub(super) fn caused_by(
        code: &'static str,
        message: impl Into<String>,
        cause: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            cause: Some(Box::new(cause)),
        }
    }

    pub(super) fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for CheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)?;
        if let Some(cause) = &self.cause {
            write!(formatter, ": {cause}")?;
        }
        Ok(())
    }
}

impl Error for CheckError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause.as_deref().map(|cause| cause as &dyn Error)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Manifest {
    program_name: String,
    exact_version: String,
    display_name: String,
    description: String,
    interface: PluginInterface,
    platforms: Vec<Platform>,
}

impl Manifest {
    /// Validates the remaining materials against this already checked manifest.
    pub(super) fn check(
        self,
        schema: &[u8],
        descriptor: &[u8],
    ) -> Result<CheckedPackage, CheckError> {
        config_schema::validate(schema)?;
        let roots = payload::validate(descriptor, self.interface)?;
        Ok(CheckedPackage {
            program_name: self.program_name,
            exact_version: self.exact_version,
            display_name: self.display_name,
            description: self.description,
            interface: self.interface,
            platforms: self.platforms,
            source_root: roots.source,
            sink_root: roots.sink,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum PluginInterface {
    Source,
    Sink,
    SourceAndSink,
}

#[derive(Debug, Deserialize, Serialize)]
struct Platform {
    os: String,
    architecture: String,
}

#[expect(
    clippy::expect_used,
    reason = "the embedded Schema and the matching manifest projection are build-time invariants"
)]
pub(super) fn manifest(bytes: &[u8]) -> Result<Manifest, CheckError> {
    let value = json::parse(bytes).map_err(|error| {
        CheckError::caused_by(
            "manifest.invalid_json",
            "manifest.json is invalid JSON",
            error,
        )
    })?;
    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../../contracts/manifest.schema.json"))
            .expect("the published manifest Schema must be valid JSON");
    let validator =
        jsonschema::draft202012::new(&schema).expect("the published manifest Schema must compile");
    validator.validate(&value).map_err(|error| {
        CheckError::caused_by(
            "manifest.invalid",
            format!("manifest.json is invalid at {}", error.instance_path()),
            error.to_owned(),
        )
    })?;
    Ok(serde_json::from_value(value)
        .expect("the manifest projection must match the published Schema"))
}

pub(super) fn read_file(directory: &Path, name: &str) -> Result<Vec<u8>, CheckError> {
    let path = directory.join(name);
    let read_error = |error| {
        CheckError::caused_by(
            "package.read_failed",
            format!("cannot read {}", path.display()),
            error,
        )
    };
    let metadata = fs::symlink_metadata(&path).map_err(read_error)?;
    if !metadata.is_file() {
        return Err(CheckError::new(
            "package.invalid_file",
            format!("{} must be a regular file", path.display()),
        ));
    }
    fs::read(&path).map_err(read_error)
}
