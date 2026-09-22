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

//! Validated identifiers used across Tenon Document parsing and runtime discovery.
//!
//! Raw text enters this module at system boundaries. Successful parsing produces
//! distinct types whose private storage keeps invalid identifiers out of the rest
//! of the program.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::error::Error;
use std::str::FromStr;
use std::{fmt, iter};

const MAX_LOCAL_ID_BYTES: usize = 128;

/// A stable reason why a domain identifier could not be parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdentifierParseError {
    /// The identifier contains no UTF-8 bytes.
    Empty,
    /// The identifier contains more than 128 UTF-8 bytes.
    TooLong,
    /// The identifier contains a C0 or C1 control character.
    ControlCharacter,
    /// The Plugin Program name violates Tenon's reverse-domain grammar.
    InvalidProgramName,
    /// The Plugin Program version violates Tenon's strict SemVer subset.
    InvalidExactVersion,
    /// The Sink Contract identity is not a program name and exact version joined by `@`.
    InvalidSinkContractId,
}

impl IdentifierParseError {
    /// Returns the stable machine-readable code for this error.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Empty => "identifier.empty",
            Self::TooLong => "identifier.too_long",
            Self::ControlCharacter => "identifier.control_character",
            Self::InvalidProgramName => "program_name.invalid",
            Self::InvalidExactVersion => "exact_version.invalid",
            Self::InvalidSinkContractId => "sink_contract_id.invalid",
        }
    }
}

impl fmt::Display for IdentifierParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "identifier must not be empty",
            Self::TooLong => "identifier must not exceed 128 UTF-8 bytes",
            Self::ControlCharacter => "identifier must not contain control characters",
            Self::InvalidProgramName => "invalid Plugin Program name",
            Self::InvalidExactVersion => "invalid exact Plugin Program version",
            Self::InvalidSinkContractId => "invalid Sink Contract identity",
        })
    }
}

impl Error for IdentifierParseError {}

/// A validated Tenon Document identity.
///
/// # Examples
///
/// ```
/// use tenon::TenonDocumentId;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let id = TenonDocumentId::try_from("pipeline-a")?;
///     assert_eq!(id.as_str(), "pipeline-a");
///     Ok(())
/// }
/// ```
///
/// # Errors
///
/// Parsing returns `IdentifierParseError::Empty`,
/// `IdentifierParseError::TooLong`, or
/// `IdentifierParseError::ControlCharacter` when the local identity contract
/// is violated.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TenonDocumentId(String);

impl TenonDocumentId {
    /// Restores text whose domain validity was already established by its owner.
    pub(crate) fn from_verified(value: String) -> Self {
        Self(value)
    }

    /// Returns the exact validated text without trimming or normalization.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for TenonDocumentId {
    type Error = IdentifierParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_local_id(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for TenonDocumentId {
    type Error = IdentifierParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_local_id(&value)?;
        Ok(Self(value))
    }
}

impl FromStr for TenonDocumentId {
    type Err = IdentifierParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for TenonDocumentId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for TenonDocumentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A validated Pipeline-local Flow identity, distinct from a Plugin Instance id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct FlowId(String);

impl FlowId {
    /// Restores text whose domain validity was already established by its owner.
    pub(crate) fn from_verified(value: String) -> Self {
        Self(value)
    }

    /// Returns the exact validated identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for FlowId {
    type Error = IdentifierParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_local_id(&value)?;
        Ok(Self(value))
    }
}

/// A validated Pipeline-local Plugin Instance identity, distinct from a Flow id.
///
/// `PluginInstanceId` deliberately differs from [`TenonDocumentId`] even though both use
/// the same text grammar. Passing one where the other is required is a compile
/// error.
///
/// # Errors
///
/// Parsing returns `IdentifierParseError::Empty`,
/// `IdentifierParseError::TooLong`, or
/// `IdentifierParseError::ControlCharacter` when the local identity contract
/// is violated.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct PluginInstanceId(String);

impl PluginInstanceId {
    /// Restores text whose domain validity was already established by its owner.
    pub(crate) fn from_verified(value: String) -> Self {
        Self(value)
    }

    /// Returns the exact validated text without trimming or normalization.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for PluginInstanceId {
    type Error = IdentifierParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_local_id(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for PluginInstanceId {
    type Error = IdentifierParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_local_id(&value)?;
        Ok(Self(value))
    }
}

impl FromStr for PluginInstanceId {
    type Err = IdentifierParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for PluginInstanceId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for PluginInstanceId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for PluginInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A validated reverse-domain Plugin Program name.
///
/// # Examples
///
/// ```
/// use tenon::ProgramName;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let name = ProgramName::try_from("com.example.kafka")?;
///     assert_eq!(name.as_str(), "com.example.kafka");
///     Ok(())
/// }
/// ```
///
/// # Errors
///
/// Parsing returns `IdentifierParseError::InvalidProgramName` when the input
/// violates Tenon's reverse-domain grammar.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ProgramName(String);

impl ProgramName {
    /// Restores text whose domain validity was already established by its owner.
    pub(crate) fn from_verified(value: String) -> Self {
        Self(value)
    }

    /// Returns the exact validated program name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ProgramName {
    type Error = IdentifierParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_program_name(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for ProgramName {
    type Error = IdentifierParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_program_name(&value)?;
        Ok(Self(value))
    }
}

impl FromStr for ProgramName {
    type Err = IdentifierParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for ProgramName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ProgramName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A validated exact Plugin Program version in Tenon's strict SemVer subset.
///
/// # Examples
///
/// ```
/// use tenon::ExactVersion;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let version = ExactVersion::try_from("2.0.0-rc.1")?;
///     assert_eq!(version.as_str(), "2.0.0-rc.1");
///     Ok(())
/// }
/// ```
///
/// # Errors
///
/// Parsing returns `IdentifierParseError::InvalidExactVersion` when the input
/// violates Tenon's strict SemVer subset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ExactVersion(String);

impl ExactVersion {
    /// Restores text whose domain validity was already established by its owner.
    pub(crate) fn from_verified(value: String) -> Self {
        Self(value)
    }

    /// Returns the exact validated version text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ExactVersion {
    type Error = IdentifierParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_exact_version(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for ExactVersion {
    type Error = IdentifierParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_exact_version(&value)?;
        Ok(Self(value))
    }
}

impl FromStr for ExactVersion {
    type Err = IdentifierParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for ExactVersion {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ExactVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Identifies one exact Plugin Program version, shared by any instances using it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginProgramIdentity {
    program_name: ProgramName,
    exact_version: ExactVersion,
}

impl PluginProgramIdentity {
    /// Creates an identity from already validated components.
    #[must_use]
    pub const fn from_parts(program_name: ProgramName, exact_version: ExactVersion) -> Self {
        Self {
            program_name,
            exact_version,
        }
    }

    /// Returns the validated Program name.
    #[must_use]
    pub const fn program_name(&self) -> &ProgramName {
        &self.program_name
    }

    /// Returns the validated exact version.
    #[must_use]
    pub const fn exact_version(&self) -> &ExactVersion {
        &self.exact_version
    }
}

/// A validated Sink Program type identity.
///
/// The value is always the canonical `<programName>@<exactVersion>` pair. It
/// identifies a program and Payload Contract, not a Tenon Document-scoped Sink instance.
///
/// # Errors
///
/// Parsing returns `IdentifierParseError::InvalidSinkContractId` when the `@`
/// separator is missing or repeated. Invalid components retain
/// `IdentifierParseError::InvalidProgramName` or
/// `IdentifierParseError::InvalidExactVersion`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SinkContractId {
    program_name: ProgramName,
    exact_version: ExactVersion,
}

impl SinkContractId {
    /// Creates a Sink Contract identity from two already validated components.
    #[must_use]
    pub const fn from_parts(program_name: ProgramName, exact_version: ExactVersion) -> Self {
        Self {
            program_name,
            exact_version,
        }
    }

    /// Returns the validated Sink Program name.
    #[must_use]
    pub const fn program_name(&self) -> &ProgramName {
        &self.program_name
    }

    /// Returns the validated exact Sink Program version.
    #[must_use]
    pub const fn exact_version(&self) -> &ExactVersion {
        &self.exact_version
    }

    /// Compares two borrowed component pairs as canonical Contract identity text.
    pub(crate) fn cmp_parts(
        program_name: &ProgramName,
        exact_version: &ExactVersion,
        other_program_name: &ProgramName,
        other_exact_version: &ExactVersion,
    ) -> Ordering {
        program_name
            .as_str()
            .bytes()
            .chain(iter::once(b'@'))
            .chain(exact_version.as_str().bytes())
            .cmp(
                other_program_name
                    .as_str()
                    .bytes()
                    .chain(iter::once(b'@'))
                    .chain(other_exact_version.as_str().bytes()),
            )
    }
}

impl TryFrom<&str> for SinkContractId {
    type Error = IdentifierParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let Some((program_name, exact_version)) = value.split_once('@') else {
            return Err(IdentifierParseError::InvalidSinkContractId);
        };
        if exact_version.contains('@') {
            return Err(IdentifierParseError::InvalidSinkContractId);
        }

        Ok(Self::from_parts(
            ProgramName::try_from(program_name)?,
            ExactVersion::try_from(exact_version)?,
        ))
    }
}

impl TryFrom<String> for SinkContractId {
    type Error = IdentifierParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl FromStr for SinkContractId {
    type Err = IdentifierParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl fmt::Display for SinkContractId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}@{}",
            self.program_name.as_str(),
            self.exact_version.as_str()
        )
    }
}

pub(crate) fn validate_local_id(value: &str) -> Result<(), IdentifierParseError> {
    if value.is_empty() {
        return Err(IdentifierParseError::Empty);
    }
    if value.len() > MAX_LOCAL_ID_BYTES {
        return Err(IdentifierParseError::TooLong);
    }
    if value.chars().any(is_c0_or_c1_control) {
        return Err(IdentifierParseError::ControlCharacter);
    }

    Ok(())
}

fn validate_program_name(value: &str) -> Result<(), IdentifierParseError> {
    let mut segment_count = 0;
    for segment in value.split('.') {
        if !is_valid_program_name_segment(segment) {
            return Err(IdentifierParseError::InvalidProgramName);
        }
        segment_count += 1;
    }
    if segment_count < 3 {
        return Err(IdentifierParseError::InvalidProgramName);
    }

    Ok(())
}

fn is_valid_program_name_segment(segment: &str) -> bool {
    let Some((first, remaining)) = segment.as_bytes().split_first() else {
        return false;
    };
    let last = remaining.last().unwrap_or(first);

    first.is_ascii_alphanumeric()
        && last.is_ascii_alphanumeric()
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn validate_exact_version(value: &str) -> Result<(), IdentifierParseError> {
    if value.contains('+') || value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(IdentifierParseError::InvalidExactVersion);
    }

    let (core, prerelease) = match value.split_once('-') {
        Some((core, prerelease)) => (core, Some(prerelease)),
        None => (value, None),
    };
    if !is_valid_version_core(core)
        || prerelease.is_some_and(|prerelease| !is_valid_prerelease(prerelease))
    {
        return Err(IdentifierParseError::InvalidExactVersion);
    }

    Ok(())
}

fn is_valid_version_core(core: &str) -> bool {
    let mut components = core.split('.');
    let (Some(major), Some(minor), Some(patch)) =
        (components.next(), components.next(), components.next())
    else {
        return false;
    };

    components.next().is_none()
        && is_valid_numeric_identifier(major)
        && is_valid_numeric_identifier(minor)
        && is_valid_numeric_identifier(patch)
}

fn is_valid_prerelease(prerelease: &str) -> bool {
    !prerelease.is_empty()
        && prerelease.split('.').all(|identifier| {
            let valid_characters = !identifier.is_empty()
                && identifier
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
            let numeric = identifier.bytes().all(|byte| byte.is_ascii_digit());

            valid_characters && (!numeric || is_valid_numeric_identifier(identifier))
        })
}

fn is_valid_numeric_identifier(identifier: &str) -> bool {
    !identifier.is_empty()
        && identifier.bytes().all(|byte| byte.is_ascii_digit())
        && (identifier.len() == 1 || !identifier.starts_with('0'))
}

fn is_c0_or_c1_control(character: char) -> bool {
    matches!(u32::from(character), 0x00..=0x1f | 0x7f..=0x9f)
}

#[cfg(test)]
mod tests;
