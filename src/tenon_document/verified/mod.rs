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

//! Complete static verification for Tenon Documents.
//! The verifier owns its compiled Schema and Lua limits;
//! each successful result owns one losslessly serializable domain model.
//!
//! Only document-local facts are checked here. No Program Store, process, task,
//! or runtime Lua binding is created. Invalid input returns ordered, redacted
//! issues and drops all partially projected data. Schema, identifier parsers,
//! and the Lua compiler remain shared with their existing owners.

use super::static_validation::{
    TenonDocumentV1SchemaValidator, TenonDocumentVerificationError, TenonDocumentVerificationIssue,
    TenonDocumentVerifierInitializationError,
};
use super::{SourceDelivery, UnverifiedTenonDocument};
use crate::config::ScriptVmLimits;
use crate::contracts::tenon_document::v1_schema_bytes;
use crate::identifiers::{
    ExactVersion, FlowId, PluginInstanceId, PluginProgramIdentity, ProgramName, TenonDocumentId,
    validate_local_id,
};
use crate::lua::validate_tenon_document_source_syntax;
use crate::strict_jsonc::parse_json;
use jsonschema::paths::Location;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Number, Value};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use tenon_ipc::queue::{capacity_for_record_limit, maximum_frame_len};

const DEFAULT_MAX_PENDING_RECORDS: u64 = 100;
const DEFAULT_MAX_RECORD_BYTES: u64 = 262_144;

/// The sole boundary that verifies every Flow and produces a complete Document.
#[derive(Debug)]
pub struct TenonDocumentVerifier {
    schema: TenonDocumentV1SchemaValidator,
    limits: ScriptVmLimits,
}

impl TenonDocumentVerifier {
    /// Compiles the embedded Schema and freezes document verification limits.
    ///
    /// # Errors
    ///
    /// Returns an initialization error if the embedded Schema cannot compile.
    pub fn try_new(
        limits: ScriptVmLimits,
    ) -> Result<Self, TenonDocumentVerifierInitializationError> {
        Ok(Self {
            schema: TenonDocumentV1SchemaValidator::compile_schema(v1_schema_bytes())
                .map_err(TenonDocumentVerifierInitializationError::schema)?,
            limits,
        })
    }

    /// Checks structure, references, and Lua source for every Flow.
    ///
    /// # Errors
    ///
    /// Returns ordered, redacted issues when any document-local check fails.
    pub fn verify(
        &self,
        document: UnverifiedTenonDocument,
    ) -> Result<VerifiedTenonDocument, TenonDocumentVerificationError> {
        validate_resource_numbers(document.as_json())?;
        validate_flow_record_limits(document.as_json())?;
        self.schema.validate(&document).map_err(|issues| {
            TenonDocumentVerificationError::from_issues(ordered_issues(issues))
        })?;
        let raw = RawDocument::from_validated_json(document.into_json());
        let mut issues = Vec::new();
        validate_identifiers(&raw, &mut issues);
        validate_references(&raw, &mut issues);
        for (flow_id, flow) in &raw.flows {
            if let Err(error) =
                validate_tenon_document_source_syntax(&flow.process.script, self.limits)
            {
                issues.push(TenonDocumentVerificationIssue::new(
                    error.issue_code(),
                    Location::new()
                        .join("flows")
                        .join(flow_id)
                        .join("process")
                        .join("script")
                        .as_str(),
                ));
            }
        }

        if !issues.is_empty() {
            return Err(TenonDocumentVerificationError::from_issues(ordered_issues(
                issues,
            )));
        }
        Ok(project(raw))
    }
}

/// One Document that passed every document-local structural and semantic check.
pub struct VerifiedTenonDocument {
    id: TenonDocumentId,
    resource_limits: Option<ResourceLimits>,
    plugin_instances: BTreeMap<PluginInstanceId, PluginInstance>,
    flows: BTreeMap<FlowId, Flow>,
}

impl VerifiedTenonDocument {
    /// Reconstructs the same-build Runner's already verified strict JSON.
    ///
    /// This is not an untrusted Document entry. Schema, references, interface
    /// usage, config, and Lua verification remain with their Runner owners.
    ///
    /// # Panics
    ///
    /// Panics when Runner violates the verified representation it alone emits.
    #[allow(
        clippy::expect_used,
        reason = "the sole same-build producer serializes a verified Document"
    )]
    pub(crate) fn from_runner_json(source: &str) -> Self {
        let json = parse_json(source.as_bytes())
            .expect("Runner must serialize its verified Document as strict JSON");
        let raw = RawDocument::from_validated_json(json);
        project(raw)
    }

    /// Returns the document's validated identity.
    #[must_use]
    pub fn id(&self) -> &TenonDocumentId {
        &self.id
    }

    /// Returns authored aggregate process limits, preserving omission.
    pub fn resource_limits(&self) -> Option<&ResourceLimits> {
        self.resource_limits.as_ref()
    }

    /// Returns the verified Instance definitions, keyed by authored identity.
    pub fn plugin_instances(&self) -> &BTreeMap<PluginInstanceId, PluginInstance> {
        &self.plugin_instances
    }

    /// Returns the verified Flow definitions, keyed by authored identity.
    pub fn flows(&self) -> &BTreeMap<FlowId, Flow> {
        &self.flows
    }

    /// Returns a Flow's channel count using this Document and the Runner CPU snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the Flow is not part of this Document or the CPU snapshot exceeds
    /// the protocol's channel-count range.
    #[allow(
        clippy::expect_used,
        reason = "the OS CPU count and Schema-bounded ratio fit the protocol channel count"
    )]
    pub fn channel_count(&self, flow_id: &FlowId, available_cpu_count: NonZeroUsize) -> NonZeroU32 {
        let Some(ratio) = self.flows[flow_id].parallelism.as_ref() else {
            return NonZeroU32::MIN;
        };
        let count = u32::try_from(ceil_decimal_product(
            ratio,
            available_cpu_count.get() as u128,
        ))
        .expect("the CPU snapshot and ratio produce a protocol-sized channel count");
        NonZeroU32::new(count).expect("a positive ratio and CPU basis create at least one channel")
    }

    /// Serializes the verified domain model without rewriting authored defaults.
    ///
    /// # Panics
    ///
    /// Panics only if this JSON-only domain model violates its serialization invariant.
    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "all map keys are strings and config remains a JSON Value"
    )]
    pub fn strict_json(&self) -> String {
        serde_json::to_string(self)
            .expect("the verified model contains only JSON-serializable data")
    }
}

impl Serialize for VerifiedTenonDocument {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut document = serializer.serialize_map(None)?;
        document.serialize_entry("specVersion", "1")?;
        document.serialize_entry("id", &self.id)?;
        if let Some(limits) = &self.resource_limits {
            document.serialize_entry("resourceLimits", limits)?;
        }
        document.serialize_entry("pluginInstances", &self.plugin_instances)?;
        document.serialize_entry("flows", &self.flows)?;
        document.end()
    }
}

impl fmt::Debug for VerifiedTenonDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedTenonDocument")
            .field("id", &self.id)
            .field("plugin_instances", &self.plugin_instances.keys())
            .field("flows", &self.flows.keys())
            .finish()
    }
}

/// Authored limits shared by a Pipeline and its Plugin descendants.
///
/// Numbers remain lossless JSON values. Integer projections are computed on
/// demand; the Document verifier establishes their exact integer representation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory_bytes: Option<Number>,
}

impl ResourceLimits {
    /// Returns the CPU ceiling in hundredths of one logical core.
    #[allow(
        clippy::expect_used,
        reason = "the verifier establishes the exact scaled representation"
    )]
    pub fn cpu_hundredths(&self) -> Option<u64> {
        self.cpu.as_ref().map(|number| {
            scaled_integer(number, 2).expect("verified CPU limit is a bounded number of hundredths")
        })
    }

    /// Returns the authored aggregate memory ceiling in bytes.
    #[allow(
        clippy::expect_used,
        reason = "the verifier establishes the exact integer representation"
    )]
    pub fn memory_bytes(&self) -> Option<u64> {
        self.memory_bytes
            .as_ref()
            .map(|number| scaled_integer(number, 0).expect("verified memory limit fits u64"))
    }

    /// Reports whether the authored object requests no additional limits.
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory_bytes.is_none()
    }
}

/// Establishes integer representation before any downstream projection.
fn validate_resource_numbers(json: &Value) -> Result<(), TenonDocumentVerificationError> {
    let mut issues = Vec::new();
    for (field, shift, maximum) in [("cpu", 2, u64::MAX / 1000), ("memoryBytes", 0, u64::MAX)] {
        let Some(number) = json
            .get("resourceLimits")
            .and_then(|limits| limits.get(field))
            .and_then(Value::as_number)
        else {
            continue;
        };
        if scaled_integer(number, shift).is_none_or(|value| value == 0 || value > maximum) {
            issues.push(TenonDocumentVerificationIssue::new(
                "tenon_document.schema_invalid",
                format!("/resourceLimits/{field}"),
            ));
        }
    }
    if issues.is_empty() {
        Ok(())
    } else {
        Err(TenonDocumentVerificationError::from_issues(
            issues.into_boxed_slice(),
        ))
    }
}

/// Establishes exact Flow limits and Queue arithmetic before Schema projection.
fn validate_flow_record_limits(json: &Value) -> Result<(), TenonDocumentVerificationError> {
    let Some(flows) = json.get("flows").and_then(Value::as_object) else {
        return Ok(());
    };
    let mut issues = Vec::new();
    for (id, flow) in flows {
        let mut limits = [None, None];
        for (index, (field, default)) in [
            ("maxPendingRecords", DEFAULT_MAX_PENDING_RECORDS),
            ("maxRecordBytes", DEFAULT_MAX_RECORD_BYTES),
        ]
        .into_iter()
        .enumerate()
        {
            limits[index] = match flow.get(field) {
                None => NonZeroU64::new(default),
                Some(value) => value
                    .as_number()
                    .and_then(|number| scaled_integer(number, 0).and_then(NonZeroU64::new)),
            };
            if limits[index].is_none() {
                issues.push(TenonDocumentVerificationIssue::new(
                    "tenon_document.schema_invalid",
                    Location::new().join("flows").join(id).join(field).as_str(),
                ));
            }
        }
        let [Some(pending), Some(bytes)] = limits else {
            continue;
        };
        match maximum_frame_len(bytes) {
            Err(_) => issues.push(TenonDocumentVerificationIssue::new(
                "flow.record_limit_invalid",
                Location::new()
                    .join("flows")
                    .join(id)
                    .join("maxRecordBytes")
                    .as_str(),
            )),
            Ok(frame) => {
                // The Java Source admission queue needs one additional shutdown slot.
                let admission_fits = i32::try_from(pending.get())
                    .ok()
                    .and_then(|count| count.checked_add(1))
                    .is_some();
                if !admission_fits || capacity_for_record_limit(pending, frame).is_err() {
                    issues.push(TenonDocumentVerificationIssue::new(
                        "flow.pending_limit_invalid",
                        Location::new()
                            .join("flows")
                            .join(id)
                            .join("maxPendingRecords")
                            .as_str(),
                    ));
                }
            }
        }
    }
    if issues.is_empty() {
        Ok(())
    } else {
        Err(TenonDocumentVerificationError::from_issues(ordered_issues(
            issues,
        )))
    }
}

#[expect(
    clippy::expect_used,
    reason = "the Document verifier establishes exact positive limits"
)]
fn effective_record_limit(number: Option<&Number>, default: u64) -> NonZeroU64 {
    NonZeroU64::new(number.map_or(default, |number| {
        scaled_integer(number, 0).expect("verified record limit fits u64")
    }))
    .expect("verified record limit and its default are positive")
}

/// Converts exactly the authored decimal times a power of ten into an integer.
fn scaled_integer(number: &Number, decimal_shift: i64) -> Option<u64> {
    let (mantissa, exponent) = number
        .as_str()
        .split_once(['e', 'E'])
        .unwrap_or((number.as_str(), "0"));
    let fractional_digits = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let shift = exponent
        .parse::<i64>()
        .ok()?
        .checked_add(decimal_shift)?
        .checked_sub(i64::try_from(fractional_digits).ok()?)?;
    let mut digits: String = mantissa
        .chars()
        .filter(|character| *character != '.')
        .collect();
    if shift < 0 {
        let retained = digits
            .len()
            .checked_sub(usize::try_from(shift.unsigned_abs()).ok()?)?;
        if !digits[retained..].bytes().all(|digit| digit == b'0') {
            return None;
        }
        digits.truncate(retained);
    } else {
        // A nonzero u64 has at most twenty decimal digits; bound work before
        // materializing zeros from an untrusted exponent.
        if shift > 19 {
            return None;
        }
        digits.extend(std::iter::repeat_n('0', usize::try_from(shift).ok()?));
    }
    digits.parse().ok()
}

/// Multiplies the authored decimal by the CPU count without floating-point rounding.
#[allow(
    clippy::expect_used,
    reason = "Schema admits only positive finite ratios with an approximate value at most ten"
)]
fn ceil_decimal_product(ratio: &Number, cpu_count: u128) -> u128 {
    let (mantissa, exponent) = ratio
        .as_str()
        .split_once(['e', 'E'])
        .unwrap_or((ratio.as_str(), "0"));
    let fractional_digits = mantissa
        .split_once('.')
        .map_or(0, |(_, digits)| digits.len());
    let mut decimal_places = fractional_digits as i64
        - exponent
            .parse::<i64>()
            .expect("a verified finite ratio has a bounded exponent");
    let mut digits = mantissa
        .trim_start_matches(['0', '.'])
        .bytes()
        .rev()
        .filter(|digit| *digit != b'.');
    let mut carry = 0;
    let mut whole = 0;
    let mut place = 10_u128.pow(
        u32::try_from(decimal_places.min(0).unsigned_abs())
            .expect("a ratio at most ten has a bounded positive exponent"),
    );
    let mut has_remainder = false;
    // Long multiplication consumes only authored digits and the bounded carry.
    // Leading zeros are excluded so scientific notation cannot inflate place.
    loop {
        match digits.next() {
            Some(digit) => carry += u128::from(digit - b'0') * cpu_count,
            None if carry == 0 => break,
            None => {}
        }
        let digit = carry % 10;
        carry /= 10;
        if decimal_places > 0 {
            has_remainder |= digit != 0;
            decimal_places -= 1;
        } else {
            whole += digit * place;
            place *= 10;
        }
    }
    whole + u128::from(has_remainder)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// One verified use of a Plugin Program with its authored configuration.
pub struct PluginInstance {
    #[serde(flatten)]
    program_identity: PluginProgramIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    extra_args: Option<ExtraArgs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    env: Option<BTreeMap<String, String>>,
    config: Value,
}

impl PluginInstance {
    /// Returns the exact Program identity shared by instances using this version.
    pub fn program_identity(&self) -> &PluginProgramIdentity {
        &self.program_identity
    }

    /// Returns the referenced Program name.
    pub fn program_name(&self) -> &ProgramName {
        self.program_identity.program_name()
    }

    /// Returns the exact referenced Program version.
    pub fn exact_version(&self) -> &ExactVersion {
        self.program_identity.exact_version()
    }

    /// Returns the unchanged authored Plugin configuration.
    pub fn config(&self) -> &Value {
        &self.config
    }

    /// Borrows authored arguments without materializing omitted fields.
    pub(crate) fn extra_args(&self) -> Option<&ExtraArgs> {
        self.extra_args.as_ref()
    }

    /// Borrows only the authored child-environment overrides.
    pub(crate) fn env(&self) -> Option<&BTreeMap<String, String>> {
        self.env.as_ref()
    }

    /// Compares effective startup settings without consulting the parent environment.
    pub(crate) fn has_same_startup_settings(&self, other: &Self) -> bool {
        fn effective_arguments(
            instance: &PluginInstance,
        ) -> Option<(&[String], ExtraArgsPosition)> {
            instance
                .extra_args()
                .filter(|extra| !extra.args().is_empty())
                .map(|extra| (extra.args(), extra.position()))
        }

        effective_arguments(self) == effective_arguments(other)
            && self.env().filter(|env| !env.is_empty()) == other.env().filter(|env| !env.is_empty())
    }
}

/// Literal per-Instance arguments with the authored insertion-position omission.
#[derive(Serialize, Deserialize)]
pub(crate) struct ExtraArgs {
    args: Box<[String]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<ExtraArgsPosition>,
}

impl ExtraArgs {
    pub(crate) fn args(&self) -> &[String] {
        &self.args
    }

    pub(crate) fn position(&self) -> ExtraArgsPosition {
        self.position.unwrap_or(ExtraArgsPosition::Append)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ExtraArgsPosition {
    Append,
    Prepend,
}

impl fmt::Debug for PluginInstance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginInstance")
            .field("program_name", self.program_identity.program_name())
            .field("exact_version", self.program_identity.exact_version())
            .field("config", &"[REDACTED]")
            .finish()
    }
}

#[derive(Serialize)]
/// One verified directional flow, including its authored concurrency proportion.
#[allow(non_snake_case, reason = "fields retain their Document spelling")]
pub struct Flow {
    // Preserve the authored number and omission; derive the effective value on read.
    #[serde(skip_serializing_if = "Option::is_none")]
    parallelism: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maxPendingRecords: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maxRecordBytes: Option<Number>,
    source: PluginInstanceId,
    #[serde(skip_serializing_if = "Option::is_none")]
    delivery: Option<SourceDelivery>,
    process: Process,
    sinks: Box<[PluginInstanceId]>,
}

impl Flow {
    /// Returns the maximum admitted incomplete Source sends per Channel.
    pub fn max_pending_records(&self) -> NonZeroU64 {
        effective_record_limit(self.maxPendingRecords.as_ref(), DEFAULT_MAX_PENDING_RECORDS)
    }

    /// Returns the maximum complete encoded IngressRecord or EgressRecord size.
    pub fn max_record_bytes(&self) -> NonZeroU64 {
        effective_record_limit(self.maxRecordBytes.as_ref(), DEFAULT_MAX_RECORD_BYTES)
    }

    /// Returns the Source Instance identity.
    pub fn source(&self) -> &PluginInstanceId {
        &self.source
    }

    pub(crate) fn delivery(&self) -> SourceDelivery {
        self.delivery.unwrap_or(SourceDelivery::AtLeastOnce)
    }

    /// Returns the authored Lua source.
    pub fn lua_source(&self) -> &str {
        &self.process.script
    }

    /// Returns the target Sink Instance identities.
    pub fn sinks(&self) -> &[PluginInstanceId] {
        &self.sinks
    }
}

impl fmt::Debug for Flow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Flow")
            .field("parallelism", &self.parallelism)
            .field("maxPendingRecords", &self.maxPendingRecords)
            .field("maxRecordBytes", &self.maxRecordBytes)
            .field("source", &self.source)
            .field("delivery", &self.delivery)
            .field("process", &"[REDACTED]")
            .field("sinks", &self.sinks)
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
struct Process {
    script: String,
}

struct RawDocument {
    id: String,
    resource_limits: Option<ResourceLimits>,
    plugin_instances: BTreeMap<String, PluginInstance>,
    flows: BTreeMap<String, RawFlow>,
}

impl RawDocument {
    #[allow(
        clippy::expect_used,
        reason = "Schema guards each typed field projection"
    )]
    fn from_validated_json(mut json: Value) -> Self {
        let Value::Object(instances) = json["pluginInstances"].take() else {
            unreachable!("Schema guarantees the Plugin Instances object");
        };
        let plugin_instances = instances
            .into_iter()
            .map(|(id, mut instance)| {
                let instance = PluginInstance {
                    program_identity: PluginProgramIdentity::from_parts(
                        ProgramName::from_verified(
                            serde_json::from_value(instance["programName"].take())
                                .expect("Schema guarantees a string Program name"),
                        ),
                        ExactVersion::from_verified(
                            serde_json::from_value(instance["exactVersion"].take())
                                .expect("Schema guarantees a string exact version"),
                        ),
                    ),
                    extra_args: serde_json::from_value(instance["extraArgs"].take())
                        .expect("Schema guarantees optional Instance arguments"),
                    env: serde_json::from_value(instance["env"].take())
                        .expect("Schema guarantees optional Instance environment overrides"),
                    // Move opaque config directly, without interpreting any object keys.
                    config: instance["config"].take(),
                };
                (id, instance)
            })
            .collect();
        Self {
            id: serde_json::from_value(json["id"].take())
                .expect("Schema guarantees a string Document id"),
            resource_limits: json.get_mut("resourceLimits").map(|limits| ResourceLimits {
                cpu: serde_json::from_value(limits["cpu"].take())
                    .expect("Schema guarantees optional CPU number"),
                memory_bytes: serde_json::from_value(limits["memoryBytes"].take())
                    .expect("Schema guarantees optional memory number"),
            }),
            plugin_instances,
            flows: serde_json::from_value(json["flows"].take())
                .expect("the Flow projection must match the validated Schema"),
        }
    }
}

#[derive(Deserialize)]
#[allow(non_snake_case, reason = "fields retain their Document spelling")]
struct RawFlow {
    parallelism: Option<Number>,
    maxPendingRecords: Option<Number>,
    maxRecordBytes: Option<Number>,
    source: String,
    delivery: Option<SourceDelivery>,
    process: Process,
    sinks: Vec<String>,
}

fn project(raw: RawDocument) -> VerifiedTenonDocument {
    let plugin_instances = raw
        .plugin_instances
        .into_iter()
        .map(|(id, instance)| (PluginInstanceId::from_verified(id), instance))
        .collect();
    let flows = raw
        .flows
        .into_iter()
        .map(|(id, flow)| {
            (
                FlowId::from_verified(id),
                Flow {
                    parallelism: flow.parallelism,
                    maxPendingRecords: flow.maxPendingRecords,
                    maxRecordBytes: flow.maxRecordBytes,
                    source: PluginInstanceId::from_verified(flow.source),
                    delivery: flow.delivery,
                    process: flow.process,
                    sinks: flow
                        .sinks
                        .into_iter()
                        .map(PluginInstanceId::from_verified)
                        .collect(),
                },
            )
        })
        .collect();
    VerifiedTenonDocument {
        id: TenonDocumentId::from_verified(raw.id),
        resource_limits: raw.resource_limits,
        plugin_instances,
        flows,
    }
}

fn validate_identifiers(raw: &RawDocument, issues: &mut Vec<TenonDocumentVerificationIssue>) {
    let paths = std::iter::once((raw.id.as_str(), Location::new().join("id")))
        .chain(raw.plugin_instances.keys().map(|id| {
            (
                id.as_str(),
                Location::new().join("pluginInstances").join(id),
            )
        }))
        .chain(
            raw.flows
                .keys()
                .map(|id| (id.as_str(), Location::new().join("flows").join(id))),
        );
    for (value, path) in paths {
        if let Err(error) = validate_local_id(value) {
            issues.push(TenonDocumentVerificationIssue::new(
                error.code(),
                path.as_str(),
            ));
        }
    }
}

fn validate_references(raw: &RawDocument, issues: &mut Vec<TenonDocumentVerificationIssue>) {
    let mut used = HashSet::new();
    let mut sources = HashSet::new();
    for (id, flow) in &raw.flows {
        let path = Location::new().join("flows").join(id);
        if raw.plugin_instances.contains_key(&flow.source) {
            used.insert(flow.source.as_str());
            if !sources.insert(flow.source.as_str()) {
                issues.push(TenonDocumentVerificationIssue::new(
                    "flow.source_reused",
                    path.join("source").as_str(),
                ));
            }
        } else {
            issues.push(TenonDocumentVerificationIssue::new(
                "flow.source_reference_invalid",
                path.join("source").as_str(),
            ));
        }
        for (index, sink) in flow.sinks.iter().enumerate() {
            if raw.plugin_instances.contains_key(sink) {
                used.insert(sink.as_str());
            } else {
                issues.push(TenonDocumentVerificationIssue::new(
                    "flow.sink_reference_invalid",
                    path.join("sinks").join(index).as_str(),
                ));
            }
        }
    }
    for id in raw.plugin_instances.keys() {
        if !used.contains(id.as_str()) {
            issues.push(TenonDocumentVerificationIssue::new(
                "plugin_instance.unused",
                Location::new().join("pluginInstances").join(id).as_str(),
            ));
        }
    }
}

fn ordered_issues(
    mut issues: Vec<TenonDocumentVerificationIssue>,
) -> Box<[TenonDocumentVerificationIssue]> {
    issues.sort_by(|left, right| {
        left.instance_path()
            .cmp(right.instance_path())
            .then(left.code().cmp(right.code()))
    });
    issues.dedup();
    issues.into_boxed_slice()
}

#[cfg(test)]
mod tests;
