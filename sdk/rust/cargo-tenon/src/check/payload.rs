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

//! Checks the language-neutral descriptor profile and exact interface roots.
//!
//! Prost resolves the descriptor structure, reflection preserves otherwise lost
//! option and edition data, and the descriptor pool validates type references.
//! These are build-time checks on external bytes, independent of Tenon runtime
//! code. Only root names leave this module; no descriptor cache is retained.

use super::{CheckError, PluginInterface};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, ReflectMessage, Value};
use prost_types::{DescriptorProto, FileDescriptorSet};
use std::collections::HashSet;

pub(super) fn validate(
    bytes: &[u8],
    interface: PluginInterface,
) -> Result<PayloadRoots, CheckError> {
    let descriptor = FileDescriptorSet::decode(bytes).map_err(|error| {
        CheckError::caused_by(
            "payload_contract.descriptor_malformed",
            "payload.descriptor.pb is malformed",
            error,
        )
    })?;
    if descriptor.file.is_empty() {
        return Err(CheckError::new(
            "payload_contract.descriptor_empty",
            "payload descriptor contains no files",
        ));
    }
    let mut names = HashSet::new();
    for file in &descriptor.file {
        let Some(name) = file.name.as_deref().filter(|name| !name.is_empty()) else {
            return Err(CheckError::new(
                "payload_contract.file_name_missing",
                "descriptor file has no name",
            ));
        };
        if !names.insert(name) {
            return Err(CheckError::new(
                "payload_contract.file_name_duplicate",
                format!("duplicate descriptor file: {name}"),
            ));
        }
    }
    for file in &descriptor.file {
        for dependency in &file.dependency {
            if !names.contains(dependency.as_str()) {
                return Err(CheckError::new(
                    "payload_contract.dependency_missing",
                    format!("missing imported descriptor: {dependency}"),
                ));
            }
        }
    }
    encoded_profile(bytes)?;
    for file in &descriptor.file {
        if file.syntax.as_deref() != Some("proto3") {
            return Err(CheckError::new(
                "payload_contract.syntax_unsupported",
                format!("{} must use proto3", file.name()),
            ));
        }
        if file
            .source_code_info
            .as_ref()
            .is_none_or(|info| info.location.is_empty())
        {
            return Err(CheckError::new(
                "payload_contract.source_info_missing",
                format!("{} has no source information", file.name()),
            ));
        }
        for message in &file.message_type {
            portable_field_names(message, file.name())?;
        }
    }
    let source_count = root_count(&descriptor, "SourceRecordPayload");
    let sink_count = root_count(&descriptor, "SinkRecordPayload");
    match interface {
        PluginInterface::Source => {
            require_root(
                source_count,
                "SourceRecordPayload",
                "payload_contract.source_root_missing",
                "payload_contract.source_root_not_unique",
            )?;
            reject_undeclared_root(sink_count)?;
        }
        PluginInterface::Sink => {
            require_root(
                sink_count,
                "SinkRecordPayload",
                "payload_contract.sink_root_missing",
                "payload_contract.sink_root_not_unique",
            )?;
            reject_undeclared_root(source_count)?;
        }
        PluginInterface::SourceAndSink => {
            require_root(
                source_count,
                "SourceRecordPayload",
                "payload_contract.source_root_missing",
                "payload_contract.source_root_not_unique",
            )?;
            require_root(
                sink_count,
                "SinkRecordPayload",
                "payload_contract.sink_root_missing",
                "payload_contract.sink_root_not_unique",
            )?;
        }
    }
    let pool = DescriptorPool::decode(bytes).map_err(|error| {
        CheckError::caused_by(
            "payload_contract.descriptor_invalid",
            "payload descriptor has an invalid type graph",
            error,
        )
    })?;
    let root_name = |name| {
        pool.all_messages()
            .find(|message| message.parent_message().is_none() && message.name() == name)
            .map(|message| message.full_name().to_owned())
    };
    Ok(PayloadRoots {
        source: root_name("SourceRecordPayload"),
        sink: root_name("SinkRecordPayload"),
    })
}

#[derive(Debug)]
pub(super) struct PayloadRoots {
    pub(super) source: Option<String>,
    pub(super) sink: Option<String>,
}

#[expect(
    clippy::expect_used,
    reason = "prost-reflect provides the standard FileDescriptorSet descriptor"
)]
fn encoded_profile(bytes: &[u8]) -> Result<(), CheckError> {
    let message_type = DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .expect("the standard FileDescriptorSet descriptor must exist");
    let message = DynamicMessage::decode(message_type, bytes).map_err(|error| {
        CheckError::caused_by(
            "payload_contract.descriptor_malformed",
            "payload descriptor cannot be decoded with reflection",
            error,
        )
    })?;
    inspect_message(&message)
}

fn inspect_message(message: &DynamicMessage) -> Result<(), CheckError> {
    let descriptor = message.descriptor();
    if descriptor.full_name() == "google.protobuf.FileDescriptorProto"
        && (message.has_field_by_number(14)
            || message.unknown_fields().any(|field| field.number() == 14))
    {
        return Err(CheckError::new(
            "payload_contract.editions_unsupported",
            "payload descriptor uses Editions",
        ));
    }
    let custom_option_definition = descriptor.full_name() == "google.protobuf.FieldDescriptorProto"
        && message
            .get_field_by_name("extendee")
            .is_some_and(|value| value.as_str().is_some_and(is_option_message));
    let custom_option_data = is_option_message(descriptor.full_name())
        && (message.unknown_fields().next().is_some() || message.extensions().next().is_some());
    if custom_option_definition || custom_option_data {
        return Err(CheckError::new(
            "payload_contract.custom_option_unsupported",
            "payload descriptor contains a custom option",
        ));
    }
    for (_, value) in message.fields() {
        inspect_value(value)?;
    }
    Ok(())
}

fn inspect_value(value: &Value) -> Result<(), CheckError> {
    match value {
        Value::Message(message) => inspect_message(message),
        Value::List(values) => {
            for value in values {
                inspect_value(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn is_option_message(name: &str) -> bool {
    matches!(
        name.trim_start_matches('.'),
        "google.protobuf.FileOptions"
            | "google.protobuf.MessageOptions"
            | "google.protobuf.FieldOptions"
            | "google.protobuf.OneofOptions"
            | "google.protobuf.EnumOptions"
            | "google.protobuf.EnumValueOptions"
            | "google.protobuf.ServiceOptions"
            | "google.protobuf.MethodOptions"
            | "google.protobuf.ExtensionRangeOptions"
    )
}

fn portable_field_names(message: &DescriptorProto, file: &str) -> Result<(), CheckError> {
    let mut names = HashSet::new();
    for field in &message.field {
        let Some(name) = field.name.as_deref().filter(|name| !name.is_empty()) else {
            continue;
        };
        let normalized: String = name
            .chars()
            .filter(|character| *character != '_')
            .map(|character| character.to_ascii_lowercase())
            .collect();
        if !names.insert(normalized) {
            return Err(CheckError::new(
                "payload_contract.field_name_collision",
                format!(
                    "portable field names collide in {file}, message {}, field {name}",
                    message.name()
                ),
            ));
        }
    }
    for nested in &message.nested_type {
        portable_field_names(nested, file)?;
    }
    Ok(())
}

fn root_count(descriptor: &FileDescriptorSet, name: &str) -> usize {
    descriptor
        .file
        .iter()
        .flat_map(|file| &file.message_type)
        .filter(|message| message.name.as_deref() == Some(name))
        .count()
}

fn require_root(
    count: usize,
    name: &str,
    missing: &'static str,
    duplicate: &'static str,
) -> Result<(), CheckError> {
    match count {
        0 => Err(CheckError::new(
            missing,
            format!("missing top-level {name}"),
        )),
        1 => Ok(()),
        _ => Err(CheckError::new(
            duplicate,
            format!("duplicate top-level {name}"),
        )),
    }
}

fn reject_undeclared_root(count: usize) -> Result<(), CheckError> {
    if count == 0 {
        Ok(())
    } else {
        Err(CheckError::new(
            "payload_contract.undeclared_interface_root",
            "descriptor contains a root for an undeclared interface",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn unknown_option_data_is_rejected_before_graph_resolution()
    -> Result<(), Box<dyn std::error::Error>> {
        let descriptor = DescriptorPool::global()
            .get_message_by_name("google.protobuf.FieldOptions")
            .ok_or("missing FieldOptions descriptor")?;
        let option = DynamicMessage::decode(descriptor, [0x88, 0xb5, 0x18, 0x01].as_slice())?;
        let error = inspect_message(&option)
            .err()
            .ok_or("custom option was accepted")?;
        assert_eq!(error.code(), "payload_contract.custom_option_unsupported");
        Ok(())
    }

    proptest! {
        #[test]
        fn arbitrary_descriptor_bytes_fail_or_resolve_an_exact_root(bytes in prop::collection::vec(any::<u8>(), 0..8192)) {
            if let Ok(roots) = validate(&bytes, PluginInterface::Sink) {
                prop_assert!(roots.source.is_none());
                prop_assert!(roots.sink.is_some());
                prop_assert!(DescriptorPool::decode(bytes.as_slice()).is_ok());
            }
        }
    }
}
