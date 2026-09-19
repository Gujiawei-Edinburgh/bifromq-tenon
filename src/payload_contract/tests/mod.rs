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

mod vectors;

use super::*;

#[test]
fn portable_field_name_validation_reports_imported_nested_message() -> Result<(), &'static str> {
    let descriptor_set = FileDescriptorSet {
        file: vec![prost_types::FileDescriptorProto {
            name: Some(String::from("helper.proto")),
            package: Some(String::from("com.example")),
            message_type: vec![DescriptorProto {
                name: Some(String::from("Helper")),
                nested_type: vec![DescriptorProto {
                    name: Some(String::from("Nested")),
                    field: vec![
                        prost_types::FieldDescriptorProto {
                            name: Some(String::from("device_id")),
                            ..Default::default()
                        },
                        prost_types::FieldDescriptorProto {
                            name: Some(String::from("Device_id")),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let error = validate_portable_field_names(&descriptor_set)
        .err()
        .ok_or("Portable field name collision was accepted")?;
    assert_eq!(
        error,
        PayloadContractError::FieldNameCollision {
            file_name: String::from("helper.proto"),
            message_name: String::from("com.example.Helper.Nested"),
            first_field: String::from("device_id"),
            second_field: String::from("Device_id"),
        }
    );
    Ok(())
}

#[test]
fn unknown_option_fields_are_custom_data() -> Result<(), &'static str> {
    let descriptor = DescriptorPool::global()
        .get_message_by_name("google.protobuf.FieldOptions")
        .ok_or("FieldOptions descriptor is missing")?;
    let options = DynamicMessage::decode(descriptor, [0x88, 0xb5, 0x18, 0x01].as_slice())
        .map_err(|_| "Unknown option fixture failed to decode")?;

    if !options_contain_custom_data(options) {
        return Err("Unknown option field was accepted");
    }
    Ok(())
}

impl PayloadContractError {
    /// Returns the stable machine-readable error code.
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::DescriptorMalformed => "payload_contract.descriptor_malformed",
            Self::DescriptorEmpty => "payload_contract.descriptor_empty",
            Self::FileNameMissing => "payload_contract.file_name_missing",
            Self::FileNameDuplicate { .. } => "payload_contract.file_name_duplicate",
            Self::DependencyMissing { .. } => "payload_contract.dependency_missing",
            Self::EditionsUnsupported { .. } => "payload_contract.editions_unsupported",
            Self::CustomOptionUnsupported { .. } => "payload_contract.custom_option_unsupported",
            Self::SyntaxUnsupported { .. } => "payload_contract.syntax_unsupported",
            Self::SourceInfoMissing { .. } => "payload_contract.source_info_missing",
            Self::FieldNameCollision { .. } => "payload_contract.field_name_collision",
            Self::DescriptorInvalid => "payload_contract.descriptor_invalid",
            Self::SourceRootMissing => "payload_contract.source_root_missing",
            Self::SourceRootNotUnique => "payload_contract.source_root_not_unique",
            Self::SinkRootMissing => "payload_contract.sink_root_missing",
            Self::SinkRootNotUnique => "payload_contract.sink_root_not_unique",
            Self::UndeclaredInterfaceRoot => "payload_contract.undeclared_interface_root",
        }
    }
}
