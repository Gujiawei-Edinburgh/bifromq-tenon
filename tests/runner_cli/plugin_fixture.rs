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

#![allow(
    dead_code,
    reason = "Shared fixture exports differ by integration test"
)]

use flate2::Compression;
use flate2::write::GzEncoder;
use prost::Message as _;
use prost_types::{
    DescriptorProto, FileDescriptorProto, FileDescriptorSet, SourceCodeInfo, source_code_info,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

pub(crate) use tenon::runner_test_support::contracts::core::PluginInterface;
#[path = "../support/controlled_plugin.rs"]
mod controlled_plugin;
pub(crate) use controlled_plugin::controlled_program_command;

/// Original contract material for checking read-only Program responses.
pub(crate) struct InstalledProgramFixture {
    pub(crate) config_schema: Vec<u8>,
    pub(crate) payload_descriptor: Vec<u8>,
}

/// Seeds the current on-disk layout with the same runnable Program used by uploads.
pub(crate) fn install_program(
    state_directory: &Path,
    interface: PluginInterface,
) -> io::Result<InstalledProgramFixture> {
    let files = program_files(interface, None)?;
    let (program_name, _) = program_identity(interface);
    let plugins = state_directory.join("plugins");
    let programs = plugins.join("programs");
    let program_root = programs.join(program_name);
    let package = program_root.join("1.0.0");
    for directory in [&plugins, &programs, &program_root, &package] {
        create_private_directory(directory)?;
    }
    for (path, bytes) in &files {
        write_private_file(&package.join(path), bytes)?;
    }
    Ok(InstalledProgramFixture {
        config_schema: files["config.schema.json"].clone(),
        payload_descriptor: files["payload.descriptor.pb"].clone(),
    })
}

pub(crate) fn plugin_package(interface: PluginInterface) -> io::Result<Vec<u8>> {
    archive(program_files(interface, None)?)
}

pub(crate) fn plugin_package_with_program(
    interface: PluginInterface,
    program: &[u8],
) -> io::Result<Vec<u8>> {
    archive(program_files(interface, Some(program))?)
}

fn program_files(
    interface: PluginInterface,
    program: Option<&[u8]>,
) -> io::Result<BTreeMap<String, Vec<u8>>> {
    let (program_name, interface_text) = program_identity(interface);
    let command = if program.is_some() {
        vec!["./plugin.sh".to_owned()]
    } else {
        controlled_program_command(interface)?
    };
    let mut files = BTreeMap::from([
        (
            "manifest.json".into(),
            serde_json::to_vec(&json!({
                "programName": program_name, "exactVersion": "1.0.0",
                "interface": interface_text, "displayName": "Example Plugin", "description": "Read and write example records.", "command": command,
                "platforms": platforms()
            }))?,
        ),
        (
            "config.schema.json".into(),
            serde_json::to_vec(&json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "endpoint": {"type": "string"},
                    "behavior": {"type": "string"},
                    "traffic": {"type": "object"}
                },
                "additionalProperties": false
            }))?,
        ),
        (
            "payload.descriptor.pb".into(),
            program_payload_descriptor(interface),
        ),
    ]);
    if let Some(program) = program {
        files.insert("plugin.sh".into(), program.to_vec());
    }
    Ok(files)
}

fn program_identity(interface: PluginInterface) -> (&'static str, &'static str) {
    match interface {
        PluginInterface::Source => ("com.example.modbus", "source"),
        PluginInterface::Sink => ("com.example.kafka", "sink"),
        PluginInterface::SourceAndSink => ("com.example.gateway", "source-and-sink"),
    }
}

fn archive(files: BTreeMap<String, Vec<u8>>) -> io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for (path, bytes) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(u64::try_from(bytes.len()).map_err(io::Error::other)?);
        header.set_mode(0o500);
        header.set_cksum();
        archive.append_data(&mut header, &path, bytes.as_slice())?;
    }
    archive.into_inner()?.finish()
}

pub(crate) fn program_payload_descriptor(interface: PluginInterface) -> Vec<u8> {
    let file = match interface {
        PluginInterface::Source => {
            vec![payload_descriptor_file(PluginInterface::Source)]
        }
        PluginInterface::Sink => {
            vec![payload_descriptor_file(PluginInterface::Sink)]
        }
        PluginInterface::SourceAndSink => vec![
            payload_descriptor_file(PluginInterface::Source),
            payload_descriptor_file(PluginInterface::Sink),
        ],
    };
    FileDescriptorSet { file }.encode_to_vec()
}

fn payload_descriptor_file(kind: PluginInterface) -> FileDescriptorProto {
    let (file_name, message_name, package) = match kind {
        PluginInterface::Source => (
            "source_record_payload.proto",
            "SourceRecordPayload",
            "com.example.modbus",
        ),
        PluginInterface::SourceAndSink => unreachable!("Each file describes exactly one interface"),
        PluginInterface::Sink => (
            "sink_record_payload.proto",
            "SinkRecordPayload",
            "com.example.kafka",
        ),
    };
    FileDescriptorProto {
        name: Some(String::from(file_name)),
        package: Some(String::from(package)),
        message_type: vec![DescriptorProto {
            name: Some(String::from(message_name)),
            ..Default::default()
        }],
        source_code_info: Some(SourceCodeInfo {
            location: vec![source_code_info::Location::default()],
        }),
        syntax: Some(String::from("proto3")),
        ..Default::default()
    }
}

fn create_private_directory(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

fn write_private_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    fs::write(path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o500))
}

pub(crate) fn platforms() -> serde_json::Value {
    json!([{"os":"linux","architecture":"amd64"},{"os":"linux","architecture":"arm64"},{"os":"darwin","architecture":"amd64"},{"os":"darwin","architecture":"arm64"}])
}
