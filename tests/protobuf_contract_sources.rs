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

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const CONTRACTS_DIRECTORY: &str = "contracts";

#[test]
fn all_protobuf_contract_sources_compile_with_the_pinned_compiler() -> io::Result<()> {
    let mut sources = Vec::new();
    collect_proto_sources(Path::new(CONTRACTS_DIRECTORY), &mut sources)?;
    sources.sort();

    assert!(!sources.is_empty(), "No Protobuf contract sources found");

    let protoc = protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?;
    let bundled_includes = protoc_bin_vendored::include_path().map_err(io::Error::other)?;
    let descriptor_path = std::env::temp_dir().join(format!(
        "tenon-protobuf-contract-validation-{}.pb",
        std::process::id()
    ));

    let status = Command::new(protoc)
        .arg(format!(
            "--descriptor_set_out={}",
            descriptor_path.display()
        ))
        .arg("--include_imports")
        .arg("--proto_path=.")
        .arg(format!("--proto_path={}", bundled_includes.display()))
        .args(&sources)
        .status()?;

    if descriptor_path.exists() {
        fs::remove_file(descriptor_path)?;
    }
    if !status.success() {
        return Err(io::Error::other("Protobuf contract validation failed"));
    }

    eprintln!("Validated {} Protobuf contract source(s)", sources.len());
    Ok(())
}

fn collect_proto_sources(directory: &Path, sources: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_proto_sources(&path, sources)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "proto")
        {
            sources.push(path);
        }
    }
    Ok(())
}
