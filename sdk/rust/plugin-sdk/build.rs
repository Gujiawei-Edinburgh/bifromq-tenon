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

//! Generates the SDK's private messages from the shared wire contracts.

use std::{env, io, path::PathBuf};

fn main() -> io::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=contracts/process_control.proto");
    println!("cargo:rerun-if-changed=contracts/ingress_record.proto");
    println!("cargo:rerun-if-changed=contracts/egress_record.proto");
    let protoc = protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?;
    let output =
        PathBuf::from(env::var_os("OUT_DIR").ok_or_else(|| io::Error::other("missing OUT_DIR"))?);
    let mut config = prost_build::Config::new();
    config.protoc_executable(&protoc);
    config.bytes([
        ".tenon.source.IngressRecord.payload",
        ".tenon.sink.EgressRecord.payload",
    ]);
    config.compile_protos(
        &[
            "contracts/ingress_record.proto",
            "contracts/egress_record.proto",
        ],
        &["contracts"],
    )?;
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.file_descriptor_set_path(output.join("process_control_descriptor.pb"));
    tonic_prost_build::configure()
        .build_server(std::env::var_os("CARGO_FEATURE_REPOSITORY_TEST_SUPPORT").is_some())
        .compile_with_config(config, &["contracts/process_control.proto"], &["contracts"])
}
