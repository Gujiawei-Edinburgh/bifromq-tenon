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

use std::{env, error::Error, path::PathBuf};
fn main() -> Result<(), Box<dyn Error>> {
    let protos = [
        "proto/mqtt_message.proto",
        "proto/source_record_payload.proto",
        "proto/sink_record_payload.proto",
    ];
    println!("cargo:rerun-if-changed=build.rs");
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.prost_path("::tenon_plugin_sdk::prost");
    config.file_descriptor_set_path(out.join("payload.descriptor.pb"));
    config.compile_protos(&protos, &["proto"])?;
    Ok(())
}
