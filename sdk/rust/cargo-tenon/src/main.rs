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

//! Builds and checks author Plugin packages through external Cargo contracts.
//!
//! Each invocation owns its synchronous work and reports one JSON result or
//! failure. Cargo compiles programs; this tool validates and packages them.

mod build;
mod check;

use check::CheckError;
use serde_json::json;
use std::env;
use std::ffi::OsString;
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run(env::args_os().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let report = json!({"error": {"code": error.code(), "message": error.to_string()}});
            eprintln!("{report}");
            ExitCode::FAILURE
        }
    }
}

fn run(mut arguments: Vec<OsString>) -> Result<(), CheckError> {
    if arguments
        .first()
        .is_some_and(|argument| argument == "tenon")
    {
        arguments.remove(0);
    }
    if arguments.len() == 1 && matches!(arguments[0].to_str(), Some("--help" | "-h")) {
        println!(
            "Usage: cargo tenon check <package-directory>\n       cargo tenon build|bundle [--manifest-path <Cargo.toml>] [--release] [--target <triple>] [--locked] [--offline] [--frozen] [--config <KEY=VALUE>]\nCheck validates existing package contracts. Build compiles and validates one Cargo package. Bundle also writes a deterministic tar.gz."
        );
        return Ok(());
    }
    let package = match arguments.split_first() {
        Some((command, [directory])) if command == "check" => {
            serde_json::json!(check::package(Path::new(directory))?)
        }
        Some((command, options)) if command == "build" => {
            build::run(options, build::Action::Build)?
        }
        Some((command, options)) if command == "bundle" => {
            build::run(options, build::Action::Bundle)?
        }
        _ => {
            return Err(CheckError::new(
                "cli.usage",
                "expected: cargo tenon check <package-directory> | build [options] | bundle [options]",
            ));
        }
    };
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &package).map_err(|error| {
        CheckError::caused_by("cli.output_failed", "cannot write command result", error)
    })?;
    writeln!(stdout).map_err(|error| {
        CheckError::caused_by("cli.output_failed", "cannot finish command result", error)
    })
}
