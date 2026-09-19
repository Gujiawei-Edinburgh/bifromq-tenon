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

//! Builds one author package with Cargo and packages its validated outputs.
//!
//! Cargo owns compilation, dependency resolution and target selection. This
//! synchronous invocation owns its subprocesses and temporary archive; no
//! Plugin is executed. Only a complete archive replaces the destination.
//! Artifact locations come from Cargo messages, never a target-directory scan.

use super::check::{self, CheckError};
use flate2::{Compression, GzBuilder};
use serde::Deserialize;
use serde_json::{Value, json};
use std::env;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::NamedTempFile;

#[derive(Clone, Copy, Debug)]
pub(super) enum Action {
    Build,
    Bundle,
}

#[expect(
    clippy::expect_used,
    reason = "a canonical Cargo.toml has a parent directory"
)]
pub(super) fn run(arguments: &[OsString], action: Action) -> Result<Value, CheckError> {
    let mut options = Options::parse(arguments)?;
    let target = match options.target.as_deref() {
        Some(target) => target.to_owned(),
        None => host_target()?,
    };
    let platform = platform(&target)?;
    options.manifest = fs::canonicalize(&options.manifest).map_err(|error| {
        CheckError::caused_by("project.read_failed", "cannot locate Cargo.toml", error)
    })?;
    let metadata: Metadata = serde_json::from_slice(&execute(
        options
            .cargo("metadata")
            .args(["--no-deps", "--format-version=1"]),
    )?)
    .map_err(|error| {
        CheckError::caused_by("cargo.invalid_output", "invalid Cargo metadata", error)
    })?;
    let package = metadata
        .packages
        .into_iter()
        .find(|package| package.manifest_path == options.manifest)
        .ok_or_else(|| {
            CheckError::new(
                "project.invalid",
                "select a package Cargo.toml, not a virtual workspace",
            )
        })?;
    let [binary] = package
        .targets
        .iter()
        .filter(|target| target.kind.iter().any(|kind| kind == "bin"))
        .collect::<Vec<_>>()[..]
    else {
        return Err(CheckError::new(
            "project.invalid",
            "the Plugin package must define exactly one binary",
        ));
    };
    let manifest = json!({
        "programName": package.metadata["tenon"]["program-name"],
        "exactVersion": package.version,
        "displayName": package.metadata["tenon"]["display-name"],
        "description": package.metadata["tenon"]["description"],
        "interface": package.metadata["tenon"]["interface"],
        "platforms": [platform],
        "command": ["./program"]
    })
    .to_string()
    .into_bytes();
    let checked_manifest = check::manifest(&manifest)?;
    let mut command = options.cargo("build");
    command.args([
        "--package",
        &package.name,
        "--bin",
        &binary.name,
        "--target",
        &target,
        "--message-format=json",
    ]);
    if let Profile::Release = options.profile {
        command.arg("--release");
    }
    let output = execute(&mut command)?;
    let messages = output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice::<Message>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            CheckError::caused_by("cargo.invalid_output", "invalid Cargo build message", error)
        })?;
    let executable = messages
        .iter()
        .find_map(|message| match message {
            Message::CompilerArtifact {
                package_id,
                executable: Some(path),
            } if package_id == &package.id => Some(path),
            _ => None,
        })
        .ok_or_else(|| {
            CheckError::new(
                "build.missing_executable",
                "Cargo did not report the package executable",
            )
        })?;
    let out_dir = messages
        .iter()
        .find_map(|message| match message {
            Message::BuildScriptExecuted {
                package_id,
                out_dir,
            } if package_id == &package.id => Some(out_dir),
            _ => None,
        })
        .ok_or_else(|| {
            CheckError::new(
                "build.missing_descriptor",
                "the package must generate OUT_DIR/payload.descriptor.pb in build.rs",
            )
        })?;
    let project = options
        .manifest
        .parent()
        .expect("a canonical Cargo.toml has a parent");
    let schema = check::read_file(project, "config.schema.json")?;
    let descriptor = check::read_file(out_dir, "payload.descriptor.pb")?;
    let checked = checked_manifest.check(&schema, &descriptor)?;
    let mut report = json!({"package": checked, "target": target, "executable": executable,
        "descriptor": out_dir.join("payload.descriptor.pb")});
    if let Action::Bundle = action {
        let directory = metadata
            .target_directory
            .join("tenon")
            .join(&target)
            .join(options.profile.directory());
        fs::create_dir_all(&directory).map_err(|error| {
            CheckError::caused_by(
                "bundle.write_failed",
                "cannot create bundle directory",
                error,
            )
        })?;
        let destination = directory.join(format!("{}-{}.tar.gz", package.name, package.version));
        write_bundle(&destination, &manifest, &schema, &descriptor, executable).map_err(
            |error| {
                CheckError::caused_by(
                    "bundle.write_failed",
                    format!("cannot write {}", destination.display()),
                    error,
                )
            },
        )?;
        report["bundle"] = json!(destination);
    }
    Ok(report)
}

#[derive(Debug)]
struct Options {
    manifest: PathBuf,
    target: Option<String>,
    profile: Profile,
    cargo_arguments: Vec<OsString>,
}

impl Options {
    fn parse(arguments: &[OsString]) -> Result<Self, CheckError> {
        let mut options = Self {
            manifest: PathBuf::from("Cargo.toml"),
            target: None,
            profile: Profile::Debug,
            cargo_arguments: Vec::new(),
        };
        let mut arguments = arguments.iter();
        while let Some(argument) = arguments.next() {
            match argument.to_str() {
                Some("--release") => options.profile = Profile::Release,
                Some("--locked" | "--offline" | "--frozen") => {
                    options.cargo_arguments.push(argument.clone())
                }
                Some("--manifest-path" | "--target" | "--config") => {
                    let value = arguments
                        .next()
                        .ok_or_else(|| CheckError::new("cli.usage", "missing option value"))?;
                    match argument.to_str() {
                        Some("--manifest-path") => options.manifest = value.into(),
                        Some("--target") => {
                            options.target = Some(
                                value
                                    .to_str()
                                    .ok_or_else(|| {
                                        CheckError::new("cli.usage", "target must be UTF-8")
                                    })?
                                    .to_owned(),
                            )
                        }
                        _ => options
                            .cargo_arguments
                            .extend([argument.clone(), value.clone()]),
                    }
                }
                _ => {
                    return Err(CheckError::new(
                        "cli.usage",
                        format!("unknown build option: {}", argument.to_string_lossy()),
                    ));
                }
            }
        }
        Ok(options)
    }

    fn cargo(&self, action: &str) -> Command {
        let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
        command
            .arg(action)
            .arg("--manifest-path")
            .arg(&self.manifest)
            .args(&self.cargo_arguments);
        command
    }
}

#[derive(Debug)]
enum Profile {
    Debug,
    Release,
}

impl Profile {
    fn directory(&self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
        }
    }
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    target_directory: PathBuf,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    version: String,
    manifest_path: PathBuf,
    metadata: Value,
    targets: Vec<Target>,
}

#[derive(Deserialize)]
struct Target {
    name: String,
    kind: Vec<String>,
}

#[derive(Deserialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
enum Message {
    CompilerArtifact {
        package_id: String,
        executable: Option<PathBuf>,
    },
    BuildScriptExecuted {
        package_id: String,
        out_dir: PathBuf,
    },
    #[serde(other)]
    Other,
}

fn host_target() -> Result<String, CheckError> {
    let output = execute(Command::new("rustc").arg("-vV"))?;
    String::from_utf8_lossy(&output)
        .lines()
        .find_map(|line| line.strip_prefix("host: ").map(str::to_owned))
        .ok_or_else(|| {
            CheckError::new(
                "build.unsupported_target",
                "rustc did not report its host target",
            )
        })
}

fn platform(target: &str) -> Result<Value, CheckError> {
    let (os, architecture) = match target {
        "x86_64-apple-darwin" => ("darwin", "amd64"),
        "aarch64-apple-darwin" => ("darwin", "arm64"),
        "x86_64-unknown-linux-gnu" | "x86_64-unknown-linux-musl" => ("linux", "amd64"),
        "aarch64-unknown-linux-gnu" | "aarch64-unknown-linux-musl" => ("linux", "arm64"),
        _ => {
            return Err(CheckError::new(
                "build.unsupported_target",
                format!("unsupported Plugin target: {target}"),
            ));
        }
    };
    Ok(json!({"os": os, "architecture": architecture}))
}

fn execute(command: &mut Command) -> Result<Vec<u8>, CheckError> {
    let Output {
        status,
        stdout,
        stderr,
    } = command.output().map_err(|error| {
        CheckError::caused_by(
            "cargo.execution_failed",
            format!("cannot execute {}", command.get_program().to_string_lossy()),
            error,
        )
    })?;
    if !status.success() {
        let diagnostics: String = stdout
            .split(|byte| *byte == b'\n')
            .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
            .filter_map(|message| message["message"]["rendered"].as_str().map(str::to_owned))
            .collect();
        return Err(CheckError::new(
            "cargo.failed",
            format!(
                "{} failed ({status}): {diagnostics}{}",
                command.get_program().to_string_lossy(),
                String::from_utf8_lossy(&stderr)
            ),
        ));
    }
    Ok(stdout)
}

#[expect(
    clippy::expect_used,
    reason = "the constructed bundle destination has a parent directory"
)]
fn write_bundle(
    destination: &Path,
    manifest: &[u8],
    schema: &[u8],
    descriptor: &[u8],
    executable: &Path,
) -> io::Result<()> {
    let directory = destination
        .parent()
        .expect("the bundle destination has a parent");
    let mut temporary = NamedTempFile::new_in(directory)?;
    let encoder = GzBuilder::new()
        .mtime(0)
        .write(&mut temporary, Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for (name, bytes) in [
        ("config.schema.json", schema),
        ("manifest.json", manifest),
        ("payload.descriptor.pb", descriptor),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive.append_data(&mut header, name, bytes)?;
    }
    let mut program = File::open(executable)?;
    let mut header = tar::Header::new_gnu();
    header.set_mtime(0);
    header.set_uid(0);
    header.set_gid(0);
    header.set_size(program.metadata()?.len());
    header.set_mode(0o755);
    header.set_cksum();
    archive.append_data(&mut header, "program", &mut program)?;
    archive.into_inner()?.finish()?.flush()?;
    temporary
        .persist(destination)
        .map_err(|error| error.error)?;
    Ok(())
}
