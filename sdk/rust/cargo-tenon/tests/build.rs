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

//! Exercises Cargo project inputs through the installed command boundary.

use serde_json::Value;
use std::error::Error;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::Command;
use tempfile::tempdir;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn invalid_project_identity_is_rejected_before_compilation() -> TestResult {
    for (name, version, interface) in [
        ("../../outside", "0.1.0", "source"),
        ("com.example.plugin", "0.1.0+build", "source"),
        ("com.example.plugin", "0.1.0", "invalid"),
    ] {
        let project = tempdir()?;
        fs::create_dir(project.path().join("src"))?;
        fs::write(
            project.path().join("src/main.rs"),
            "compile_error!(\"must not compile invalid metadata\");",
        )?;
        fs::write(
            project.path().join("Cargo.toml"),
            format!(
                "[package]\nname = \"example\"\nversion = \"{version}\"\nedition = \"2024\"\n[package.metadata.tenon]\ndisplay-name = \"Example Plugin\"\ndescription = \"Read and write example records.\"\nprogram-name = \"{name}\"\ninterface = \"{interface}\"\n"
            ),
        )?;
        let output = Command::new(env!("CARGO_BIN_EXE_cargo-tenon"))
            .args(["bundle", "--offline"])
            .current_dir(project.path())
            .output()?;
        assert!(!output.status.success());
        let report: Value = serde_json::from_slice(&output.stderr)?;
        assert_eq!(report["error"]["code"], "manifest.invalid", "{report}");
        assert!(!project.path().join("target").exists());
        assert!(output.stdout.is_empty());
    }
    Ok(())
}

#[test]
fn unsupported_platform_is_rejected_before_compilation() -> TestResult {
    let project = tempdir()?;
    let output = Command::new(env!("CARGO_BIN_EXE_cargo-tenon"))
        .args(["build", "--target", "wasm32-unknown-unknown"])
        .current_dir(project.path())
        .output()?;
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stderr)?;
    assert_eq!(
        report["error"]["code"], "build.unsupported_target",
        "{report}"
    );
    assert!(!project.path().join("target").exists());
    Ok(())
}

#[test]
fn bundle_validates_real_outputs_and_preserves_previous_archive_on_failure() -> TestResult {
    let project = tempdir()?;
    fs::create_dir(project.path().join("src"))?;
    fs::write(project.path().join("src/main.rs"), "fn main() {}")?;
    fs::write(
        project.path().join("Cargo.toml"),
        "[package]\nname = \"example\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[package.metadata.tenon]\ndisplay-name = \"Example Plugin\"\ndescription = \"Read and write example records.\"\nprogram-name = \"com.example.plugin\"\ninterface = \"source\"\n",
    )?;
    fs::write(
        project.path().join("build.rs"),
        "fn main() -> Result<(), Box<dyn std::error::Error>> { let out = std::env::var(\"OUT_DIR\")?; std::fs::copy(\"payload.pb\", std::path::PathBuf::from(out).join(\"payload.descriptor.pb\"))?; println!(\"cargo:rerun-if-changed=payload.pb\"); Ok(()) }",
    )?;
    fs::write(
        project.path().join("payload.proto"),
        "syntax = \"proto3\"; package example; message SourceRecordPayload { string message = 1; }",
    )?;
    let protoc = Command::new(protoc_bin_vendored::protoc_bin_path()?)
        .args([
            "--include_source_info",
            "--descriptor_set_out=payload.pb",
            "payload.proto",
        ])
        .current_dir(project.path())
        .output()?;
    assert!(
        protoc.status.success(),
        "{}",
        String::from_utf8_lossy(&protoc.stderr)
    );
    let schema = b"{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"object\",\"properties\":{},\"additionalProperties\":false}";
    fs::write(project.path().join("config.schema.json"), schema)?;

    let invoke = || {
        Command::new(env!("CARGO_BIN_EXE_cargo-tenon"))
            .args(["bundle", "--offline", "--manifest-path"])
            .arg(project.path().join("Cargo.toml"))
            .output()
    };
    let first = invoke()?;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let report: Value = serde_json::from_slice(&first.stdout)?;
    let bundle = Path::new(report["bundle"].as_str().ok_or("bundle path missing")?);
    let bytes = fs::read(bundle)?;
    let again = invoke()?;
    assert!(
        again.status.success(),
        "{}",
        String::from_utf8_lossy(&again.stderr)
    );
    assert_eq!(bytes, fs::read(bundle)?);
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let mut names = Vec::new();
    for entry in archive.entries()? {
        let entry = entry?;
        assert!(entry.header().entry_type().is_file());
        assert_eq!(entry.header().mtime()?, 0);
        assert_eq!(entry.header().uid()?, 0);
        assert_eq!(entry.header().gid()?, 0);
        let name = entry.path()?.to_string_lossy().into_owned();
        assert_eq!(
            entry.header().mode()?,
            if name == "program" { 0o755 } else { 0o644 }
        );
        names.push(name);
    }
    assert_eq!(
        names,
        [
            "config.schema.json",
            "manifest.json",
            "payload.descriptor.pb",
            "program"
        ]
    );

    fs::remove_file(bundle)?;
    fs::create_dir(bundle)?;
    fs::write(bundle.join("preserved.tar.gz"), &bytes)?;
    let blocked = invoke()?;
    assert!(!blocked.status.success());
    let error: Value = serde_json::from_slice(&blocked.stderr)?;
    assert_eq!(error["error"]["code"], "bundle.write_failed", "{error}");
    assert_eq!(bytes, fs::read(bundle.join("preserved.tar.gz"))?);
    assert_eq!(
        fs::read_dir(bundle.parent().ok_or("bundle parent missing")?)?.count(),
        1
    );
    fs::remove_file(bundle.join("preserved.tar.gz"))?;
    fs::remove_dir(bundle)?;
    fs::write(bundle, &bytes)?;

    let executable = Path::new(report["executable"].as_str().ok_or("executable missing")?);
    fs::set_permissions(executable, fs::Permissions::from_mode(0o600))?;
    let repackaged = invoke()?;
    assert!(
        repackaged.status.success(),
        "{}",
        String::from_utf8_lossy(&repackaged.stderr)
    );
    assert_eq!(bytes, fs::read(bundle)?);

    fs::remove_file(project.path().join("config.schema.json"))?;
    symlink("payload.pb", project.path().join("config.schema.json"))?;
    let failed = invoke()?;
    assert!(
        !failed.status.success(),
        "unexpected successful bundle: {}",
        String::from_utf8_lossy(&failed.stdout)
    );
    let error: Value = serde_json::from_slice(&failed.stderr)?;
    assert!(!failed.status.success());
    assert_eq!(error["error"]["code"], "package.invalid_file", "{error}");
    assert_eq!(bytes, fs::read(bundle)?);
    fs::remove_file(project.path().join("config.schema.json"))?;
    let failed = invoke()?;
    assert!(
        !failed.status.success(),
        "unexpected successful bundle: {}",
        String::from_utf8_lossy(&failed.stdout)
    );
    let error: Value = serde_json::from_slice(&failed.stderr)?;
    assert_eq!(error["error"]["code"], "package.read_failed", "{error}");
    assert_eq!(bytes, fs::read(bundle)?);
    fs::write(project.path().join("config.schema.json"), schema)?;

    fs::write(project.path().join("payload.pb"), b"invalid protobuf")?;
    let failed = invoke()?;
    assert!(!failed.status.success());
    let error: Value = serde_json::from_slice(&failed.stderr)?;
    assert!(
        error["error"]["code"]
            .as_str()
            .ok_or("code missing")?
            .starts_with("payload_contract."),
        "{error}"
    );
    assert_eq!(bytes, fs::read(bundle)?);

    fs::write(
        project.path().join("build.rs"),
        "fn main() { println!(\"cargo:rustc-link-lib=tenon_missing_library_fixture\"); }",
    )?;
    let failed = invoke()?;
    assert!(!failed.status.success());
    let error: Value = serde_json::from_slice(&failed.stderr)?;
    assert_eq!(error["error"]["code"], "cargo.failed", "{error}");
    assert!(
        error["error"]["message"]
            .as_str()
            .ok_or("message missing")?
            .contains("tenon_missing_library_fixture")
    );
    assert_eq!(bytes, fs::read(bundle)?);
    Ok(())
}
