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

use super::*;

/// Export the existing real Plugin peer for the external container test driver.
#[test]
#[ignore = "Invoked only by verify-container-resource-limits.py"]
fn export_deployment_fixture() -> io::Result<()> {
    let directory = PathBuf::from(
        std::env::var_os("TENON_TEST_DEPLOYMENT_DIRECTORY")
            .ok_or_else(|| io::Error::other("Missing deployment fixture directory"))?,
    );
    fs::write(
        directory.join("plugin.tar.gz"),
        plugin_fixture::plugin_package(PluginInterface::SourceAndSink)?,
    )?;
    fs::write(
        directory.join("document.json"),
        serde_json::to_vec(&document(json!({"cpu":0.5,"memoryBytes":536870912})))?,
    )?;
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn bootstrap_claims_scope_and_rejects_second_runner_before_recovery() -> io::Result<()> {
    let scope = Scope::new()?;
    assert!(!scope.path.join("tenon.runner").exists());
    let first_state = tempfile::tempdir()?;
    install_program(first_state.path(), PluginInterface::SourceAndSink)?;
    let first_address = available_address()?;
    let first_config = write_config(first_state.path(), first_address)?;
    let mut first = scope.spawn(&first_config)?;
    wait_for_http(&mut first, first_address)?;
    let etag = put(first_address, &document(json!({"cpu":0.5})), None)?;
    wait_applied(first_address, &etag)?;
    let pipeline = pipeline_pid(first_state.path())?;
    let group = scope.group()?;
    assert!(
        fs::read_to_string(scope.path.join("cgroup.procs"))?
            .trim()
            .is_empty()
    );
    assert!(
        !fs::read_to_string(scope.path.join("tenon.runner/cgroup.procs"))?
            .trim()
            .is_empty()
    );

    let second_state = tempfile::tempdir()?;
    let second_address = available_address()?;
    let second_config = write_config(second_state.path(), second_address)?;
    let mut second = scope.spawn(&second_config)?;
    second.wait_for_failure("Cannot exclusively claim cgroup scope")?;
    assert!(group.exists());
    assert_eq!(pipeline_pid(first_state.path())?, pipeline);
    assert_eq!(details(first_address)?["appliedDocumentEtag"], etag);
    assert!(
        !second_state.path().join("pipelines").exists(),
        "A duplicate must fail before state recovery"
    );

    first.terminate()?;
    let mut successor = scope.spawn(&second_config)?;
    wait_for_http(&mut successor, second_address)?;
    successor.terminate()?;
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn foreign_subtree_is_not_adopted_or_removed() -> io::Result<()> {
    let scope = Scope::new()?;
    let neighbor = scope.path.join("another.service");
    fs::create_dir(&neighbor)?;
    let state = tempfile::tempdir()?;
    let config = write_config(state.path(), available_address()?)?;
    let mut runner = scope.spawn(&config)?;
    runner.wait_for_failure("another manager's subtree")?;
    assert!(neighbor.exists());
    assert!(!scope.path.join("tenon.runner").exists());
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn an_ancestor_delegation_is_not_a_runner_delegation() -> io::Result<()> {
    let scope = Scope::new()?;
    let session = scope.path.join("unmarked-session");
    fs::create_dir(&session)?;
    let state = tempfile::tempdir()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let executable = std::env::var_os("TENON_TEST_RUNNER_BINARY")
        .map_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_tenon")), PathBuf::from);
    let mut command = Command::new("/bin/sh");
    command
        .args([
            "-ec",
            "printf 0 > \"$1/cgroup.procs\"; shift; exec \"$@\"",
            "unmarked-session",
        ])
        .arg(&session)
        .arg(executable)
        .arg("--config")
        .arg(&config);
    let mut runner = TestRunner::spawn_command(command)?;
    wait_for_http(&mut runner, address)?;
    let first = put(address, &document(json!({"cpu":1})), None)?;
    wait_until(|| {
        Ok(
            (details(address)?["lastError"]["code"] == "resource_limits_apply_failed")
                .then_some(()),
        )
    })?;
    assert!(!scope.path.join("tenon.runner").exists());
    assert!(!session.join("tenon.runner").exists());
    let unlimited = put(address, &document(json!({})), Some(&first))?;
    wait_applied(address, &unlimited)?;
    runner.terminate()?;
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn denied_subgroup_creation_keeps_unlimited_documents_working() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let scope = Scope::new()?;
    let state = tempfile::tempdir()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let executable = std::env::var_os("TENON_TEST_RUNNER_BINARY")
        .map_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_tenon")), PathBuf::from);
    let mut command = Command::new("/bin/sh");
    command
        .args([
            "-ec",
            "printf 0 > \"$1/cgroup.procs\"; shift; exec \"$@\"",
            "permission-test",
        ])
        .arg(&scope.path);
    if rustix::process::geteuid().is_root() {
        // A root test harness must drop privileges after entering the scope.
        // setpriv execs directly, leaving no extra process in the claimed group.
        fn give_to_test_user(path: &Path) -> io::Result<()> {
            std::os::unix::fs::chown(path, Some(65534), Some(65534))?;
            if path.is_dir() {
                for entry in fs::read_dir(path)? {
                    give_to_test_user(&entry?.path())?;
                }
            }
            Ok(())
        }
        give_to_test_user(state.path())?;
        for path in [
            &scope.path,
            &scope.path.join("cgroup.procs"),
            &scope.path.join("cgroup.subtree_control"),
        ] {
            std::os::unix::fs::chown(path, Some(65534), Some(65534))?;
        }
        command.args([
            "setpriv",
            "--reuid=65534",
            "--regid=65534",
            "--clear-groups",
        ]);
    }
    fs::set_permissions(&scope.path, fs::Permissions::from_mode(0o500))?;
    command.arg(executable).arg("--config").arg(config);
    let mut runner = TestRunner::spawn_command(command)?;
    wait_for_http(&mut runner, address)?;
    assert!(!scope.path.join("tenon.runner").exists());
    let limited = put(address, &document(json!({"cpu":0.5})), None)?;
    wait_until(|| {
        Ok(
            (details(address)?["lastError"]["code"] == "resource_limits_apply_failed")
                .then_some(()),
        )
    })?;
    assert!(details(address)?.get("appliedDocumentEtag").is_none());
    let unlimited = put(address, &document(json!({})), Some(&limited))?;
    wait_applied(address, &unlimited)?;
    runner.terminate()?;
    assert!(
        runner
            .read_stderr()?
            .contains("Cannot create the Runner cgroup")
    );
    fs::set_permissions(&scope.path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}
