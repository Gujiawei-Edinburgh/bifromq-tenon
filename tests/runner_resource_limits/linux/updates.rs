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

fn shutdown_marker(state: &Path) -> io::Result<PathBuf> {
    let mut result = None;
    wait_until(|| {
        result = file_tree::named_files(&state.join("pipelines"), "shutdown.received")?
            .into_iter()
            .next();
        Ok(result.as_ref().map(|_| ()))
    })?;
    result.ok_or_else(|| io::Error::other("Shutdown marker is missing"))
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn replacement_uses_latest_ready_target_after_unready_interrupts_shutdown() -> io::Result<()> {
    let scope = Scope::new()?;
    let state = tempfile::tempdir()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut runner = scope.spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let mut desired = document(json!({"cpu":1}));
    desired["pluginInstances"]["gateway"]["config"]["behavior"] = json!("delay-shutdown");
    let first = put(address, &desired, None)?;
    wait_applied(address, &first)?;
    let old_group = scope.group()?;
    let old_pid = pipeline_pid(state.path())?;
    desired["resourceLimits"]["cpu"] = json!(2);
    let intermediate = put(address, &desired, Some(&first))?;
    let marker = shutdown_marker(state.path())?;
    assert_eq!(scope.group()?, old_group);
    assert_eq!(details(address)?["appliedDocumentEtag"], first);
    desired["pluginInstances"]["gateway"]["exactVersion"] = json!("2.0.0");
    let unready = put(address, &desired, Some(&intermediate))?;
    fs::write(marker.with_file_name("allow-shutdown"), "go")?;
    wait_until(|| {
        Ok(
            (!old_group.exists() && details(address)?.get("appliedDocumentEtag").is_none())
                .then_some(()),
        )
    })?;
    assert!(!PathBuf::from(format!("/proc/{old_pid}")).exists());
    assert_eq!(details(address)?["state"], "unready");
    assert!(file_tree::named_files(&state.path().join("pipelines"), "parent.pid")?.is_empty());
    desired["pluginInstances"]["gateway"]["exactVersion"] = json!("1.0.0");
    desired["pluginInstances"]["gateway"]["config"] = json!({});
    desired["resourceLimits"]["cpu"] = json!(0.75);
    let ready = put(address, &desired, Some(&unready))?;
    wait_applied(address, &ready)?;
    assert_eq!(
        fs::read_to_string(scope.group()?.join("cpu.max"))?.trim(),
        "75000 100000"
    );
    runner.terminate()?;
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn deletion_during_resource_replacement_never_starts_the_intermediate_target() -> io::Result<()> {
    let scope = Scope::new()?;
    let state = tempfile::tempdir()?;
    let launches = tempfile::NamedTempFile::new()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut runner = scope.spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let mut desired = document(json!({"cpu":1}));
    desired["pluginInstances"]["gateway"]["config"]["behavior"] = json!("delay-shutdown");
    desired["pluginInstances"]["gateway"]["env"] =
        json!({"TENON_TEST_PLUGIN_STARTS":launches.path()});
    desired["pluginInstances"]["gateway"]["config"]["endpoint"] = json!("first");
    let first = put(address, &desired, None)?;
    wait_applied(address, &first)?;
    let old_group = scope.group()?;
    desired["resourceLimits"]["cpu"] = json!(2);
    desired["pluginInstances"]["gateway"]["config"]["endpoint"] = json!("intermediate");
    let intermediate = put(address, &desired, Some(&first))?;
    let marker = shutdown_marker(state.path())?;
    assert_eq!(
        request(
            address,
            "DELETE",
            "/documents/limited",
            &[("If-Match", &intermediate)],
            &[]
        )?
        .status,
        204
    );
    fs::write(marker.with_file_name("allow-shutdown"), "go")?;
    wait_until(|| Ok((!old_group.exists()).then_some(())))?;
    wait_until(|| {
        Ok(
            file_tree::named_files(&state.path().join("pipelines"), "parent.pid")?
                .is_empty()
                .then_some(()),
        )
    })?;
    assert!(scope.groups()?.is_empty());
    assert_eq!(
        request(address, "GET", "/pipelines/limited", &[], &[])?.status,
        404
    );
    // Re-creation must wait for the retired lifecycle. A durable Plugin startup
    // record remains observable after that lifecycle's runtime files disappear.
    desired["resourceLimits"]["cpu"] = json!(0.75);
    desired["pluginInstances"]["gateway"]["config"] = json!({"endpoint":"recreated"});
    let recreated = put(address, &desired, None)?;
    wait_applied(address, &recreated)?;
    let starts = fs::read_to_string(launches.path())?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        starts
            .iter()
            .map(|config| config["endpoint"].as_str())
            .collect::<Vec<_>>(),
        [Some("first"), Some("recreated")]
    );
    runner.terminate()?;
    assert!(scope.groups()?.is_empty());
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn shutdown_deadline_kills_old_resource_group_before_replacement() -> io::Result<()> {
    let scope = Scope::new()?;
    let state = tempfile::tempdir()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut runner = scope.spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let mut desired = document(json!({"cpu":1}));
    desired["pluginInstances"]["gateway"]["config"]["behavior"] = json!("delay-shutdown");
    let first = put(address, &desired, None)?;
    wait_applied(address, &first)?;
    let old_group = scope.group()?;
    let old_pid = pipeline_pid(state.path())?;
    desired["pluginInstances"]["gateway"]["config"] = json!({});
    desired["resourceLimits"] = json!({});
    let replacement = put(address, &desired, Some(&first))?;
    let marker = shutdown_marker(state.path())?;
    // The real Plugin withholds shutdown completion until Runner's configured deadline.
    assert!(marker.exists());
    assert!(old_group.exists());
    wait_applied(address, &replacement)?;
    assert!(!old_group.exists());
    assert!(!PathBuf::from(format!("/proc/{old_pid}")).exists());
    assert!(details(address)?.get("resourceLimits").is_none());
    runner.terminate()?;
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn unrepresentable_kernel_limits_never_execute_and_leave_neighbor_running() -> io::Result<()> {
    let scope = Scope::new()?;
    let state = tempfile::tempdir()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut runner = scope.spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let mut neighbor = document(json!({}));
    neighbor["id"] = json!("neighbor");
    let neighbor_etag = put(address, &neighbor, None)?;
    wait_until(|| {
        Ok(
            (request(address, "GET", "/pipelines/neighbor", &[], &[])?.json()["pluginInstances"]
                [0]["state"]
                == "running")
                .then_some(()),
        )
    })?;
    let neighbor_pid = pipeline_pid(state.path())?;
    let mut previous = None;
    for limits in [
        // The kernel may reject this or overflow its nanosecond conversion on write.
        json!({"cpu":184467440737.11}),
        json!({"memoryBytes":u64::MAX}),
        // Page-aligned u64 values must not be interpreted by Linux as unlimited.
        json!({"memoryBytes":18446744073709486080_u64}),
    ] {
        let etag = put(address, &document(limits), previous.as_deref())?;
        wait_until(|| {
            let failed = details(address)?;
            Ok((failed["lastError"]["documentEtag"] == etag
                && failed["lastError"]["code"] == "resource_limits_apply_failed")
                .then_some(()))
        })?;
        assert!(details(address)?.get("appliedDocumentEtag").is_none());
        assert!(scope.groups()?.is_empty());
        assert_eq!(
            file_tree::named_files(&state.path().join("pipelines"), "parent.pid")?.len(),
            1
        );
        assert_eq!(pipeline_pid(state.path())?, neighbor_pid);
        let observed = request(address, "GET", "/pipelines/neighbor", &[], &[])?.json();
        assert_eq!(observed["appliedDocumentEtag"], neighbor_etag);
        assert_eq!(observed["pluginInstances"][0]["state"], "running");
        previous = Some(etag);
    }
    runner.terminate()?;
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn cpu_quotas_keep_oversubscribed_channels_on_the_inherited_cpu_set() -> io::Result<()> {
    let state = tempfile::tempdir()?;
    let markers = tempfile::tempdir()?;
    let scope = Scope::new()?;
    // An ancestor quota must not reduce the CPU-count snapshot either.
    fs::write(scope.path.join("cpu.max"), "50000 100000")?;
    install_load(state.path(), "cpu", markers.path())?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let inherited = rustix::thread::sched_getaffinity(None)?;
    let mut allowed = rustix::thread::CpuSet::new();
    for cpu in (0..rustix::thread::CpuSet::MAX_CPU)
        .filter(|cpu| inherited.is_set(*cpu))
        .take(4)
    {
        allowed.set(cpu);
    }
    assert!(
        allowed.count() >= 2,
        "This scenario requires at least two allowed CPUs"
    );
    let channels = 2 * allowed.count() as usize;
    // Constrain only the fixture's launch thread; Runner and all descendants
    // inherit this mask. The test driver keeps its original CPU affinity.
    let (scope, mut runner) = std::thread::spawn(move || -> io::Result<_> {
        rustix::thread::sched_setaffinity(None, &allowed)?;
        let runner = scope.spawn(&config)?;
        Ok((scope, runner))
    })
    .join()
    .map_err(|_| io::Error::other("Runner launch thread panicked"))??;
    wait_for_http(&mut runner, address)?;
    let mut etag = None;
    let mut previous_pid = None;
    let mut previous_group: Option<PathBuf> = None;
    let mut previous_plugin_pid = None;
    for (cpu, quota) in [(0.5, "50000 100000"), (1.5, "150000 100000")] {
        let mut desired = document(json!({"cpu": cpu}));
        desired["flows"]["loop"]["parallelism"] = json!(2);
        let applied = put(address, &desired, etag.as_deref())?;
        wait_applied(address, &applied)?;
        let pid = pipeline_pid(state.path())?;
        assert_ne!(Some(pid), previous_pid);
        if let Some(old) = previous_group {
            assert!(
                !old.exists(),
                "Old resource group must be removed before replacement"
            );
        }
        let group = scope.group()?;
        let plugin_path = file_tree::named_files(&state.path().join("pipelines"), "process.pid")?
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("Plugin PID is missing"))?;
        let plugin_pid = fs::read_to_string(plugin_path)?;
        assert_ne!(Some(&plugin_pid), previous_plugin_pid.as_ref());
        let pipeline_mask = rustix::thread::sched_getaffinity(rustix::process::Pid::from_raw(
            i32::try_from(pid).map_err(io::Error::other)?,
        ))?;
        assert_eq!(pipeline_mask, allowed);
        assert_eq!(
            fs::read_to_string(scope.group()?.join("cpu.max"))?.trim(),
            quota
        );
        let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
        let allowed_list = status
            .lines()
            .find(|line| line.starts_with("Cpus_allowed_list:"))
            .ok_or_else(|| io::Error::other("Pipeline CPU mask is missing"))?;
        let mut workers = 0;
        for thread in fs::read_dir(format!("/proc/{pid}/task"))? {
            let thread = thread?.path();
            if !fs::read_to_string(thread.join("comm"))?.starts_with("tenon-flow-") {
                continue;
            }
            let status = fs::read_to_string(thread.join("status"))?;
            assert_eq!(
                status
                    .lines()
                    .find(|line| line.starts_with("Cpus_allowed_list:")),
                Some(allowed_list)
            );
            workers += 1;
        }
        assert_eq!(workers, channels);
        let before = usage(&scope.path.join("cpu.stat"), "usage_usec")?;
        let start = Instant::now();
        std::thread::sleep(Duration::from_secs(2));
        let cores = (usage(&scope.path.join("cpu.stat"), "usage_usec")? - before) as f64
            / start.elapsed().as_micros() as f64;
        eprintln!(
            "{} allowed CPUs, {channels} channels, document quota {cpu}, aggregate CPU use {cores:.3}",
            allowed.count()
        );
        assert!(
            cores > 0.2 && cores < 0.7,
            "Ancestor CPU ceiling measured {cores} cores"
        );
        assert!(usage(&scope.path.join("cpu.stat"), "nr_throttled")? > 0);
        for channel in 0..workers {
            assert_eq!(
                file_tree::named_files(
                    &state.path().join("pipelines"),
                    &format!("submission-{channel}.queue")
                )?
                .len(),
                1
            );
        }
        assert!(
            file_tree::named_files(
                &state.path().join("pipelines"),
                &format!("submission-{workers}.queue")
            )?
            .is_empty()
        );
        previous_pid = Some(pid);
        previous_plugin_pid = Some(plugin_pid);
        previous_group = Some(group);
        etag = Some(applied);
    }
    runner.terminate()?;
    assert!(scope.groups()?.is_empty());
    Ok(())
}
