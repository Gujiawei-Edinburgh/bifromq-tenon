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

//! File-backed Queue pages are charged to the Document that first writes them.
use super::*;

#[test]
#[ignore = "Requires the delegated Linux resource test harness on a disk-backed temporary directory"]
fn queue_pages_obey_each_document_memory_limit_without_restarting_its_neighbor() -> io::Result<()> {
    let state = tempfile::tempdir()?;
    let scope = Scope::new()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut configuration: Value = serde_json::from_slice(&fs::read(&config)?)?;
    configuration["pipeline"]["retryBackoff"] =
        json!({"initialDelayMs":5000,"maximumDelayMs":5000});
    // Page faults under memory pressure also spend Channel CPU time. Keep
    // this scenario from testing the independent 50 ms Lua budget instead.
    configuration["lua"]["cpuTimeLimitMs"] = json!(5000);
    fs::write(&config, serde_json::to_vec(&configuration)?)?;
    let mut runner = scope.spawn(&config)?;
    wait_for_http(&mut runner, address)?;

    // A valid unknown Protobuf field keeps the business contract empty. Each
    // Plugin reuses one small payload while touching 256 MiB of its real Queue.
    let mut payload = vec![10, 128, 128, 2];
    payload.resize(32 * 1024 + 4, 42);
    let mut desired = document(json!({}));
    desired["flows"]["loop"]["maxPendingRecords"] = json!(8192);
    desired["flows"]["loop"]["maxRecordBytes"] = json!(65536);
    desired["flows"]["loop"]["process"]["script"] = json!(
        "local b = registry:getBuilder('com.example.gateway@1.0.0'); function main(event) emit(b:build()) end"
    );
    desired["pluginInstances"]["gateway"]["config"] = json!({"traffic": {
        "session":"queue-memory", "benchmark": {
            "warmupRecords":1024, "records":8192, "maxInFlight":8,
            "payload":payload, "startGate":"start-queue-load"
        }
    }});

    let mut workloads = Vec::new();
    for (id, limit) in [
        ("limited", 96 * 1024 * 1024_u64),
        ("neighbor", 512 * 1024 * 1024),
    ] {
        desired["id"] = json!(id);
        desired["resourceLimits"]["memoryBytes"] = json!(limit);
        desired["pluginInstances"]["gateway"]["config"]["traffic"]["session"] = json!(id);
        let etag = put(address, &desired, None)?;
        wait_until(|| {
            let current = status(address, id)?;
            Ok((current["appliedDocumentEtag"] == etag
                && current["pluginInstances"][0]["state"] == "running")
                .then_some(()))
        })?;
        let marker =
            file_tree::named_files(state.path(), &format!("traffic-{id}-submitted.received"))?
                .into_iter()
                .next()
                .ok_or_else(|| io::Error::other("Plugin traffic marker is missing"))?;
        let root = marker
            .parent()
            .ok_or_else(|| io::Error::other("Plugin directory is missing"))?
            .to_owned();
        wait_until(|| {
            Ok(root
                .join("benchmark-ready.received")
                .try_exists()?
                .then_some(()))
        })?;
        let pid = fs::read_to_string(root.join("parent.pid"))?
            .trim()
            .parse::<u32>()
            .map_err(io::Error::other)?;
        let group = Path::new("/sys/fs/cgroup").join(membership(pid)?.trim_start_matches('/'));
        assert!(scope.groups()?.contains(&group));
        assert_eq!(
            fs::read_to_string(group.join("memory.max"))?.trim(),
            limit.to_string()
        );
        let plugin_pid = fs::read_to_string(root.join("process.pid"))?
            .trim()
            .parse::<u32>()
            .map_err(io::Error::other)?;
        assert_eq!(membership(pid)?, membership(plugin_pid)?);
        assert_eq!(status(address, id)?["resourceLimits"]["state"], "enforced");
        // A 512 MiB Queue fits initially. The completed 32 MiB warm-up proves
        // actual Queue page charges before allowing memory pressure to start.
        assert!(fs::metadata(root.join("source/submission-0.queue"))?.len() > 512 * 1024 * 1024);
        assert!(!root.join("benchmark-started.received").exists());
        assert_eq!(usage(&group.join("memory.events"), "max")?, 0);
        workloads.push(Workload {
            id,
            limit,
            root,
            group,
            pid,
            plugin_pid,
            etag,
        });
    }

    for Workload {
        id,
        limit,
        root,
        group,
        pid,
        plugin_pid,
        etag,
    } in &workloads
    {
        let before_file = usage(&group.join("memory.stat"), "file")?;
        let before_anon = usage(&group.join("memory.stat"), "anon")?;
        assert!(
            before_file > 16 * 1024 * 1024,
            "Warm-up Queue pages must be charged inside the Document"
        );
        // Pause only the supervisor so an OOM cannot delete the original
        // cgroup counters before we inspect them. The data plane stays live.
        let supervisor = scope.path.join("tenon.runner");
        fs::write(supervisor.join("cgroup.freeze"), "1")?;
        wait_until(
            || Ok((usage(&supervisor.join("cgroup.events"), "frozen")? == 1).then_some(())),
        )?;
        let events = group.join("memory.events.local");
        fs::write(root.join("start-queue-load"), [])?;
        let result_path = root.join("benchmark-result.json");
        wait_until(|| {
            Ok((result_path.try_exists()? || usage(&events, "oom_kill")? > 0).then_some(()))
        })?;
        let max_events = usage(&events, "max")?;
        let oom_events = usage(&events, "oom")?;
        let oom_kills = usage(&events, "oom_kill")?;
        fs::write(supervisor.join("cgroup.freeze"), "0")?;
        if oom_kills > 0 {
            assert_eq!(
                *id, "limited",
                "The neighbor's budget must fit the same traffic"
            );
            assert!(
                max_events > 0 && oom_events > 0,
                "The original Document cgroup must confirm its own memory.max OOM"
            );
            println!(
                "Queue memory OOM evidence: {}",
                json!({
                    "document":id, "memoryMax":limit, "warmupFileBytes":before_file,
                    "warmupAnonBytes":before_anon, "maxEvents":max_events, "oomEvents":oom_events, "oomKills":oom_kills
                })
            );
            wait_until(|| Ok((!group.exists()).then_some(())))?;
            wait_applied(address, etag)?;
            assert_ne!(
                fs::read_to_string(root.join("parent.pid"))?.trim(),
                pid.to_string()
            );
            continue;
        }
        let result: Value = serde_json::from_slice(&fs::read(result_path)?)?;
        assert_eq!(result["records"], 8192);
        assert_eq!(result["egressRecords"], 9216);
        let file = usage(&group.join("memory.stat"), "file")?;
        let anon = usage(&group.join("memory.stat"), "anon")?;
        let current = fs::read_to_string(group.join("memory.current"))?
            .trim()
            .parse::<u64>()
            .map_err(io::Error::other)?;
        println!(
            "Queue memory evidence: {}",
            json!({
                "document":id, "memoryMax":limit, "memoryCurrent":current,
                "fileBefore":before_file, "fileAfter":file, "anonBefore":before_anon,
                "anonAfter":anon, "maxEvents":max_events, "records":result["records"]
            })
        );
        assert!(
            file > before_file + 16 * 1024 * 1024,
            "Queue pages must be charged inside the Document"
        );
        assert!(
            anon < before_anon + 32 * 1024 * 1024,
            "The load must not be a large private allocation"
        );
        assert_eq!(usage(&group.join("memory.events"), "oom_kill")?, 0);
        if *id == "limited" {
            assert!(
                max_events > 0,
                "The kernel must actually enforce memory.max"
            );
        } else {
            assert_eq!(
                max_events, 0,
                "The same traffic fits the neighbor's own budget"
            );
            assert!(
                file > before_file + 128 * 1024 * 1024,
                "The neighbor must retain more Queue pages than the smaller budget permits"
            );
        }
        assert_eq!(status(address, id)?["appliedDocumentEtag"], *etag);
        assert_eq!(
            fs::read_to_string(root.join("parent.pid"))?.trim(),
            pid.to_string()
        );
        if fs::read_to_string(root.join("process.pid"))?.trim() != plugin_pid.to_string() {
            let metrics = request(address, "GET", "/metrics?format=prometheus", &[], &[])?;
            return Err(io::Error::other(format!(
                "Unexpected Plugin restart; Flow errors: {}",
                metrics
                    .body_text()
                    .lines()
                    .filter(|line| line.contains("error"))
                    .collect::<Vec<_>>()
                    .join("\n")
            )));
        }
        assert_eq!(membership(*pid)?, membership(*plugin_pid)?);
    }
    runner.terminate()?;
    assert!(scope.groups()?.is_empty());
    assert!(file_tree::named_files(&state.path().join("pipelines"), "parent.pid")?.is_empty());
    Ok(())
}

struct Workload {
    id: &'static str,
    limit: u64,
    root: PathBuf,
    group: PathBuf,
    pid: u32,
    plugin_pid: u32,
    etag: String,
}

fn status(address: SocketAddr, id: &str) -> io::Result<Value> {
    let response = request(address, "GET", &format!("/pipelines/{id}"), &[], &[])?;
    assert_eq!(response.status, 200, "{}", response.body_text());
    Ok(response.json())
}
