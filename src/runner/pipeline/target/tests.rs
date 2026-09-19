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

use super::PipelineRunningState;
use crate::config::RunnerConfig;
use crate::contracts::core::{
    PipelineBootstrap, PipelineRevisionPlan, PluginInstanceState,
    PluginInterface as ProtocolPluginInterface,
};
use crate::pipeline::test_support::PipelineRevision;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::pipeline::test_support::{RevisionFixture, revision_status as snapshot};
use crate::runner::test_support::load_config;
use prost::Message as _;
use serde_json::{Value, json};
use std::fs;
use std::io;
use std::path::Path;

#[test]
fn bootstrap_transports_the_configured_deadline_through_protobuf() -> io::Result<()> {
    let fixture = RevisionFixture::new(&["com.example.first"])?;
    let target = fixture.target("com.example.first", "first")?;
    let directory = tempfile::tempdir()?;
    let config = load_config(directory.path())?;
    let working_directory = directory.path().join("pipeline");
    for timeout_ms in [30_000, 15_000, u64::MAX] {
        let config = if timeout_ms == 30_000 {
            &config
        } else {
            let path = directory.path().join("runner.jsonc");
            let mut authored: Value = serde_json::from_slice(&fs::read(&path)?)?;
            authored["pipeline"]["reconfigureTimeoutMs"] = Value::from(timeout_ms);
            fs::write(&path, serde_json::to_vec(&authored)?)?;
            &RunnerConfig::load(&path).map_err(io::Error::other)?
        };
        let bytes = target.bootstrap(config, &working_directory).encode_to_vec();
        let wire = PipelineBootstrap::decode(bytes.as_slice()).map_err(io::Error::other)?;
        let environment = wire
            .environment
            .ok_or_else(|| io::Error::other("Bootstrap environment is missing"))?;
        let received = PipelineRevision::from_runner(
            wire.revision_plan
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?,
        );
        assert_eq!(environment.reconfigure_timeout_ms, timeout_ms);
        let lua_limits = environment
            .lua_limits
            .ok_or_else(|| io::Error::other("Lua limits are missing"))?;
        assert_eq!(
            u128::from(lua_limits.cpu_time_limit_ms),
            config.lua_cpu_time_limit().as_millis()
        );
        assert_eq!(
            lua_limits.memory_limit_bytes,
            config.lua_memory_limit_bytes().get() as u64
        );
        assert_eq!(
            received.document_etag(),
            target.document_etag().strong_value()
        );
        assert_eq!(
            Path::new(&environment.pipeline_working_directory),
            working_directory
        );
    }
    Ok(())
}

#[test]
fn revision_preserves_authored_etag_config_and_deduplicated_program_material() -> io::Result<()> {
    let fixture = RevisionFixture::new(&["com.example.gateway"])?;
    let target = fixture.target("com.example.gateway", "authored comment")?;
    let wire = target.revision();
    assert_eq!(wire.document_etag, target.document_etag().strong_value());
    assert_ne!(
        TenonDocumentEtag::for_source(wire.tenon_document_json.as_bytes()),
        target.document_etag()
    );
    let document: Value = serde_json::from_str(&wire.tenon_document_json)?;
    assert_eq!(
        document["pluginInstances"]["left"]["config"],
        json!({"endpoint": "private left value"})
    );
    assert_eq!(
        document["pluginInstances"]["right"]["config"],
        json!({"endpoint": "private right value"})
    );
    assert!(
        document["flows"]["forward"]["source"]
            .get("delivery")
            .is_none()
    );
    assert_eq!(wire.plugin_programs.len(), 1);
    assert_eq!(wire.plugin_programs[0].program_name, "com.example.gateway");
    assert_eq!(
        wire.plugin_programs[0].plugin_interface,
        ProtocolPluginInterface::SourceAndSink as i32
    );
    let decoded =
        PipelineRevisionPlan::decode(wire.encode_to_vec().as_slice()).map_err(io::Error::other)?;
    assert_eq!(decoded, wire);
    let received = PipelineRevision::from_runner(decoded);
    assert_eq!(received.document_etag(), wire.document_etag);
    assert_eq!(serde_json::to_value(received.document())?, document);
    assert_eq!(received.document().plugin_instances().len(), 2);
    assert_eq!(received.document().flows().len(), 2);
    assert_eq!(received.programs().len(), 1);
    let material = received
        .programs()
        .values()
        .next()
        .ok_or_else(|| io::Error::other("Program material is missing"))?;
    assert_eq!(
        material.program_directory(),
        Path::new(&wire.plugin_programs[0].program_directory)
    );
    assert_eq!(material.command(), wire.plugin_programs[0].command);
    assert_eq!(
        material.payload_contract().descriptor_bytes(),
        wire.plugin_programs[0].payload_descriptor_set
    );
    assert_eq!(target.revision(), wire);
    let debug = format!("{target:?}");
    assert!(!debug.contains("private left value"));
    assert!(!debug.contains("private right value"));
    Ok(())
}

#[test]
fn applied_view_preserves_each_pipeline_instance_state() -> io::Result<()> {
    let fixture = RevisionFixture::new(&["com.example.gateway"])?;
    let target = fixture.target("com.example.gateway", "status states")?;
    let etag = target.document_etag().strong_value();
    for state in [
        PluginInstanceState::Starting,
        PluginInstanceState::Running,
        PluginInstanceState::StartFailed,
        PluginInstanceState::RestartBackoff,
    ] {
        let status = snapshot(&etag, state);
        let applied: PipelineRunningState = target.running_state(status);
        assert!(
            applied
                .snapshot()
                .plugin_instances
                .iter()
                .all(|instance| instance.state == state as i32)
        );
        assert_eq!(applied.snapshot().plugin_instances[0].id, "left");
    }
    Ok(())
}
