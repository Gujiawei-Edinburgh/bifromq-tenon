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

use std::error::Error;
use std::io;

use rustix::process::{WaitOptions, waitpid};
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

use super::PluginInstances;
use crate::identifiers::PluginInstanceId;
use crate::payload_contract::PluginInterface;
use crate::pipeline::diagnostics::test_support as diagnostic_test_support;
use crate::pipeline::plugin::PluginInstanceEvent;
use crate::pipeline::plugin::control::TestPluginControlServer as PluginControlServer;
use crate::pipeline::plugin::lifecycle::PluginStatusState;
use crate::pipeline::plugin::test_support::{
    ControlledTestLaunch, TEST_DEADLINE, assert_reaped, lifecycle_events, recorded_pid,
    retry_backoff, wait_for_file,
};

#[tokio::test(flavor = "current_thread")]
async fn all_interfaces_quiesce_before_any_instance_shutdown() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let source = ControlledTestLaunch::new(
        root.path(),
        "source",
        PluginInterface::Source,
        json!({"behavior": "delay-quiesce"}),
    )?;
    let sink = ControlledTestLaunch::new(
        root.path(),
        "sink",
        PluginInterface::Sink,
        json!({"behavior": "delay-shutdown"}),
    )?;
    let source_and_sink = ControlledTestLaunch::new(
        root.path(),
        "source-and-sink",
        PluginInterface::SourceAndSink,
        json!({"behavior": "normal"}),
    )?;
    let source_id = PluginInstanceId::try_from("source".to_owned())?;
    let sink_id = PluginInstanceId::try_from("sink".to_owned())?;
    let source_and_sink_id = PluginInstanceId::try_from("source-and-sink".to_owned())?;
    let diagnostics = diagnostic_test_support::publisher();
    let mut instances = PluginInstances::launch_controlled(
        [
            (
                source_id.clone(),
                source.borrowed(&launcher),
                diagnostics.instance_plugin(source_id),
            ),
            (
                sink_id.clone(),
                sink.borrowed(&launcher),
                diagnostics.instance_plugin(sink_id),
            ),
            (
                source_and_sink_id.clone(),
                source_and_sink.borrowed(&launcher),
                diagnostics.instance_plugin(source_and_sink_id),
            ),
        ],
        retry_backoff(),
    );

    timeout(TEST_DEADLINE, async {
        while instances
            .owners
            .iter()
            .any(|(_, owner)| owner.status().0 != PluginStatusState::Running)
        {
            assert_eq!(
                instances.next_event().await?,
                PluginInstanceEvent::StatusChanged
            );
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .await??;

    let process_ids = [
        recorded_pid(source.working_directory())?,
        recorded_pid(sink.working_directory())?,
        recorded_pid(source_and_sink.working_directory())?,
    ];
    let source_quiesce = source.working_directory().join("quiesce-source.received");
    let source_and_sink_quiesce = source_and_sink
        .working_directory()
        .join("quiesce-source.received");
    let mut quiesce = Box::pin(instances.quiesce_sources());
    timeout(TEST_DEADLINE, async {
        tokio::select! {
            result = &mut quiesce => {
                result?;
                Err(io::Error::other(
                    "Source quiesce completed before the delayed Source acknowledged",
                ).into())
            }
            result = async {
                tokio::try_join!(
                    wait_for_file(&source_quiesce),
                    wait_for_file(&source_and_sink_quiesce),
                )?;
                Ok::<_, io::Error>(())
            } => {
                result?;
                Ok::<_, Box<dyn Error>>(() )
            }
        }
    })
    .await??;

    assert_eq!(
        lifecycle_events(sink.working_directory())?,
        "attach\nready\n"
    );
    for launch in [&source, &sink, &source_and_sink] {
        assert!(!lifecycle_events(launch.working_directory())?.contains("shutdown\n"));
    }
    for process_id in process_ids {
        assert!(waitpid(Some(process_id), WaitOptions::NOHANG)?.is_none());
    }

    std::fs::write(source.working_directory().join("allow-quiesce"), [])?;
    timeout(TEST_DEADLINE, quiesce).await??;
    assert_eq!(
        lifecycle_events(source.working_directory())?,
        "attach\nready\nquiesce-source\nsource-quiesced\n"
    );
    assert_eq!(
        lifecycle_events(source_and_sink.working_directory())?,
        "attach\nready\nquiesce-source\nsource-quiesced\n"
    );
    for process_id in process_ids {
        assert!(waitpid(Some(process_id), WaitOptions::NOHANG)?.is_none());
    }

    let source_shutdown = source.working_directory().join("shutdown.received");
    let sink_shutdown = sink.working_directory().join("shutdown.received");
    let source_and_sink_shutdown = source_and_sink
        .working_directory()
        .join("shutdown.received");
    let mut shutdown = Box::pin(instances.shutdown_all());
    timeout(TEST_DEADLINE, async {
        tokio::select! {
            result = &mut shutdown => {
                result?;
                Err(io::Error::other(
                    "Plugin shutdown completed before the delayed Sink could exit",
                ).into())
            }
            result = async {
                tokio::try_join!(
                    wait_for_file(&source_shutdown),
                    wait_for_file(&sink_shutdown),
                    wait_for_file(&source_and_sink_shutdown),
                )?;
                Ok::<_, io::Error>(())
            } => {
                result?;
                Ok::<_, Box<dyn Error>>(())
            }
        }
    })
    .await??;
    std::fs::write(sink.working_directory().join("allow-shutdown"), [])?;
    timeout(TEST_DEADLINE, shutdown).await??;
    assert_eq!(
        lifecycle_events(source.working_directory())?,
        "attach\nready\nquiesce-source\nsource-quiesced\nshutdown\nexit\n"
    );
    assert_eq!(
        lifecycle_events(sink.working_directory())?,
        "attach\nready\nshutdown\nexit\n"
    );
    assert_eq!(
        lifecycle_events(source_and_sink.working_directory())?,
        "attach\nready\nquiesce-source\nsource-quiesced\nshutdown\nexit\n"
    );
    for process_id in process_ids {
        assert_reaped(process_id)?;
    }
    server.shutdown().await?;
    Ok(())
}
