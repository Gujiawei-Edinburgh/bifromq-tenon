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

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tokio::time::{sleep, timeout};

pub(in crate::pipeline) use super::control::TestPluginControlServer as PluginControlServer;
use super::instances::PluginInstances;
use super::lifecycle::PluginBells;
use super::{PluginLaunch, PluginRetryBackoff};
use crate::contracts::core::PluginInstanceState;
use std::collections::BTreeMap;

mod controlled_child;

pub(super) use controlled_child::ControlledTestLaunch;
pub(crate) use controlled_child::controlled_program_command;
pub(in crate::pipeline) use controlled_child::{assert_reaped, lifecycle_events, recorded_pid};

pub(crate) const TEST_DEADLINE: Duration = Duration::from_secs(5);

#[cfg(not(feature = "loom-model"))]
pub(in crate::pipeline) fn control_socket_path(server: &PluginControlServer) -> PathBuf {
    server.launcher().socket_path().to_owned()
}

pub(super) struct TestLaunch {
    program_directory: PathBuf,
    command: Vec<String>,
    working_directory: PathBuf,
    config: Value,
}

impl TestLaunch {
    pub(super) fn new(root: &Path, name: &str, script: &str, config: Value) -> io::Result<Self> {
        let working_directory = root.join(name);
        std::fs::create_dir(&working_directory)?;
        Ok(Self {
            program_directory: root.to_owned(),
            command: vec![
                String::from("/bin/sh"),
                String::from("-c"),
                script.to_owned(),
                String::from("tenon-test-plugin"),
                // $1 is the Instance directory so a scripted child can write
                // beside the Plugin without reading the startup document.
                working_directory.display().to_string(),
            ],
            working_directory,
            config,
        })
    }

    pub(super) fn borrowed(&self) -> PluginLaunch<'_> {
        PluginLaunch {
            program_directory: &self.program_directory,
            command: &self.command,
            working_directory: self.working_directory.clone(),
            config: &self.config,
            extra_args: None,
            env: None,
            bells: PluginBells {
                source_channel_region: None,
                sink_inputs: None,
            },
        }
    }

    pub(super) fn working_directory(&self) -> &Path {
        &self.working_directory
    }

    pub(super) fn config(&self) -> &Value {
        &self.config
    }
}

pub(in crate::pipeline) fn retry_backoff() -> PluginRetryBackoff {
    PluginRetryBackoff::new(Duration::from_millis(10), Duration::from_millis(40))
}

pub(crate) async fn wait_for_file(path: &Path) -> io::Result<()> {
    timeout(TEST_DEADLINE, async {
        loop {
            if path.exists() {
                return Ok(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(io::Error::other)?
}

#[expect(
    clippy::expect_used,
    reason = "the sole status producer exhaustively maps the internal lifecycle enum"
)]
pub(in crate::pipeline) fn instance_statuses(
    instances: &PluginInstances,
) -> BTreeMap<String, PluginInstanceState> {
    instances
        .statuses()
        .into_iter()
        .map(|status| {
            let state = PluginInstanceState::try_from(status.state)
                .expect("PluginInstances produced an unknown lifecycle state");
            (status.id, state)
        })
        .collect()
}
