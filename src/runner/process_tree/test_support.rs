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

//! One reaping boundary for isolated Controller tests, including timeout failures.

use super::PipelineProcessTree;
use crate::pipeline::test_support::{TEST_DEADLINE, wait_for_file};
use std::io;
use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;
use tokio::process::Command;

pub(crate) async fn run_pipeline_child_until_exit(
    mut command: Command,
    entered: &Path,
) -> io::Result<ExitStatus> {
    command.process_group(0).kill_on_drop(true);
    let mut tree = PipelineProcessTree::from_spawned_child(command.spawn()?)?;
    let result = tokio::time::timeout(TEST_DEADLINE, async {
        wait_for_file(entered).await.map_err(|error| {
            io::Error::other(format!(
                "Pipeline did not enter {}: {error}",
                entered.display()
            ))
        })?;
        tokio::time::timeout(Duration::from_secs(2), tree.wait())
            .await
            .map_err(|error| io::Error::other(format!("Entered Pipeline did not exit: {error}")))?
    })
    .await
    .map_err(|error| io::Error::other(format!("Pipeline boundary test timed out: {error}")))
    .and_then(std::convert::identity);
    if let Err(error) = result {
        tree.force_kill_and_reap().await.map_err(|cleanup| {
            io::Error::other(format!("{error}; Pipeline cleanup also failed: {cleanup}"))
        })?;
        return Err(error);
    }
    result
}
