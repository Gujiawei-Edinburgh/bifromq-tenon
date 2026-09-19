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
use std::io;
use std::time::Duration;
use tonic::transport::Endpoint;

#[tokio::test(flavor = "current_thread")]
async fn shutdown_before_the_first_poll_does_not_wait_for_a_remote_server() -> io::Result<()> {
    let directory = tempfile::tempdir_in("/tmp")?;
    let endpoint = Endpoint::from_shared(format!(
        "unix://{}",
        directory.path().join("absent.sock").display()
    ))
    .map_err(io::Error::other)?;
    let diagnostics = PipelineDiagnostics::start(endpoint.connect_lazy(), vec![0; 16]);
    tokio::time::timeout(Duration::from_secs(1), diagnostics.shutdown())
        .await
        .map_err(|_| io::Error::other("Diagnostic shutdown waited for an absent server"))?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_the_diagnostics_owner_cancels_its_transport_task() -> io::Result<()> {
    let directory = tempfile::tempdir_in("/tmp")?;
    let endpoint = Endpoint::from_shared(format!(
        "unix://{}",
        directory.path().join("absent.sock").display()
    ))
    .map_err(io::Error::other)?;
    let diagnostics = PipelineDiagnostics::start(endpoint.connect_lazy(), vec![0; 16]);
    let task = diagnostics
        .task
        .as_ref()
        .ok_or_else(|| io::Error::other("Diagnostic task is missing"))?
        .abort_handle();
    drop(diagnostics);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| io::Error::other("Diagnostic transport survived owner drop"))?;
    Ok(())
}
