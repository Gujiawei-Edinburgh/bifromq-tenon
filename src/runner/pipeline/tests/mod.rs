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

use super::diagnostics::RunnerPipelineDiagnostics;
use super::launch_registry::{
    PendingPipelineLaunch, RunnerPipelineLaunchRegistry,
    test_support as launch_registry_test_support,
};
use super::launcher::{
    RunnerPipelineLaunchError, RunnerPipelineLauncher, RunnerPipelineProcessEvent,
    RunnerPipelineStartupError, RunnerPipelineStartupFailure,
    test_support as launcher_test_support,
};
use super::{RunnerPipelineControl, RunnerPipelineControlSessionError};
use crate::config::RunnerConfig;
use crate::contracts::core::pipeline_control_client::PipelineControlClient;
use crate::contracts::core::{
    PipelineAttach, PipelineBootstrap, PipelineRevisionPlan, PipelineStatusSnapshot,
    PipelineToRunner, PluginDiagnosticStream, PluginInstanceState, PluginInstanceStatus,
    RunnerToPipeline, pipeline_to_runner, runner_to_pipeline,
};
use crate::error::ErrorChain;
use crate::identifiers::{PluginInstanceId, TenonDocumentId};
use crate::pipeline::test_support::PipelineDiagnostics;
use crate::runner::diagnostics::{
    RunnerDiagnosticFrame, RunnerDiagnosticSource, RunnerDiagnosticTarget, RunnerDiagnostics,
    RunnerPluginDiagnosticStream,
};
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::pipeline::PipelineLifecycleTarget;
use crate::runner::test_support::{document, target};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use prost::Message as _;
use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, io};
use tempfile::TempDir;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::{Channel, Endpoint, Error as TransportError, Server};
use tonic::{Code, Response, Status, Streaming};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct CapturedChildProgram {
    _temporary_directory: TempDir,
    executable: PathBuf,
    arguments: PathBuf,
    process_id: PathBuf,
}

impl CapturedChildProgram {
    fn blocking() -> io::Result<Self> {
        Self::create(None)
    }

    fn exiting(exit_code: u8) -> io::Result<Self> {
        Self::create(Some(exit_code))
    }

    fn create(exit_code: Option<u8>) -> io::Result<Self> {
        let temporary_directory = tempfile::tempdir()?;
        let executable = temporary_directory.path().join("pipeline-child");
        let arguments = temporary_directory.path().join("pipeline-child.arguments");
        let process_id = temporary_directory.path().join("pipeline-child.pid");
        let ending = exit_code.map_or_else(
            || String::from("IFS= read -r ignored\n"),
            |exit_code| format!("exit {exit_code}\n"),
        );
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"${{0}}.arguments.tmp\"\nmv \"${{0}}.arguments.tmp\" \"${{0}}.arguments\"\nprintf '%s\\n' \"$$\" > \"${{0}}.pid.tmp\"\nmv \"${{0}}.pid.tmp\" \"${{0}}.pid\"\n{ending}"
            ),
        )?;
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))?;
        Ok(Self {
            _temporary_directory: temporary_directory,
            executable,
            arguments,
            process_id,
        })
    }

    async fn captured_arguments(&self) -> io::Result<Vec<String>> {
        let source = wait_for_file(&self.arguments).await?;
        Ok(source.lines().map(String::from).collect())
    }

    async fn captured_process_id(&self) -> io::Result<u32> {
        wait_for_file(&self.process_id)
            .await?
            .trim()
            .parse()
            .map_err(io::Error::other)
    }
}

async fn wait_for_file(path: &Path) -> io::Result<String> {
    time::timeout(TEST_TIMEOUT, async {
        loop {
            match fs::read_to_string(path) {
                Ok(source) => return Ok(source),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| io::Error::other("Child argument capture timed out"))?
}

fn captured_launch_id(arguments: &[String], control_socket: &str) -> io::Result<Vec<u8>> {
    let expected = [
        "pipeline",
        "--control-socket",
        control_socket,
        "--launch-id",
    ];
    if arguments.len() != 5 || arguments[..4].iter().map(String::as_str).ne(expected) {
        return Err(io::Error::other(format!(
            "Child received unexpected arguments: {arguments:?}"
        )));
    }
    STANDARD.decode(&arguments[4]).map_err(io::Error::other)
}

fn process_exists(process_id: u32) -> io::Result<bool> {
    Ok(Command::new("ps")
        .args(["-p", &process_id.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?
        .success())
}

fn wait_for_process_exit_without_reaping(process_id: u32) -> io::Result<()> {
    let process_id = i32::try_from(process_id)
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(|| io::Error::other("Test child process id is invalid"))?;
    waitid(
        WaitId::Pid(process_id),
        WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
    )?
    .ok_or_else(|| io::Error::other("Test child did not retain a waitable exit status"))?;
    Ok(())
}

#[derive(Clone)]
struct TestEndpoints {
    control: RunnerPipelineControl,
    launches: RunnerPipelineLaunchRegistry,
    diagnostics: RunnerDiagnostics,
}

fn endpoints() -> io::Result<TestEndpoints> {
    let launches = RunnerPipelineLaunchRegistry::try_new().map_err(io::Error::other)?;
    Ok(TestEndpoints {
        control: RunnerPipelineControl::new(launches.clone()),
        launches,
        diagnostics: RunnerDiagnostics::new(),
    })
}

struct LaunchFixture {
    directory: TempDir,
    config: RunnerConfig,
    target: Arc<PipelineLifecycleTarget>,
}

impl LaunchFixture {
    fn new() -> io::Result<Self> {
        use crate::payload_contract::PluginInterface;
        use crate::runner::test_support::{install_plugin, load_config};
        let directory = tempfile::tempdir()?;
        install_plugin(directory.path(), PluginInterface::Source)?;
        install_plugin(directory.path(), PluginInterface::Sink)?;
        let config = load_config(directory.path())?;
        let target = Arc::new(target(
            directory.path(),
            &config,
            "pipeline-a",
            "function main(event) emit() end",
        )?);
        Ok(Self {
            directory,
            config,
            target,
        })
    }
}

#[allow(
    clippy::expect_used,
    reason = "the shared fixture always produces valid JSON"
)]
fn status_etag() -> String {
    let document: serde_json::Value =
        serde_json::from_str(&document("pipeline-a")).expect("the fixture document is JSON");
    TenonDocumentEtag::for_source(&serde_json::to_vec(&document).expect("JSON values serialize"))
        .strong_value()
}

fn document_id() -> io::Result<TenonDocumentId> {
    TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)
}

fn register(
    endpoints: &TestEndpoints,
    bootstrap: PipelineBootstrap,
) -> io::Result<PendingPipelineLaunch> {
    Ok(endpoints.launches.register(document_id()?, bootstrap))
}

struct TestServer {
    _temporary_directory: TempDir,
    socket_path: Box<str>,
    launcher: RunnerPipelineLauncher,
    diagnostics: RunnerDiagnostics,
    task: Option<JoinHandle<Result<(), TransportError>>>,
}

impl TestServer {
    async fn start(endpoints: TestEndpoints) -> io::Result<Self> {
        let temporary_directory = tempfile::tempdir()?;
        let socket_path = temporary_directory.path().join("control.sock");
        let socket_path_text = socket_path
            .to_str()
            .ok_or_else(|| io::Error::other("Test control socket path is not UTF-8"))?
            .to_owned()
            .into_boxed_str();
        let listener = UnixListener::bind(&socket_path)?;
        let launcher = RunnerPipelineLauncher::new(
            endpoints.launches.clone(),
            socket_path.clone(),
            crate::runner::process_resources::test_support::unavailable(),
        );
        let diagnostics = endpoints.diagnostics.clone();
        let pipeline_diagnostics =
            RunnerPipelineDiagnostics::new(endpoints.launches, endpoints.diagnostics);
        let task = tokio::spawn(async move {
            let router = Server::builder().add_service(endpoints.control.into_service());
            let router = router.add_service(pipeline_diagnostics.into_service());
            router
                .serve_with_incoming(UnixListenerStream::new(listener))
                .await
        });

        Ok(Self {
            _temporary_directory: temporary_directory,
            socket_path: socket_path_text,
            launcher,
            diagnostics,
            task: Some(task),
        })
    }

    async fn attach(
        &self,
        launch_id: Vec<u8>,
    ) -> Result<Response<Streaming<RunnerToPipeline>>, Status> {
        let attach = PipelineToRunner {
            message: Some(pipeline_to_runner::Message::Attach(PipelineAttach {
                launch_id,
            })),
        };
        time::timeout(TEST_TIMEOUT, async {
            let mut client = PipelineControlClient::new(self.connect().await?);
            client
                .run(tokio_stream::once(attach).chain(tokio_stream::pending()))
                .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("Test Attach timed out"))?
    }

    async fn open(
        &self,
        launch_id: Vec<u8>,
    ) -> Result<(mpsc::Sender<PipelineToRunner>, Streaming<RunnerToPipeline>), Status> {
        time::timeout(TEST_TIMEOUT, async {
            let (outbound, inbound) = mpsc::channel(1);
            outbound
                .send(PipelineToRunner {
                    message: Some(pipeline_to_runner::Message::Attach(PipelineAttach {
                        launch_id,
                    })),
                })
                .await
                .map_err(|_| Status::internal("Test request stream closed before Attach"))?;
            let mut client = PipelineControlClient::new(self.connect().await?);
            let response = client.run(ReceiverStream::new(inbound)).await?;
            Ok((outbound, response.into_inner()))
        })
        .await
        .map_err(|_| Status::deadline_exceeded("Test Attach timed out"))?
    }

    async fn connect(&self) -> Result<Channel, Status> {
        let endpoint = Endpoint::from_shared(format!("unix://{}", self.socket_path))
            .map_err(|error| Status::internal(format!("Test endpoint is invalid: {error}")))?;
        endpoint
            .connect()
            .await
            .map_err(|error| Status::internal(format!("Test control connection failed: {error}")))
    }

    async fn finish(mut self) -> io::Result<()> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.abort();
        match task.await {
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(io::Error::other(format!(
                "Test server task failed: {error}"
            ))),
            Ok(result) => result.map_err(io::Error::other),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

mod control;
mod diagnostics;
mod instance_flow_diagnostics;
mod launch_registry;
mod launcher;
