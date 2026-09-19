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

//! Composition and lifecycle owner for the Runner's private gRPC server.
//!
//! The server hosts the correctness-control and best-effort diagnostics
//! services as peer transport adapters. Both share one launch registry with
//! the Pipeline launcher, while neither service owns Pipeline processes.

use super::runtime_resources::runner_runtime_suffix;
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::pipeline::{
    RunnerPipelineControl, RunnerPipelineDiagnostics, RunnerPipelineLaunchRegistry,
    RunnerPipelineLaunchRegistryCreateError, RunnerPipelineLauncher,
};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

type ControlServerTaskResult = Result<(), tonic::transport::Error>;
const CONTROL_SOCKET_NAME: &str = "control.sock";
const CONTROL_SOCKET_PARENT: &str = "/tmp";
const CONTROL_SOCKET_PREFIX: &str = "tenon-";

/// Owns the private server task, socket, and Pipeline launcher assembly.
pub(super) struct RunnerControlServer {
    launcher: RunnerPipelineLauncher,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<ControlServerTaskResult>>,
    socket: RunnerControlSocket,
}

impl RunnerControlServer {
    pub(super) fn start(
        runtime_directory: &Path,
        resources: std::sync::Arc<super::process_resources::RunnerResources>,
        diagnostics: RunnerDiagnostics,
        metrics: crate::runner::metrics::RunnerMetrics,
    ) -> Result<Self, RunnerControlServerError> {
        let launches = RunnerPipelineLaunchRegistry::try_new()
            .map_err(RunnerControlServerError::LaunchRegistryInitialization)?;
        let mut socket = RunnerControlSocket::create(runtime_directory)?;
        let listener = match UnixListener::bind(socket.path()) {
            Ok(listener) => listener,
            Err(source) => {
                let primary = RunnerControlServerError::SocketBind(source);
                let cleanup = socket.close().err();
                return Err(combine_failures(primary, cleanup));
            }
        };
        let (shutdown, shutdown_requested) = oneshot::channel();
        let launcher = RunnerPipelineLauncher::new(launches.clone(), socket.path(), resources);
        let task = tokio::spawn(async move {
            let control = RunnerPipelineControl::new(launches.clone());
            let diagnostics = RunnerPipelineDiagnostics::new(launches.clone(), diagnostics);
            let metrics = metrics.into_service(launches);
            Server::builder()
                .add_service(control.into_service())
                .add_service(diagnostics.into_service())
                .add_service(metrics)
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                    let _ = shutdown_requested.await;
                })
                .await
        });
        Ok(Self {
            launcher,
            shutdown: Some(shutdown),
            task: Some(task),
            socket,
        })
    }

    pub(super) fn remove_stale_socket(
        runtime_directory: &Path,
    ) -> Result<(), RunnerControlServerError> {
        let directory = control_socket_directory(runtime_directory)?;
        match fs::remove_dir_all(&directory) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(RunnerControlServerError::SocketCleanup { directory, source }),
        }
    }

    pub(super) fn pipeline_launcher(&self) -> RunnerPipelineLauncher {
        self.launcher.clone()
    }

    /// Waits for an unexpected server exit without detaching the task if this
    /// future loses a `select!` race.
    pub(super) async fn wait(&mut self) -> RunnerControlServerError {
        match self.join().await {
            Some(result) => RunnerControlServerError::unexpected_exit(result),
            None => RunnerControlServerError::Stopped,
        }
    }

    /// Stops admission and chooses graceful connection drain only when every
    /// Pipeline process owner was recovered.
    pub(super) async fn shutdown(
        mut self,
        mode: ControlServerShutdown,
    ) -> RunnerControlServerShutdownResult {
        match mode {
            ControlServerShutdown::Graceful => {
                if let Some(shutdown) = self.shutdown.take() {
                    let _ = shutdown.send(());
                }
            }
            ControlServerShutdown::Abort => {
                self.shutdown = None;
                if let Some(task) = self.task.as_ref() {
                    task.abort();
                }
            }
        }

        let task_failure = match self.join().await {
            Some(Err(source))
                if matches!(mode, ControlServerShutdown::Abort) && source.is_cancelled() =>
            {
                None
            }
            Some(Ok(Ok(()))) | None => None,
            Some(Ok(Err(source))) => Some(RunnerControlServerError::Serve(source)),
            Some(Err(source)) => Some(RunnerControlServerError::Task(source)),
        };
        let socket_failure = match mode {
            ControlServerShutdown::Graceful => self.socket.close().err(),
            ControlServerShutdown::Abort => {
                self.socket.keep();
                None
            }
        };
        let socket_cleanup =
            if matches!(mode, ControlServerShutdown::Abort) || socket_failure.is_some() {
                ControlSocketCleanup::Incomplete
            } else {
                ControlSocketCleanup::Complete
            };
        let failure = match (task_failure, socket_failure) {
            (Some(primary), cleanup) => Some(combine_failures(primary, cleanup)),
            (None, cleanup) => cleanup,
        };
        RunnerControlServerShutdownResult {
            failure,
            socket_cleanup,
        }
    }

    async fn join(&mut self) -> Option<Result<ControlServerTaskResult, JoinError>> {
        let result = self.task.as_mut()?.await;
        self.task = None;
        Some(result)
    }
}

impl Drop for RunnerControlServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // Dropping the server does not prove that any Pipeline was reaped.
        self.socket.keep();
    }
}

#[derive(Clone, Copy)]
pub(super) enum ControlServerShutdown {
    Graceful,
    Abort,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ControlSocketCleanup {
    Complete,
    Incomplete,
}

pub(super) struct RunnerControlServerShutdownResult {
    pub(super) failure: Option<RunnerControlServerError>,
    pub(super) socket_cleanup: ControlSocketCleanup,
}

#[derive(Debug)]
pub(super) enum RunnerControlServerError {
    LaunchRegistryInitialization(RunnerPipelineLaunchRegistryCreateError),
    SocketPathInvalid(PathBuf),
    RuntimeIdentity {
        directory: PathBuf,
        source: io::Error,
    },
    SocketDirectory {
        directory: PathBuf,
        source: io::Error,
    },
    SocketCleanup {
        directory: PathBuf,
        source: io::Error,
    },
    SocketBind(io::Error),
    Serve(tonic::transport::Error),
    Task(JoinError),
    Stopped,
    Cleanup {
        primary: Box<Self>,
        cleanup: Box<Self>,
    },
}

impl RunnerControlServerError {
    pub(super) const fn requires_runtime_recovery(&self) -> bool {
        match self {
            Self::SocketCleanup { .. } | Self::Cleanup { .. } => true,
            Self::LaunchRegistryInitialization(_)
            | Self::SocketPathInvalid(_)
            | Self::RuntimeIdentity { .. }
            | Self::SocketDirectory { .. }
            | Self::SocketBind(_)
            | Self::Serve(_)
            | Self::Task(_)
            | Self::Stopped => false,
        }
    }
    fn unexpected_exit(result: Result<ControlServerTaskResult, JoinError>) -> Self {
        match result {
            Ok(Ok(())) => Self::Stopped,
            Ok(Err(source)) => Self::Serve(source),
            Err(source) => Self::Task(source),
        }
    }
}

impl fmt::Display for RunnerControlServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LaunchRegistryInitialization(_) => {
                formatter.write_str("Runner Pipeline launch registry initialization failed")
            }
            Self::SocketPathInvalid(path) => write!(
                formatter,
                "Runner Pipeline control socket path is invalid: {}",
                path.display()
            ),
            Self::RuntimeIdentity { directory, .. } => write!(
                formatter,
                "Runner runtime directory identity could not be read: {}",
                directory.display()
            ),
            Self::SocketDirectory { directory, .. } => write!(
                formatter,
                "Runner Pipeline control directory operation failed: {}",
                directory.display()
            ),
            Self::SocketCleanup { directory, .. } => write!(
                formatter,
                "Runner Pipeline control directory cleanup failed: {}",
                directory.display()
            ),
            Self::SocketBind(_) => {
                formatter.write_str("Runner Pipeline control socket could not be bound")
            }
            Self::Serve(_) | Self::Task(_) | Self::Stopped => {
                formatter.write_str("Runner Pipeline control service stopped unexpectedly")
            }
            Self::Cleanup { primary, cleanup } => {
                write!(formatter, "{primary}; cleanup also failed: {cleanup}")
            }
        }
    }
}

impl Error for RunnerControlServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LaunchRegistryInitialization(source) => Some(source),
            Self::RuntimeIdentity { source, .. }
            | Self::SocketDirectory { source, .. }
            | Self::SocketCleanup { source, .. }
            | Self::SocketBind(source) => Some(source),
            Self::Serve(source) => Some(source),
            Self::Task(source) => Some(source),
            Self::Cleanup { primary, .. } => Some(primary.as_ref()),
            Self::SocketPathInvalid(_) | Self::Stopped => None,
        }
    }
}

fn combine_failures(
    primary: RunnerControlServerError,
    cleanup: Option<RunnerControlServerError>,
) -> RunnerControlServerError {
    match cleanup {
        Some(cleanup) => RunnerControlServerError::Cleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        },
        None => primary,
    }
}

struct RunnerControlSocket {
    directory: PathBuf,
}

impl RunnerControlSocket {
    fn create(runtime_directory: &Path) -> Result<Self, RunnerControlServerError> {
        let directory = control_socket_directory(runtime_directory)?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .map_err(|source| RunnerControlServerError::SocketDirectory {
                directory: directory.clone(),
                source,
            })?;
        let mut socket = Self { directory };
        if let Err(source) =
            fs::set_permissions(&socket.directory, fs::Permissions::from_mode(0o700))
        {
            let primary = RunnerControlServerError::SocketDirectory {
                directory: socket.directory.clone(),
                source,
            };
            let cleanup = socket.close().err();
            return Err(combine_failures(primary, cleanup));
        }
        Ok(socket)
    }

    fn path(&self) -> PathBuf {
        self.directory.join(CONTROL_SOCKET_NAME)
    }

    fn close(&mut self) -> Result<(), RunnerControlServerError> {
        let result = self.remove();
        if result.is_ok() {
            self.directory.clear();
        }
        result
    }

    fn keep(&mut self) {
        self.directory.clear();
    }

    fn remove(&self) -> Result<(), RunnerControlServerError> {
        match fs::remove_dir_all(&self.directory) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(RunnerControlServerError::SocketCleanup {
                directory: self.directory.clone(),
                source,
            }),
        }
    }
}

impl Drop for RunnerControlSocket {
    fn drop(&mut self) {
        if !self.directory.as_os_str().is_empty() {
            drop(fs::remove_dir_all(&self.directory));
        }
    }
}

fn control_socket_directory(runtime_directory: &Path) -> Result<PathBuf, RunnerControlServerError> {
    let name = runner_runtime_suffix(runtime_directory).map_err(|_| {
        RunnerControlServerError::SocketPathInvalid(runtime_directory.to_path_buf())
    })?;
    let metadata = runtime_directory.metadata().map_err(|source| {
        RunnerControlServerError::RuntimeIdentity {
            directory: runtime_directory.to_path_buf(),
            source,
        }
    })?;
    Ok(Path::new(CONTROL_SOCKET_PARENT).join(format!(
        "{CONTROL_SOCKET_PREFIX}{name}-{:x}-{:x}",
        metadata.dev(),
        metadata.ino()
    )))
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;

    pub(in crate::runner) fn socket_path(server: &RunnerControlServer) -> PathBuf {
        server.socket.path()
    }

    pub(super) fn server(
        shutdown: oneshot::Sender<()>,
        task: JoinHandle<ControlServerTaskResult>,
    ) -> io::Result<RunnerControlServer> {
        let directory = tempfile::tempdir()?.keep();
        let launches = RunnerPipelineLaunchRegistry::try_new().map_err(io::Error::other)?;
        let launcher = RunnerPipelineLauncher::new(
            launches,
            directory.join(CONTROL_SOCKET_NAME),
            crate::runner::process_resources::test_support::unavailable(),
        );
        Ok(RunnerControlServer {
            launcher,
            shutdown: Some(shutdown),
            task: Some(task),
            socket: RunnerControlSocket { directory },
        })
    }

    pub(in crate::runner) fn pipeline_launcher(
        socket_path: PathBuf,
    ) -> io::Result<RunnerPipelineLauncher> {
        let launches = RunnerPipelineLaunchRegistry::try_new().map_err(io::Error::other)?;
        Ok(RunnerPipelineLauncher::new(
            launches,
            socket_path,
            crate::runner::process_resources::test_support::unavailable(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::server;
    use super::*;
    use std::io;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_prepared_runner_owners_preserves_recovery_identity() -> io::Result<()> {
        use super::super::runtime_resources::{
            RunnerRuntimeResources, remove_stale_runtime_directory,
        };
        use crate::runner::test_support::captured_runner_executable;
        let directory = tempfile::tempdir()?;
        let resources =
            RunnerRuntimeResources::prepare(directory.path(), captured_runner_executable()?)
                .map_err(io::Error::other)?;
        let runtime = resources.directory().to_path_buf();
        let server = RunnerControlServer::start(
            &runtime,
            crate::runner::process_resources::test_support::unavailable(),
            RunnerDiagnostics::new(),
            crate::runner::metrics::test_support::empty()?,
        )
        .map_err(io::Error::other)?;
        let retained = RunnerControlSocket {
            directory: server.socket.directory.clone(),
        };

        drop(server);
        drop(resources);

        assert!(
            runtime.is_dir(),
            "recovery must retain the exact short-directory identity"
        );
        assert!(retained.directory.is_dir());
        RunnerControlServer::remove_stale_socket(&runtime).map_err(io::Error::other)?;
        remove_stale_runtime_directory(runtime).map_err(io::Error::other)?;
        assert!(!retained.directory.exists());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn complete_recovery_gracefully_stops_the_server() -> io::Result<()> {
        let (shutdown, shutdown_requested) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = shutdown_requested.await;
            Ok(())
        });
        let server = server(shutdown, task)?;

        let path = server.socket.directory.clone();

        let cleanup = server.shutdown(ControlServerShutdown::Graceful).await;

        assert!(cleanup.failure.is_none());
        assert!(matches!(
            cleanup.socket_cleanup,
            ControlSocketCleanup::Complete
        ));
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incomplete_recovery_aborts_without_waiting_for_connections() -> io::Result<()> {
        let (shutdown, _shutdown_requested) = oneshot::channel();
        let task = tokio::spawn(async move {
            std::future::pending::<()>().await;
            Ok(())
        });
        let server = server(shutdown, task)?;
        let retained = RunnerControlSocket {
            directory: server.socket.directory.clone(),
        };
        let plugin_directory = retained.directory.join("pipeline-launch");
        fs::create_dir(&plugin_directory)?;
        fs::write(plugin_directory.join("control.sock"), b"owned endpoint")?;

        let cleanup = tokio::time::timeout(
            Duration::from_secs(1),
            server.shutdown(ControlServerShutdown::Abort),
        )
        .await
        .map_err(|_| io::Error::other("Forced control-server shutdown timed out"))?;

        assert!(cleanup.failure.is_none());
        assert!(matches!(
            cleanup.socket_cleanup,
            ControlSocketCleanup::Incomplete
        ));
        assert!(plugin_directory.join("control.sock").is_file());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_owner_aborts_its_server_task() -> io::Result<()> {
        let (shutdown, _shutdown_requested) = oneshot::channel();
        let (started, started_receiver) = oneshot::channel();
        let (dropped, dropped_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = DropSignal(Some(dropped));
            let _ = started.send(());
            std::future::pending::<()>().await;
            Ok(())
        });
        let server = server(shutdown, task)?;
        let _retained = RunnerControlSocket {
            directory: server.socket.directory.clone(),
        };
        let _ = started_receiver.await;

        drop(server);

        tokio::time::timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .map_err(|_| io::Error::other("Dropped control server task was not aborted"))?
            .map_err(|_| io::Error::other("Control server task dropped without its guard"))?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_wait_keeps_the_task_owned_for_a_later_wait() -> io::Result<()> {
        let (shutdown, _shutdown_requested) = oneshot::channel();
        let (finish, finish_requested) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = finish_requested.await;
            Ok(())
        });
        let mut server = server(shutdown, task)?;
        let _retained = RunnerControlSocket {
            directory: server.socket.directory.clone(),
        };

        {
            let first_wait = server.wait();
            tokio::pin!(first_wait);
            tokio::select! {
                _ = &mut first_wait => {
                    return Err(io::Error::other(
                        "Pending control server wait completed early",
                    ));
                }
                () = tokio::task::yield_now() => {}
            }
        }
        let _ = finish.send(());

        assert!(matches!(
            server.wait().await,
            RunnerControlServerError::Stopped
        ));
        Ok(())
    }

    #[test]
    fn socket_directory_failures_keep_the_paired_runtime_for_recovery() {
        let identity_failure = RunnerControlServerError::RuntimeIdentity {
            directory: PathBuf::from("/tmp/missing-runtime"),
            source: io::Error::other("injected identity failure"),
        };
        assert!(!identity_failure.requires_runtime_recovery());

        let cleaned_failure = RunnerControlServerError::SocketDirectory {
            directory: PathBuf::from("/tmp/tenon-cleaned"),
            source: io::Error::other("injected creation failure"),
        };
        assert!(!cleaned_failure.requires_runtime_recovery());

        let cleanup_failure = RunnerControlServerError::Cleanup {
            primary: Box::new(RunnerControlServerError::SocketBind(io::Error::other(
                "injected bind failure",
            ))),
            cleanup: Box::new(RunnerControlServerError::SocketCleanup {
                directory: PathBuf::from("/tmp/tenon-retained"),
                source: io::Error::other("injected cleanup failure"),
            }),
        };
        assert!(cleanup_failure.requires_runtime_recovery());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn equal_runtime_suffixes_in_different_state_directories_can_run_concurrently()
    -> io::Result<()> {
        let left_state = tempfile::tempdir()?;
        let right_state = tempfile::tempdir()?;
        let runtime_name = ".tenon-runner-equal";
        let left_runtime = left_state.path().join(runtime_name);
        let right_runtime = right_state.path().join(runtime_name);
        fs::create_dir(&left_runtime)?;
        fs::create_dir(&right_runtime)?;

        let left_server = RunnerControlServer::start(
            &left_runtime,
            crate::runner::process_resources::test_support::unavailable(),
            RunnerDiagnostics::new(),
            crate::runner::metrics::test_support::empty()?,
        )
        .map_err(io::Error::other)?;
        let right_server = RunnerControlServer::start(
            &right_runtime,
            crate::runner::process_resources::test_support::unavailable(),
            RunnerDiagnostics::new(),
            crate::runner::metrics::test_support::empty()?,
        )
        .map_err(io::Error::other)?;

        assert_ne!(left_server.socket.path(), right_server.socket.path());
        assert!(
            left_server
                .shutdown(ControlServerShutdown::Graceful)
                .await
                .failure
                .is_none()
        );
        assert!(
            right_server
                .shutdown(ControlServerShutdown::Graceful)
                .await
                .failure
                .is_none()
        );
        Ok(())
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }
}
