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
use std::error::Error;
use std::io;
use std::sync::mpsc;
use std::task::{Context, Waker};
use std::thread::{self, ThreadId};
use std::time::Duration;
use tokio::sync::oneshot;

type TestResult = Result<(), Box<dyn Error>>;
const WAIT_LIMIT: Duration = Duration::from_secs(2);

#[test]
fn force_shutdown_cannot_be_downgraded_to_planned_cleanup() {
    let (sender, receiver) = tokio::sync::watch::channel(None);
    let handle = super::super::PipelineReconfigureShutdownHandle { sender };

    handle.request(ReconfigureShutdown::Planned);
    assert_eq!(*receiver.borrow(), Some(ReconfigureShutdown::Planned));

    handle.request(ReconfigureShutdown::Force);
    assert_eq!(*receiver.borrow(), Some(ReconfigureShutdown::Force));

    handle.request(ReconfigureShutdown::Planned);
    assert_eq!(*receiver.borrow(), Some(ReconfigureShutdown::Force));
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_a_running_blocking_job_joins_and_drops_its_output_before_return() -> TestResult
{
    let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let (dropped_sender, dropped_receiver) = tokio::sync::oneshot::channel();
    let (cancelled_sender, mut cancelled_receiver) = tokio::sync::oneshot::channel();
    let mut job = spawn_blocking_reconfigure_work(
        Some(Box::new(move || {
            let _ = cancelled_sender.send(());
        })),
        move || {
            let _ = started_sender.send(());
            let _ = release_receiver.recv();
            Ok(DropSignal(Some(dropped_sender)))
        },
    );
    started_receiver
        .await
        .map_err(|_| io::Error::other("Blocking job did not start"))?;

    let cancellation = job.cancel_and_discard();
    tokio::pin!(cancellation);
    let mut context = Context::from_waker(Waker::noop());
    assert!(cancellation.as_mut().poll(&mut context).is_pending());
    assert!(matches!(cancelled_receiver.try_recv(), Ok(())));

    release_sender
        .send(())
        .map_err(|_| io::Error::other("Blocking job stopped before release"))?;
    cancellation.await?;
    dropped_receiver
        .await
        .map_err(|_| io::Error::other("Blocking job output was not dropped"))?;
    Ok(())
}

struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_does_not_hide_a_failed_directory_cleanup() -> TestResult {
    let (started_sender, started_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let mut job = spawn_blocking_reconfigure_work(
        Some(Box::new(move || {
            let _ = release_sender.send(());
        })),
        move || {
            let _ = started_sender.send(());
            assert!(
                release_receiver.recv_timeout(WAIT_LIMIT).is_ok(),
                "Cancellation did not release the running job"
            );
            Err::<(), _>(PipelineReconfigureError::DirectoryRemove {
                path: "candidate".into(),
                source: io::Error::new(io::ErrorKind::PermissionDenied, "cleanup denied"),
            })
        },
    );
    started_receiver.await?;
    let error = job
        .cancel_and_discard()
        .await
        .err()
        .ok_or_else(|| io::Error::other("Cancellation hid the directory cleanup failure"))?;
    assert!(matches!(error,
        PipelineReconfigureError::DirectoryRemove { path, source }
        if path == std::path::Path::new("candidate") && source.kind() == io::ErrorKind::PermissionDenied
    ));
    Ok(())
}

#[test]
fn cancelling_a_queued_job_never_executes_its_work() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    runtime.block_on(async {
        let (occupied_sender, occupied_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let occupied = tokio::task::spawn_blocking(move || {
            let _ = occupied_sender.send(());
            release_receiver.recv_timeout(WAIT_LIMIT)
        });
        occupied_receiver.await?;
        let (started_sender, mut started_receiver) = oneshot::channel();
        let (cancelled_sender, cancelled_receiver) = oneshot::channel();
        let mut job = spawn_blocking_reconfigure_work(
            Some(Box::new(move || {
                let _ = cancelled_sender.send(());
            })),
            move || {
                let _ = started_sender.send(());
                Ok(())
            },
        );
        let release_pool = async {
            cancelled_receiver.await?;
            release_sender.send(())?;
            occupied.await??;
            Ok::<_, Box<dyn Error>>(())
        };
        let (cancelled, released) = tokio::join!(job.cancel_and_discard(), release_pool);
        cancelled?;
        released?;
        assert!(matches!(
            started_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        Ok(())
    })
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_a_completed_job_keeps_control_responsive_until_disposal_finishes() -> TestResult
{
    let control_thread = thread::current().id();
    let (started_sender, started_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let (finished_sender, mut finished_receiver) = oneshot::channel();
    let mut job = spawn_blocking_reconfigure_work(None, move || {
        Ok(ControlledDisposal {
            started: Some(started_sender),
            release: release_receiver,
            finished: Some(finished_sender),
        })
    });
    tokio::time::timeout(WAIT_LIMIT, async {
        while !job.task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let control_progress = async {
        let disposal_thread = started_receiver.await?;
        // Only this control-thread future can allow disposal to finish.
        release_sender.send(())?;
        assert_ne!(disposal_thread, control_thread);
        Ok::<_, Box<dyn Error>>(())
    };
    let cancellation = async {
        job.cancel_and_discard().await?;
        finished_receiver.try_recv().map_err(io::Error::other)??;
        Ok::<_, Box<dyn Error>>(())
    };
    let (cancelled, progress) = tokio::join!(cancellation, control_progress);
    cancelled?;
    progress?;
    Ok(())
}

struct ControlledDisposal {
    started: Option<oneshot::Sender<ThreadId>>,
    release: mpsc::Receiver<()>,
    finished: Option<oneshot::Sender<io::Result<()>>>,
}

impl Drop for ControlledDisposal {
    fn drop(&mut self) {
        if let Some(started) = self.started.take() {
            let _ = started.send(thread::current().id());
        }
        let released = self
            .release
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other);
        if let Some(finished) = self.finished.take() {
            let _ = finished.send(released);
        }
    }
}
