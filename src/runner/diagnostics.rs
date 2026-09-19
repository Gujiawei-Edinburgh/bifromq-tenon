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

//! Adapter-independent fan-out for live Runner diagnostics.
//!
//! Each HTTP or future TUI subscriber owns one bounded receiver. Slow
//! subscribers lose only their own diagnostic records; the next delivered
//! record's producer sequence exposes the discontinuity. Subscriber reference
//! counts are projected into one latest runtime-object interest per Pipeline;
//! no transport adapter owns that policy.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;

use crate::identifiers::{FlowId, PluginInstanceId, TenonDocumentId};

const SUBSCRIBER_QUEUE_CAPACITY: usize = 256;

/// One exact runtime producer selected by a diagnostics subscriber.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RunnerDiagnosticTarget {
    FlowChannel { flow_id: FlowId, channel_index: u32 },
    Plugin(PluginInstanceId),
}

/// One standard stream emitted by a Plugin process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunnerPluginDiagnosticStream {
    Stdout,
    Stderr,
}

/// The category of one Flow Channel diagnostic record.
///
/// A Lua `print` is ordinary output; an error is a diagnostic detail emitted
/// only after the Flow Channel already recorded the failure in its metrics and
/// let the Pipeline continue on its normal failure path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunnerChannelDiagnosticKind {
    Print,
    Error,
}

/// Runtime identity carried by one normalized diagnostic record.
#[derive(Debug)]
pub(crate) enum RunnerDiagnosticSource {
    FlowChannel {
        flow_id: FlowId,
        channel_index: u32,
        channel_instance_id: u64,
        lua_vm_instance_id: u64,
        kind: RunnerChannelDiagnosticKind,
        /// Flow Channel processing phase that failed, present only for errors.
        phase: Option<Box<str>>,
        /// Failure code shared with the Flow error metric, present only for errors.
        code: Option<Box<str>>,
    },
    Plugin {
        plugin_instance_id: PluginInstanceId,
        plugin_process_instance_id: u64,
        stream: RunnerPluginDiagnosticStream,
    },
}

impl RunnerDiagnosticSource {
    #[must_use]
    pub(crate) fn target(&self) -> RunnerDiagnosticTarget {
        match self {
            Self::FlowChannel {
                flow_id,
                channel_index,
                ..
            } => RunnerDiagnosticTarget::FlowChannel {
                flow_id: flow_id.clone(),
                channel_index: *channel_index,
            },
            Self::Plugin {
                plugin_instance_id, ..
            } => RunnerDiagnosticTarget::Plugin(plugin_instance_id.clone()),
        }
    }
}

/// One normalized diagnostic record received from an exact Pipeline launch.
#[derive(Debug)]
pub(crate) struct RunnerDiagnostic {
    pub(crate) pipeline_instance_id: Arc<str>,
    pub(crate) source: RunnerDiagnosticSource,
    pub(crate) observed_at_unix_millis: u64,
    pub(crate) text: Box<str>,
    pub(crate) truncated: bool,
    pub(crate) invalid_utf8: bool,
    pub(crate) sequence: u64,
}

/// One adapter-independent item delivered to a live subscriber.
#[derive(Debug)]
pub(crate) enum RunnerDiagnosticFrame {
    Attached,
    Diagnostic(Arc<RunnerDiagnostic>),
    Closed,
}

/// Shared live-diagnostics boundary used by internal transport and public adapters.
#[derive(Clone)]
pub(crate) struct RunnerDiagnostics {
    shared: Arc<Mutex<RunnerDiagnosticsState>>,
}

impl RunnerDiagnostics {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(Mutex::new(RunnerDiagnosticsState::default())),
        }
    }

    /// Subscribes one bounded consumer and updates the latest Pipeline interest.
    #[allow(
        clippy::expect_used,
        reason = "a newly created bounded queue always accepts its first frame"
    )]
    pub(crate) fn subscribe(
        &self,
        pipeline_id: TenonDocumentId,
        target: RunnerDiagnosticTarget,
    ) -> Result<RunnerDiagnosticSubscription, RunnerDiagnosticsClosed> {
        let mut state = self.lock();
        if state.closed {
            return Err(RunnerDiagnosticsClosed);
        }
        let subscription_id = state.next_subscription_id;
        state.next_subscription_id += 1;
        let (sender, receiver) = mpsc::channel(SUBSCRIBER_QUEUE_CAPACITY);
        sender
            .try_send(RunnerDiagnosticFrame::Attached)
            .expect("New diagnostic subscription queue must accept its Attached frame");
        let document = state.document_mut(&pipeline_id);
        document
            .subscribers
            .insert(subscription_id, DiagnosticSubscriber { target, sender });
        document.publish_interest();
        Ok(RunnerDiagnosticSubscription {
            pipeline_id,
            subscription_id,
            receiver,
            diagnostics: self.clone(),
        })
    }

    /// Returns the latest runtime-object interest snapshot for one Pipeline.
    pub(crate) fn interest(
        &self,
        pipeline_id: &TenonDocumentId,
    ) -> watch::Receiver<Arc<[RunnerDiagnosticTarget]>> {
        self.lock().document_mut(pipeline_id).interest.subscribe()
    }

    /// Selects the exact current Pipeline process that may publish diagnostics.
    pub(crate) fn activate_pipeline_instance(
        &self,
        pipeline_id: &TenonDocumentId,
        pipeline_instance_id: Arc<str>,
    ) {
        let mut state = self.lock();
        if state.closed {
            return;
        }
        let document = state.document_mut(pipeline_id);
        if document
            .active_pipeline
            .as_ref()
            .is_some_and(|active| active.as_ref() == pipeline_instance_id.as_ref())
        {
            return;
        }
        document.active_pipeline = Some(pipeline_instance_id);
    }

    /// Retires one exact Pipeline process without disturbing a newer instance.
    pub(crate) fn deactivate_pipeline_instance(
        &self,
        pipeline_id: &TenonDocumentId,
        pipeline_instance_id: &str,
    ) {
        let mut state = self.lock();
        let remove = match state.documents.get_mut(pipeline_id) {
            Some(document)
                if document
                    .active_pipeline
                    .as_ref()
                    .is_some_and(|active| active.as_ref() == pipeline_instance_id) =>
            {
                document.active_pipeline = None;
                document.subscribers.is_empty()
            }
            Some(_) | None => false,
        };
        if remove {
            state.documents.remove(pipeline_id);
        }
    }

    /// Publishes one best-effort record to every matching subscriber.
    pub(crate) fn publish(&self, pipeline_id: &TenonDocumentId, record: RunnerDiagnostic) {
        let mut state = self.lock();
        if state.closed {
            return;
        }
        let Some(document) = state.documents.get_mut(pipeline_id) else {
            return;
        };
        let Some(active_pipeline) = document.active_pipeline.as_ref() else {
            return;
        };
        if active_pipeline.as_ref() != record.pipeline_instance_id.as_ref() {
            return;
        }
        let target = record.source.target();
        let record = Arc::new(record);
        for subscriber in document.subscribers.values_mut() {
            if subscriber.target != target {
                continue;
            }
            let _ = subscriber
                .sender
                .try_send(RunnerDiagnosticFrame::Diagnostic(Arc::clone(&record)));
        }
    }

    /// Ends all subscribers after Pipeline cleanup and clears every interest.
    pub(crate) fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        for document in state.documents.values_mut() {
            for subscriber in document.subscribers.values_mut() {
                let _ = subscriber.sender.try_send(RunnerDiagnosticFrame::Closed);
            }
            document.subscribers.clear();
            document.publish_interest();
        }
    }

    fn detach(&self, pipeline_id: &TenonDocumentId, subscription_id: u64) {
        let mut state = self.lock();
        let remove = match state.documents.get_mut(pipeline_id) {
            Some(document) => {
                document.subscribers.remove(&subscription_id);
                document.publish_interest();
                document.subscribers.is_empty() && document.active_pipeline.is_none()
            }
            None => false,
        };
        if remove {
            state.documents.remove(pipeline_id);
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "the short diagnostics critical sections execute no panicking user code"
    )]
    fn lock(&self) -> MutexGuard<'_, RunnerDiagnosticsState> {
        self.shared
            .lock()
            .expect("Runner diagnostics state lock must not be poisoned")
    }
}

impl fmt::Debug for RunnerDiagnostics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        formatter
            .debug_struct("RunnerDiagnostics")
            .field("closed", &state.closed)
            .field("document_count", &state.documents.len())
            .finish()
    }
}

#[derive(Default)]
struct RunnerDiagnosticsState {
    closed: bool,
    next_subscription_id: u64,
    documents: HashMap<TenonDocumentId, DocumentDiagnostics>,
}

impl RunnerDiagnosticsState {
    fn document_mut(&mut self, pipeline_id: &TenonDocumentId) -> &mut DocumentDiagnostics {
        self.documents
            .entry(pipeline_id.clone())
            .or_insert_with(DocumentDiagnostics::new)
    }
}

struct DocumentDiagnostics {
    subscribers: HashMap<u64, DiagnosticSubscriber>,
    // The watch sender is the live notification resource; its current value is
    // projected from subscribers but cannot be replaced by a computed return.
    interest: watch::Sender<Arc<[RunnerDiagnosticTarget]>>,
    active_pipeline: Option<Arc<str>>,
}

impl DocumentDiagnostics {
    fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
            interest: watch::channel(Arc::from([])).0,
            active_pipeline: None,
        }
    }

    fn publish_interest(&self) {
        let mut targets = self
            .subscribers
            .values()
            .map(|subscriber| subscriber.target.clone())
            .collect::<Vec<_>>();
        targets.sort_unstable();
        targets.dedup();
        let targets = Arc::<[RunnerDiagnosticTarget]>::from(targets);
        self.interest.send_if_modified(|current| {
            if current.as_ref() == targets.as_ref() {
                return false;
            }
            *current = targets;
            true
        });
    }
}

struct DiagnosticSubscriber {
    target: RunnerDiagnosticTarget,
    sender: mpsc::Sender<RunnerDiagnosticFrame>,
}

/// A live bounded subscription whose drop removes its Pipeline interest.
pub(crate) struct RunnerDiagnosticSubscription {
    pipeline_id: TenonDocumentId,
    subscription_id: u64,
    receiver: mpsc::Receiver<RunnerDiagnosticFrame>,
    diagnostics: RunnerDiagnostics,
}

impl Stream for RunnerDiagnosticSubscription {
    type Item = RunnerDiagnosticFrame;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(context)
    }
}

impl Drop for RunnerDiagnosticSubscription {
    fn drop(&mut self) {
        self.diagnostics
            .detach(&self.pipeline_id, self.subscription_id);
    }
}

impl fmt::Debug for RunnerDiagnosticSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerDiagnosticSubscription")
            .field("pipeline_id", &self.pipeline_id)
            .field("subscription_id", &self.subscription_id)
            .finish_non_exhaustive()
    }
}

/// The Runner is no longer admitting live diagnostics subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RunnerDiagnosticsClosed;

impl fmt::Display for RunnerDiagnosticsClosed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Runner diagnostics are closed")
    }
}

impl std::error::Error for RunnerDiagnosticsClosed {}

#[cfg(test)]
mod tests {
    use std::io;

    use tokio_stream::StreamExt as _;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn subscribers_fan_out_without_crossing_channel_targets() -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        activate(&diagnostics, &pipeline_id);
        let mut channel_zero = diagnostics
            .subscribe(pipeline_id.clone(), channel(0))
            .map_err(io::Error::other)?;
        let mut channel_one = diagnostics
            .subscribe(pipeline_id.clone(), channel(1))
            .map_err(io::Error::other)?;
        let _attached_zero = channel_zero.next().await;
        let _attached_one = channel_one.next().await;

        diagnostics.publish(&pipeline_id, record(0, "zero"));

        assert!(matches!(
            channel_zero.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(_))
        ));
        assert!(channel_one.receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plugin_subscribers_fan_out_by_exact_instance() -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        let sink_id = PluginInstanceId::try_from("archive").map_err(io::Error::other)?;
        let other_sink_id = PluginInstanceId::try_from("backup").map_err(io::Error::other)?;
        activate(&diagnostics, &pipeline_id);
        let mut source = diagnostics
            .subscribe(
                pipeline_id.clone(),
                RunnerDiagnosticTarget::Plugin(source_id()),
            )
            .map_err(io::Error::other)?;
        let mut sink = diagnostics
            .subscribe(
                pipeline_id.clone(),
                RunnerDiagnosticTarget::Plugin(sink_id.clone()),
            )
            .map_err(io::Error::other)?;
        let mut other_sink = diagnostics
            .subscribe(
                pipeline_id.clone(),
                RunnerDiagnosticTarget::Plugin(other_sink_id),
            )
            .map_err(io::Error::other)?;
        let _source_attached = source.next().await;
        let _sink_attached = sink.next().await;
        let _other_sink_attached = other_sink.next().await;

        diagnostics.publish(&pipeline_id, source_record("source"));
        assert!(matches!(
            source.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(record)) if record.text.as_ref() == "source"
        ));
        assert!(sink.receiver.try_recv().is_err());
        assert!(other_sink.receiver.try_recv().is_err());

        diagnostics.publish(&pipeline_id, sink_record(sink_id, "sink"));
        assert!(matches!(
            sink.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(record)) if record.text.as_ref() == "sink"
        ));
        assert!(source.receiver.try_recv().is_err());
        assert!(other_sink.receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn last_drop_clears_channel_interest() -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        activate(&diagnostics, &pipeline_id);
        let mut interest = diagnostics.interest(&pipeline_id);
        let first = diagnostics
            .subscribe(pipeline_id.clone(), channel(2))
            .map_err(io::Error::other)?;
        let second = diagnostics
            .subscribe(pipeline_id.clone(), channel(2))
            .map_err(io::Error::other)?;
        interest.changed().await.map_err(io::Error::other)?;
        assert_eq!(interest.borrow_and_update().as_ref(), [channel(2)]);

        drop(first);
        assert_eq!(interest.borrow().as_ref(), [channel(2)]);
        drop(second);
        interest.changed().await.map_err(io::Error::other)?;
        assert!(interest.borrow_and_update().is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn producer_sequence_discontinuity_is_delivered_without_a_synthetic_frame()
    -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        activate(&diagnostics, &pipeline_id);
        let mut subscription = diagnostics
            .subscribe(pipeline_id.clone(), channel(0))
            .map_err(io::Error::other)?;
        let _attached = subscription.next().await;
        diagnostics.publish(&pipeline_id, record(0, "before-loss"));
        assert!(matches!(
            subscription.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(_))
        ));
        let mut event = record(0, "after-loss");
        event.sequence = 8;

        diagnostics.publish(&pipeline_id, event);

        assert!(matches!(
            subscription.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(record)) if record.sequence == 8
        ));
        assert!(subscription.receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rebuilt_pipeline_starts_an_independent_sequence_namespace() -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        let mut subscription = diagnostics
            .subscribe(pipeline_id.clone(), channel(0))
            .map_err(io::Error::other)?;
        let _attached = subscription.next().await;
        diagnostics.activate_pipeline_instance(&pipeline_id, Arc::from("instance-old"));
        let mut old = record_with_sequence(0, "old", 3);
        old.pipeline_instance_id = Arc::from("instance-old");
        diagnostics.publish(&pipeline_id, old);
        assert!(matches!(
            subscription.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(record))
                if record.pipeline_instance_id.as_ref() == "instance-old"
                    && record.sequence == 3
        ));

        let rebuilt_instance = Arc::from("instance-new");
        diagnostics.activate_pipeline_instance(&pipeline_id, Arc::clone(&rebuilt_instance));
        let mut rebuilt = record(0, "rebuilt");
        rebuilt.pipeline_instance_id = rebuilt_instance;
        diagnostics.publish(&pipeline_id, rebuilt);

        assert!(matches!(
            subscription.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(record))
                if record.pipeline_instance_id.as_ref() == "instance-new"
        ));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn channel_instances_keep_independent_sequence_namespaces() -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        activate(&diagnostics, &pipeline_id);
        let mut subscription = diagnostics
            .subscribe(pipeline_id.clone(), channel(0))
            .map_err(io::Error::other)?;
        let _attached = subscription.next().await;
        let old = record_with_sequence(0, "old", 7);
        diagnostics.publish(&pipeline_id, old);
        assert!(matches!(
            subscription.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(record))
                if channel_instance_id(&record) == Some(1) && record.sequence == 7
        ));

        let mut replacement = record(0, "replacement");
        replacement.source = lua_source(0, 2);
        diagnostics.publish(&pipeline_id, replacement);
        assert!(matches!(
            subscription.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(record))
                if channel_instance_id(&record) == Some(2) && record.sequence == 0
        ));

        let late_old = record_with_sequence(0, "late-old", 8);
        diagnostics.publish(&pipeline_id, late_old);
        assert!(matches!(
            subscription.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(record))
                if channel_instance_id(&record) == Some(1) && record.sequence == 8
        ));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_subscriber_keeps_the_next_real_record_after_a_dropped_record() -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        activate(&diagnostics, &pipeline_id);
        let mut slow = diagnostics
            .subscribe(pipeline_id.clone(), channel(0))
            .map_err(io::Error::other)?;
        let mut fast = diagnostics
            .subscribe(pipeline_id.clone(), channel(0))
            .map_err(io::Error::other)?;
        let _slow_attached = slow.next().await;
        let _fast_attached = fast.next().await;

        for sequence in 0..=SUBSCRIBER_QUEUE_CAPACITY {
            diagnostics.publish(
                &pipeline_id,
                record_with_sequence(0, &sequence.to_string(), sequence as u64),
            );
            let Some(RunnerDiagnosticFrame::Diagnostic(_)) = fast.next().await else {
                return Err(io::Error::other(
                    "fast subscriber did not receive a diagnostic record",
                ));
            };
        }
        let _first_slow_record = slow.next().await;
        diagnostics.publish(
            &pipeline_id,
            record_with_sequence(0, "after-loss", (SUBSCRIBER_QUEUE_CAPACITY + 1) as u64),
        );
        assert!(matches!(
            fast.next().await,
            Some(RunnerDiagnosticFrame::Diagnostic(_))
        ));

        let mut sequences = Vec::new();
        while let Ok(frame) = slow.receiver.try_recv() {
            let RunnerDiagnosticFrame::Diagnostic(record) = frame else {
                return Err(io::Error::other(
                    "slow subscriber received an unexpected frame",
                ));
            };
            sequences.push(record.sequence);
        }
        assert_eq!(sequences.first(), Some(&1));
        assert_eq!(
            sequences.last(),
            Some(&((SUBSCRIBER_QUEUE_CAPACITY + 1) as u64))
        );
        assert!(!sequences.contains(&(SUBSCRIBER_QUEUE_CAPACITY as u64)));
        Ok(())
    }

    #[test]
    fn exact_process_retirement_releases_only_an_idle_matching_document() -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        diagnostics.activate_pipeline_instance(&pipeline_id, Arc::from("old"));
        diagnostics.activate_pipeline_instance(&pipeline_id, Arc::from("new"));

        diagnostics.deactivate_pipeline_instance(&pipeline_id, "old");
        assert_eq!(
            diagnostics
                .lock()
                .documents
                .get(&pipeline_id)
                .and_then(|document| document.active_pipeline.as_ref())
                .map(AsRef::as_ref),
            Some("new")
        );

        let mut subscription = diagnostics
            .subscribe(pipeline_id.clone(), channel(0))
            .map_err(io::Error::other)?;
        let _attached = subscription.receiver.try_recv().map_err(io::Error::other)?;
        diagnostics.deactivate_pipeline_instance(&pipeline_id, "new");
        assert!(diagnostics.lock().documents.contains_key(&pipeline_id));
        let mut retired = record(0, "retired");
        retired.pipeline_instance_id = Arc::from("new");
        diagnostics.publish(&pipeline_id, retired);
        assert!(subscription.receiver.try_recv().is_err());
        drop(subscription);
        assert!(!diagnostics.lock().documents.contains_key(&pipeline_id));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_clears_interest_and_ends_subscriptions() -> io::Result<()> {
        let diagnostics = RunnerDiagnostics::new();
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        let mut interest = diagnostics.interest(&pipeline_id);
        let mut subscription = diagnostics
            .subscribe(pipeline_id.clone(), channel(3))
            .map_err(io::Error::other)?;
        let _attached = subscription.next().await;
        interest.changed().await.map_err(io::Error::other)?;
        assert_eq!(interest.borrow_and_update().as_ref(), [channel(3)]);

        diagnostics.close();

        interest.changed().await.map_err(io::Error::other)?;
        assert!(interest.borrow_and_update().is_empty());
        assert!(matches!(
            subscription.next().await,
            Some(RunnerDiagnosticFrame::Closed)
        ));
        assert!(subscription.next().await.is_none());
        assert!(diagnostics.subscribe(pipeline_id, channel(3)).is_err());
        Ok(())
    }

    fn record(channel_index: u32, text: &str) -> RunnerDiagnostic {
        record_with_sequence(channel_index, text, 0)
    }

    fn activate(diagnostics: &RunnerDiagnostics, pipeline_id: &TenonDocumentId) {
        diagnostics.activate_pipeline_instance(pipeline_id, Arc::from("instance-a"));
    }

    fn record_with_sequence(channel_index: u32, text: &str, sequence: u64) -> RunnerDiagnostic {
        RunnerDiagnostic {
            pipeline_instance_id: Arc::from("instance-a"),
            source: lua_source(channel_index, 1),
            observed_at_unix_millis: 2,
            text: text.to_owned().into_boxed_str(),
            truncated: false,
            invalid_utf8: false,
            sequence,
        }
    }

    fn source_record(text: &str) -> RunnerDiagnostic {
        RunnerDiagnostic {
            pipeline_instance_id: Arc::from("instance-a"),
            source: RunnerDiagnosticSource::Plugin {
                plugin_instance_id: source_id(),
                plugin_process_instance_id: 1,
                stream: RunnerPluginDiagnosticStream::Stdout,
            },
            observed_at_unix_millis: 2,
            text: text.to_owned().into_boxed_str(),
            truncated: false,
            invalid_utf8: false,
            sequence: 0,
        }
    }

    fn sink_record(sink_id: PluginInstanceId, text: &str) -> RunnerDiagnostic {
        RunnerDiagnostic {
            pipeline_instance_id: Arc::from("instance-a"),
            source: RunnerDiagnosticSource::Plugin {
                plugin_instance_id: sink_id,
                plugin_process_instance_id: 1,
                stream: RunnerPluginDiagnosticStream::Stderr,
            },
            observed_at_unix_millis: 2,
            text: text.to_owned().into_boxed_str(),
            truncated: false,
            invalid_utf8: false,
            sequence: 0,
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "the fixture identity is a valid literal"
    )]
    fn source_id() -> PluginInstanceId {
        PluginInstanceId::try_from("source").expect("valid fixture Instance id")
    }

    #[allow(
        clippy::expect_used,
        reason = "the fixture identity is a valid literal"
    )]
    fn flow_id() -> FlowId {
        FlowId::try_from(String::from("main")).expect("valid fixture Flow id")
    }

    fn channel(channel_index: u32) -> RunnerDiagnosticTarget {
        RunnerDiagnosticTarget::FlowChannel {
            flow_id: flow_id(),
            channel_index,
        }
    }

    fn lua_source(channel_index: u32, channel_instance_id: u64) -> RunnerDiagnosticSource {
        RunnerDiagnosticSource::FlowChannel {
            flow_id: flow_id(),
            channel_index,
            channel_instance_id,
            lua_vm_instance_id: 1,
            kind: RunnerChannelDiagnosticKind::Print,
            phase: None,
            code: None,
        }
    }

    fn channel_instance_id(record: &RunnerDiagnostic) -> Option<u64> {
        match record.source {
            RunnerDiagnosticSource::FlowChannel {
                channel_instance_id,
                ..
            } => Some(channel_instance_id),
            RunnerDiagnosticSource::Plugin { .. } => None,
        }
    }
}
