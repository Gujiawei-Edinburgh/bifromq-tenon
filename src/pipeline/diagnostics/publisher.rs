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

//! Shared diagnostic producers and their single latest-interest snapshot.
//!
//! Incarnation counters belong to one Pipeline; each producer owns its own
//! sequence. Worker threads never await a transport or subscriber.

use super::publication::{self, DiagnosticLine};
use super::unix_millis;
use super::wire::CurrentDiagnosticInterest;
use crate::contracts::core::{self, PluginDiagnosticStream, bound_diagnostic_text};
use crate::identifiers::{FlowId, PluginInstanceId};
use crate::lua::{LuaPrintCallback, LuaPrintRecord};
use std::borrow::Cow;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Cloneable producer factory shared by the Pipeline's runtime generations.
#[derive(Clone)]
pub(crate) struct PipelineDiagnosticsPublisher {
    pub(super) shared: Arc<PipelineDiagnosticsShared>,
}

impl PipelineDiagnosticsPublisher {
    /// Creates one producer for the exact Flow Channel.
    pub(crate) fn flow_channel(
        &self,
        flow_id: &FlowId,
        channel_index: u32,
    ) -> ChannelDiagnosticPublisher {
        let channel_instance_id = self
            .shared
            .next_channel_instance_id
            .fetch_add(1, Ordering::Relaxed);
        ChannelDiagnosticPublisher {
            inner: Arc::new(ChannelDiagnosticPublisherInner {
                target: ChannelDiagnosticTarget {
                    flow_id: flow_id.clone(),
                    channel_index,
                },
                channel_instance_id,
                next_sequence: AtomicU64::new(0),
                next_lua_vm_instance_id: AtomicU64::new(1),
                shared: Arc::clone(&self.shared),
            }),
        }
    }

    /// Creates the sole stdout/stderr producer for one Plugin Instance incarnation.
    pub(crate) fn instance_plugin(&self, id: PluginInstanceId) -> PluginDiagnosticPublisher {
        let plugin_process_instance_id = self
            .shared
            .next_plugin_process_instance_id
            .fetch_add(1, Ordering::Relaxed);
        PluginDiagnosticPublisher {
            inner: Arc::new(PluginDiagnosticPublisherInner {
                target: id,
                plugin_process_instance_id,
                next_sequence: AtomicU64::new(0),
                shared: Arc::clone(&self.shared),
            }),
        }
    }
}

impl fmt::Debug for PipelineDiagnosticsPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineDiagnosticsPublisher")
            .field("interest", &*self.shared.read_interest())
            .finish_non_exhaustive()
    }
}

/// Nonblocking output owned by one exact Channel thread.
#[derive(Clone)]
pub(crate) struct ChannelDiagnosticPublisher {
    inner: Arc<ChannelDiagnosticPublisherInner>,
}

impl ChannelDiagnosticPublisher {
    /// Creates one exact Lua VM-incarnation producer within this Channel.
    #[must_use]
    pub(crate) fn lua_vm(&self) -> LuaDiagnosticPublisher {
        let lua_vm_instance_id = self
            .inner
            .next_lua_vm_instance_id
            .fetch_add(1, Ordering::Relaxed);
        LuaDiagnosticPublisher {
            channel: Arc::clone(&self.inner),
            lua_vm_instance_id,
        }
    }
}

impl fmt::Debug for ChannelDiagnosticPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChannelDiagnosticPublisher")
            .field("target", &self.inner.target)
            .field("channel_instance_id", &self.inner.channel_instance_id)
            .finish_non_exhaustive()
    }
}

struct ChannelDiagnosticPublisherInner {
    target: ChannelDiagnosticTarget,
    channel_instance_id: u64,
    next_sequence: AtomicU64,
    next_lua_vm_instance_id: AtomicU64,
    shared: Arc<PipelineDiagnosticsShared>,
}

/// Nonblocking output owned by one exact Lua VM incarnation.
#[derive(Clone)]
pub(crate) struct LuaDiagnosticPublisher {
    channel: Arc<ChannelDiagnosticPublisherInner>,
    lua_vm_instance_id: u64,
}

impl LuaDiagnosticPublisher {
    /// Adapts this exact VM producer to the Lua module's lazy print callback.
    #[must_use]
    pub(in crate::pipeline) fn into_print_callback(self) -> LuaPrintCallback {
        Some(Box::new(move |record: LuaPrintRecord| {
            if self.is_enabled() {
                let (text, truncated, invalid_utf8) = record.render()?;
                self.publish_print(text, truncated, invalid_utf8);
            }
            Ok(())
        }))
    }

    /// Reports whether at least one current subscriber selected this Channel.
    #[must_use]
    pub(crate) fn is_enabled(&self) -> bool {
        let interest = self.channel.shared.read_interest();
        interest
            .flow_channels
            .get(self.channel.target.flow_id.as_str())
            .is_some_and(|channels| channels.contains(&self.channel.target.channel_index))
    }

    /// Attempts one bounded `print` publication after the caller observed interest.
    pub(crate) fn publish_print(&self, text: Box<str>, truncated: bool, invalid_utf8: bool) {
        self.emit(
            core::ChannelDiagnosticKind::Print,
            "",
            "",
            text,
            truncated,
            invalid_utf8,
        );
    }

    /// Attempts one bounded runtime-failure publication under current interest.
    ///
    /// `phase` and `code` are the same stable identity this Channel records on
    /// its error metrics. `detail` is advisory text: it is rendered only while
    /// at least one subscriber selected this Channel, it is bounded to one
    /// diagnostic record, and it never carries Rust error chains, tracebacks,
    /// or internal paths.
    pub(crate) fn publish_error<'a>(
        &self,
        phase: &'static str,
        code: &'static str,
        detail: impl FnOnce() -> Cow<'a, str>,
    ) {
        if !self.is_enabled() {
            return;
        }
        let (text, truncated) = bound_diagnostic_text(&detail());
        self.emit(
            core::ChannelDiagnosticKind::Error,
            phase,
            code,
            text,
            truncated,
            false,
        );
    }

    /// Attempts one bounded publication of an already rendered Channel line.
    fn emit(
        &self,
        kind: core::ChannelDiagnosticKind,
        phase: &'static str,
        code: &'static str,
        text: Box<str>,
        truncated: bool,
        invalid_utf8: bool,
    ) {
        let channel = &self.channel;
        let Some(observed_at_unix_millis) = unix_millis() else {
            return;
        };
        let sequence = channel.next_sequence.fetch_add(1, Ordering::Relaxed);
        publication::channel(
            &channel.shared.events,
            &channel.target,
            channel.channel_instance_id,
            self.lua_vm_instance_id,
            publication::ChannelDiagnosticLine {
                kind,
                phase,
                code,
                line: DiagnosticLine {
                    observed_at_unix_millis,
                    text,
                    truncated,
                    invalid_utf8,
                    sequence,
                },
            },
        );
    }
}

impl fmt::Debug for LuaDiagnosticPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LuaDiagnosticPublisher")
            .field("target", &self.channel.target)
            .field("channel_instance_id", &self.channel.channel_instance_id)
            .field("lua_vm_instance_id", &self.lua_vm_instance_id)
            .finish_non_exhaustive()
    }
}

/// Nonblocking output owned by one exact Plugin process incarnation.
#[derive(Clone)]
pub(crate) struct PluginDiagnosticPublisher {
    inner: Arc<PluginDiagnosticPublisherInner>,
}

impl PluginDiagnosticPublisher {
    /// Reports whether at least one current subscriber selected this Plugin.
    #[must_use]
    pub(crate) fn is_enabled(&self) -> bool {
        let interest = self.inner.shared.read_interest();
        interest
            .plugin_instances
            .contains(self.inner.target.as_str())
    }

    /// Attempts one bounded publication after a complete output line was observed.
    pub(crate) fn publish(
        &self,
        stream: PluginDiagnosticStream,
        text: Box<str>,
        truncated: bool,
        invalid_utf8: bool,
    ) {
        let Some(observed_at_unix_millis) = unix_millis() else {
            return;
        };
        let sequence = self.inner.next_sequence.fetch_add(1, Ordering::Relaxed);
        publication::plugin(
            &self.inner.shared.events,
            &self.inner.target,
            self.inner.plugin_process_instance_id,
            stream,
            DiagnosticLine {
                observed_at_unix_millis,
                text,
                truncated,
                invalid_utf8,
                sequence,
            },
        );
    }
}

impl fmt::Debug for PluginDiagnosticPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginDiagnosticPublisher")
            .field("target", &self.inner.target)
            .field(
                "plugin_process_instance_id",
                &self.inner.plugin_process_instance_id,
            )
            .finish_non_exhaustive()
    }
}

struct PluginDiagnosticPublisherInner {
    target: PluginInstanceId,
    plugin_process_instance_id: u64,
    next_sequence: AtomicU64,
    shared: Arc<PipelineDiagnosticsShared>,
}

pub(super) struct PipelineDiagnosticsShared {
    pub(super) interest: RwLock<CurrentDiagnosticInterest>,
    pub(super) events: tokio::sync::mpsc::Sender<crate::contracts::core::PipelineDiagnosticRecord>,
    pub(super) next_channel_instance_id: AtomicU64,
    pub(super) next_plugin_process_instance_id: AtomicU64,
}

impl PipelineDiagnosticsShared {
    pub(super) fn replace_interest(&self, next: CurrentDiagnosticInterest) {
        *self.write_interest() = next;
    }

    pub(super) fn clear_interest(&self) {
        *self.write_interest() = CurrentDiagnosticInterest::default();
    }

    #[allow(
        clippy::expect_used,
        reason = "diagnostic interest mutations do not execute panicking user code"
    )]
    fn read_interest(&self) -> RwLockReadGuard<'_, CurrentDiagnosticInterest> {
        self.interest
            .read()
            .expect("Pipeline diagnostic interest lock must not be poisoned")
    }

    #[allow(
        clippy::expect_used,
        reason = "diagnostic interest mutations do not execute panicking user code"
    )]
    fn write_interest(&self) -> RwLockWriteGuard<'_, CurrentDiagnosticInterest> {
        self.interest
            .write()
            .expect("Pipeline diagnostic interest lock must not be poisoned")
    }
}

#[derive(Debug)]
pub(super) struct ChannelDiagnosticTarget {
    pub(super) flow_id: FlowId,
    pub(super) channel_index: u32,
}

#[cfg(test)]
mod tests {
    use super::super::test_support;
    use std::io;

    use super::*;
    use crate::contracts::core::PipelineDiagnosticRecord;
    use crate::contracts::core::{
        ChannelDiagnosticRecord, PluginDiagnosticRecord, pipeline_diagnostic_record,
    };
    use std::collections::HashSet;

    #[test]
    fn full_instance_queue_preserves_identity_and_exposes_a_sequence_gap() -> io::Result<()> {
        use crate::contracts::core::pipeline_diagnostic_record::Record;
        let id = PluginInstanceId::try_from(String::from("device/a")).map_err(io::Error::other)?;
        let (publisher, mut records) = test_support::interested_instance(id);
        publisher.publish(PluginDiagnosticStream::Stdout, "first".into(), false, false);
        publisher.publish(
            PluginDiagnosticStream::Stderr,
            "dropped".into(),
            false,
            false,
        );
        let first = records.try_recv().map_err(io::Error::other)?;
        assert!(
            matches!(first.record, Some(Record::Plugin(record)) if record.plugin_instance_id == "device/a" && record.sequence == 0 && record.stream == 0)
        );
        publisher.publish(PluginDiagnosticStream::Stderr, "third".into(), false, false);
        let third = records.try_recv().map_err(io::Error::other)?;
        assert!(
            matches!(third.record, Some(Record::Plugin(record)) if record.plugin_instance_id == "device/a" && record.sequence == 2 && record.stream == 1)
        );
        Ok(())
    }

    #[test]
    fn full_queue_leaves_a_detectable_sequence_gap() -> io::Result<()> {
        let (events, mut receiver) = tokio::sync::mpsc::channel(1);
        let shared = Arc::new(PipelineDiagnosticsShared {
            interest: RwLock::new(CurrentDiagnosticInterest {
                flow_channels: std::collections::HashMap::from([(
                    String::from("main"),
                    HashSet::from([0]),
                )]),
                ..CurrentDiagnosticInterest::default()
            }),
            events,
            next_channel_instance_id: AtomicU64::new(1),
            next_plugin_process_instance_id: AtomicU64::new(1),
        });
        let publisher = PipelineDiagnosticsPublisher { shared };
        let diagnostics = publisher.flow_channel(&test_support::flow_id(), 0).lua_vm();

        diagnostics.publish_print(String::from("first").into_boxed_str(), false, false);
        diagnostics.publish_print(String::from("dropped").into_boxed_str(), false, false);
        let first = channel_record(receiver.try_recv().map_err(io::Error::other)?)?;
        assert_eq!(first.text, "first");
        diagnostics.publish_print(String::from("third").into_boxed_str(), false, false);
        let third = channel_record(receiver.try_recv().map_err(io::Error::other)?)?;
        assert_eq!(third.sequence, 2);
        Ok(())
    }

    #[test]
    fn each_channel_receives_a_new_instance_identity() {
        let (events, _receiver) = tokio::sync::mpsc::channel(1);
        let publisher = PipelineDiagnosticsPublisher {
            shared: Arc::new(PipelineDiagnosticsShared {
                interest: RwLock::new(CurrentDiagnosticInterest::default()),
                events,
                next_channel_instance_id: AtomicU64::new(1),
                next_plugin_process_instance_id: AtomicU64::new(1),
            }),
        };
        let first = publisher.flow_channel(&test_support::flow_id(), 0);
        let second = publisher.flow_channel(&test_support::flow_id(), 0);

        assert_eq!(first.inner.channel_instance_id, 1);
        assert_eq!(second.inner.channel_instance_id, 2);
    }

    #[test]
    fn lua_vm_replacements_have_distinct_identity_and_one_channel_sequence() -> io::Result<()> {
        let (events, mut receiver) = tokio::sync::mpsc::channel(3);
        let publisher = PipelineDiagnosticsPublisher {
            shared: Arc::new(PipelineDiagnosticsShared {
                interest: RwLock::new(CurrentDiagnosticInterest {
                    flow_channels: std::collections::HashMap::from([(
                        String::from("main"),
                        HashSet::from([0]),
                    )]),
                    ..CurrentDiagnosticInterest::default()
                }),
                events,
                next_channel_instance_id: AtomicU64::new(1),
                next_plugin_process_instance_id: AtomicU64::new(1),
            }),
        };
        let channel = publisher.flow_channel(&test_support::flow_id(), 0);
        let first = channel.lua_vm();
        let second = channel.lua_vm();

        assert_eq!(first.lua_vm_instance_id, 1);
        assert_eq!(second.lua_vm_instance_id, 2);
        first.publish_print(String::from("old").into_boxed_str(), false, false);
        second.publish_print(String::from("candidate").into_boxed_str(), false, false);
        first.publish_print(String::from("resumed").into_boxed_str(), false, false);
        let old = channel_record(receiver.try_recv().map_err(io::Error::other)?)?;
        let candidate = channel_record(receiver.try_recv().map_err(io::Error::other)?)?;
        let resumed = channel_record(receiver.try_recv().map_err(io::Error::other)?)?;
        assert_eq!((old.lua_vm_instance_id, old.sequence), (1, 0));
        assert_eq!((candidate.lua_vm_instance_id, candidate.sequence), (2, 1));
        assert_eq!((resumed.lua_vm_instance_id, resumed.sequence), (1, 2));
        Ok(())
    }

    #[test]
    fn plugin_processes_have_distinct_identity_and_share_sequence_between_streams() -> io::Result<()>
    {
        let id = PluginInstanceId::try_from(String::from("source")).map_err(io::Error::other)?;
        let (events, mut receiver) = tokio::sync::mpsc::channel(3);
        let publisher = PipelineDiagnosticsPublisher {
            shared: Arc::new(PipelineDiagnosticsShared {
                interest: RwLock::new(CurrentDiagnosticInterest {
                    plugin_instances: HashSet::from([id.as_str().to_owned()]),
                    ..CurrentDiagnosticInterest::default()
                }),
                events,
                next_channel_instance_id: AtomicU64::new(1),
                next_plugin_process_instance_id: AtomicU64::new(1),
            }),
        };
        let plugin = publisher.instance_plugin(id.clone());

        plugin.publish(
            PluginDiagnosticStream::Stdout,
            String::from("ordinary").into_boxed_str(),
            false,
            false,
        );
        plugin.publish(
            PluginDiagnosticStream::Stderr,
            String::from("exceptional").into_boxed_str(),
            false,
            false,
        );

        let first = plugin_record(receiver.try_recv().map_err(io::Error::other)?)?;
        let second = plugin_record(receiver.try_recv().map_err(io::Error::other)?)?;
        assert_eq!(first.plugin_process_instance_id, 1);
        assert_eq!(second.plugin_process_instance_id, 1);
        assert_eq!(first.sequence, 0);
        assert_eq!(second.sequence, 1);
        assert_eq!(
            first.stream,
            crate::contracts::core::PluginDiagnosticStream::Stdout as i32
        );
        assert_eq!(
            second.stream,
            crate::contracts::core::PluginDiagnosticStream::Stderr as i32
        );

        let replacement = publisher.instance_plugin(id);
        replacement.publish(
            PluginDiagnosticStream::Stdout,
            String::from("replacement").into_boxed_str(),
            false,
            false,
        );
        let replacement = plugin_record(receiver.try_recv().map_err(io::Error::other)?)?;
        assert_eq!(replacement.plugin_process_instance_id, 2);
        assert_eq!(replacement.sequence, 0);
        Ok(())
    }

    fn channel_record(record: PipelineDiagnosticRecord) -> io::Result<ChannelDiagnosticRecord> {
        match record.record {
            Some(pipeline_diagnostic_record::Record::Channel(record)) => Ok(record),
            Some(pipeline_diagnostic_record::Record::Plugin(_)) | None => {
                Err(io::Error::other("Expected one Channel diagnostic record"))
            }
        }
    }

    fn plugin_record(record: PipelineDiagnosticRecord) -> io::Result<PluginDiagnosticRecord> {
        match record.record {
            Some(pipeline_diagnostic_record::Record::Plugin(record)) => Ok(record),
            Some(pipeline_diagnostic_record::Record::Channel(_)) | None => {
                Err(io::Error::other("Expected one Plugin diagnostic record"))
            }
        }
    }
}
