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

//! Publishes the current complete core-owned Plugin lifecycle observation.

use crate::contracts::core::PipelineStatusSnapshot;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Meter;
use std::sync::{Arc, Mutex};

type PluginSnapshot = Box<[(String, u64)]>;

pub(super) struct PluginMetrics(Arc<Mutex<PluginSnapshot>>);

impl PluginMetrics {
    #[allow(
        clippy::expect_used,
        reason = "the collector and publisher run synchronously on the owning current-thread runtime"
    )]
    pub(super) fn new(meter: &Meter) -> Self {
        // This is the last published status snapshot, independent of later updates.
        let snapshot = Arc::new(Mutex::new(Box::<[(String, u64)]>::default()));
        let observed = Arc::clone(&snapshot);
        meter
            .u64_observable_gauge("tenon.plugin.state")
            .with_unit("1")
            .with_callback(move |observer| {
                let snapshot = observed.lock().expect("plugin metrics snapshot poisoned");
                for (id, state) in snapshot.iter() {
                    observer.observe(
                        *state,
                        &[KeyValue::new("tenon.plugin.instance.id", id.clone())],
                    );
                }
            })
            .build();
        Self(snapshot)
    }

    #[allow(
        clippy::expect_used,
        reason = "the snapshot lock only replaces or reads immutable scalar observations"
    )]
    pub(super) fn publish(&self, snapshot: &PipelineStatusSnapshot) {
        let observations = snapshot
            .plugin_instances
            .iter()
            .map(|instance| (instance.id.clone(), instance.state as u64))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        *self.0.lock().expect("plugin metrics snapshot poisoned") = observations;
    }
}

impl Drop for PluginMetrics {
    #[allow(
        clippy::expect_used,
        reason = "the observation lock never spans user code or await"
    )]
    fn drop(&mut self) {
        *self.0.lock().expect("plugin metrics snapshot poisoned") = Box::default();
    }
}
