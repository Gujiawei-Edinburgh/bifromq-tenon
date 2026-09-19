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

//! Publishes observation snapshots without querying Programs or constructing VMs.

use super::{PipelineConvergence, PipelineStatus};
use crate::identifiers::TenonDocumentId;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Meter;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(super) struct ManagementMetrics(Arc<Mutex<HashMap<TenonDocumentId, Observation>>>);

impl ManagementMetrics {
    #[allow(
        clippy::expect_used,
        reason = "the collector and publisher run synchronously on the owning current-thread runtime"
    )]
    pub(super) fn new(meter: &Meter) -> Self {
        let observations = Arc::new(Mutex::new(HashMap::<TenonDocumentId, Observation>::new()));
        let states = Arc::clone(&observations);
        meter
            .u64_observable_gauge("tenon.pipeline.state")
            .with_unit("1")
            .with_callback(move |observer| {
                let states = states.lock().expect("management metrics snapshot poisoned");
                for (id, value) in states.iter() {
                    observer.observe(
                        value.state as u64,
                        &[KeyValue::new("tenon.pipeline.id", id.as_str().to_owned())],
                    );
                }
            })
            .build();
        let applied = Arc::clone(&observations);
        meter
            .u64_observable_gauge("tenon.pipeline.configuration.applied")
            .with_unit("1")
            .with_callback(move |observer| {
                let states = applied
                    .lock()
                    .expect("management metrics snapshot poisoned");
                for (id, value) in states.iter() {
                    observer.observe(
                        u64::from(value.applied),
                        &[KeyValue::new("tenon.pipeline.id", id.as_str().to_owned())],
                    );
                }
            })
            .build();
        Self(observations)
    }

    #[allow(
        clippy::expect_used,
        reason = "this snapshot lock never spans user code or await"
    )]
    pub(super) fn record(&self, status: PipelineStatus) {
        self.0
            .lock()
            .expect("management metrics snapshot poisoned")
            .insert(status.id.clone(), observe(status));
    }

    #[allow(
        clippy::expect_used,
        reason = "current lifecycle updates retain their reconciled observation"
    )]
    pub(super) fn refresh(
        &self,
        id: &TenonDocumentId,
        project: impl FnOnce(bool) -> PipelineStatus,
    ) {
        let mut observations = self.0.lock().expect("management metrics snapshot poisoned");
        let observation = observations
            .get_mut(id)
            .expect("current lifecycle retains its metrics observation");
        *observation = observe(project(matches!(
            observation.state,
            PipelineMetricState::Unready
        )));
    }

    #[allow(
        clippy::expect_used,
        reason = "this snapshot lock never spans user code or await"
    )]
    pub(super) fn remove(&self, id: &TenonDocumentId) {
        self.0
            .lock()
            .expect("management metrics snapshot poisoned")
            .remove(id);
    }
}

impl Drop for ManagementMetrics {
    #[allow(
        clippy::expect_used,
        reason = "the observation lock never spans user code or await"
    )]
    fn drop(&mut self) {
        self.0
            .lock()
            .expect("management metrics snapshot poisoned")
            .clear();
    }
}

// These values freeze the latest published management observation. They never
// participate in reconciliation, process admission, or persistence decisions.
struct Observation {
    state: PipelineMetricState,
    applied: bool,
}

#[repr(u64)]
#[derive(Clone, Copy)]
enum PipelineMetricState {
    Unready = 0,
    Starting = 1,
    Updating = 2,
    Running = 3,
    RestartBackoff = 4,
}

fn observe(status: PipelineStatus) -> Observation {
    let (state, applied) = match status.convergence {
        PipelineConvergence::Unready { applied, .. } => (PipelineMetricState::Unready, applied),
        PipelineConvergence::Starting => (PipelineMetricState::Starting, None),
        PipelineConvergence::Updating { applied } => (PipelineMetricState::Updating, Some(applied)),
        PipelineConvergence::Running { applied } => (PipelineMetricState::Running, Some(applied)),
        PipelineConvergence::RestartBackoff => (PipelineMetricState::RestartBackoff, None),
    };
    Observation {
        state,
        applied: applied.is_some_and(|applied| applied == status.document_etag),
    }
}
