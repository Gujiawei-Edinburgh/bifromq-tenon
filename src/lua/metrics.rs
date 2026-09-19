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

//! Lua execution duration and worker-published VM memory snapshots.
//! Collector callbacks never enter Lua; a non-cloneable VM token retires its
//! snapshot when that VM is destroyed. Flow ownership preserves zero during rebuild.

use crate::metrics::observations::Observations;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Histogram, Meter};

#[derive(Debug)]
pub(crate) struct LuaMetrics {
    duration: Histogram<f64>,
    flows: Arc<Observations<FlowMemory>>,
}

impl LuaMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        let flows = Arc::new(Observations::<FlowMemory>::new());
        let observed = Arc::clone(&flows);
        meter
            .u64_observable_gauge("tenon.flow.lua.memory")
            .with_unit("By")
            .with_callback(move |observer| {
                if let Some(flows) = observed.try_snapshot() {
                    for flow in flows {
                        if let Some(vms) = flow.vms.try_snapshot() {
                            let total = vms
                                .iter()
                                .map(|snapshot| snapshot.load(Ordering::Relaxed))
                                .sum();
                            observer.observe(total, &flow.attributes);
                        }
                    }
                }
            })
            .build();
        Self {
            duration: meter
                .f64_histogram("tenon.flow.lua.duration")
                .with_unit("s")
                .with_boundaries(Vec::new())
                .build(),
            flows,
        }
    }

    pub(crate) fn flow(&self, id: &str) -> LuaFlowMetrics {
        let memory = Arc::new(FlowMemory {
            attributes: [KeyValue::new("tenon.flow.id", Arc::<str>::from(id))],
            vms: Observations::new(),
        });
        self.flows.register(&memory);
        LuaFlowMetrics {
            duration: self.duration.clone(),
            memory,
        }
    }
}

#[derive(Debug)]
pub(crate) struct LuaFlowMetrics {
    duration: Histogram<f64>,
    memory: Arc<FlowMemory>,
}

impl LuaFlowMetrics {
    pub(crate) fn vm(&self, attributes: [KeyValue; 2]) -> VmMetrics {
        let snapshot = Arc::new(AtomicU64::new(0));
        self.memory.vms.register(&snapshot);
        VmMetrics {
            duration: self.duration.clone(),
            attributes,
            snapshot,
        }
    }
}

/// This token moves with its actual VM and cannot duplicate a memory observation.
#[derive(Debug)]
pub(crate) struct VmMetrics {
    duration: Histogram<f64>,
    attributes: [KeyValue; 2],
    // The last worker-thread reading is independent of subsequent Lua mutations.
    snapshot: Arc<AtomicU64>,
}

impl VmMetrics {
    pub(crate) fn publish_memory(&self, bytes: usize) {
        self.snapshot.store(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_duration(&self, elapsed: Duration) {
        self.duration
            .record(elapsed.as_secs_f64(), &self.attributes);
    }
}

#[derive(Debug)]
struct FlowMemory {
    attributes: [KeyValue; 1],
    vms: Observations<AtomicU64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::capture_test_support::Capture;
    use std::io;

    #[test]
    fn duration_exports_only_cumulative_summary_statistics_without_default_buckets()
    -> io::Result<()> {
        let captured = Capture::new();
        let metrics = LuaMetrics::new(&captured.meter());
        let flow = metrics.flow("flow");
        let vm = flow.vm([
            KeyValue::new("tenon.flow.id", "flow"),
            KeyValue::new("tenon.channel.index", 0_i64),
        ]);
        for seconds in [1, 3, 2] {
            vm.record_duration(Duration::from_secs(seconds));
        }
        for _ in 0..2 {
            let snapshot = captured.collect()?;
            let point = snapshot
                .histogram("tenon.flow.lua.duration", &[])
                .ok_or_else(|| io::Error::other("duration sample missing"))?;
            assert_eq!(
                (point.count, point.sum, point.min, point.max),
                (3, Some(6.0), Some(1.0), Some(3.0))
            );
        }
        vm.record_duration(Duration::from_secs(4));
        let snapshot = captured.collect()?;
        let point = snapshot
            .histogram("tenon.flow.lua.duration", &[])
            .ok_or_else(|| io::Error::other("duration sample missing"))?;
        assert_eq!(
            (point.count, point.sum, point.min, point.max),
            (4, Some(10.0), Some(1.0), Some(4.0))
        );
        Ok(())
    }
}
