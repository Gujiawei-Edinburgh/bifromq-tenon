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

//! Queue occupancy instruments. Registrations contain only weak mapping views,
//! and disappear when the owning Channel releases its metric registration.

use crate::metrics::observations::Observations;
use std::sync::Arc;

use opentelemetry::KeyValue;
use opentelemetry::metrics::Meter;

use tenon_ipc::queue::QueueObserver;

#[derive(Debug)]
pub(crate) struct QueueMetrics(Arc<Observations<QueueMetric>>);

impl QueueMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        let queues = Arc::new(Observations::<QueueMetric>::new());
        for (name, select) in [
            (
                "tenon.queue.usage",
                (|sample: (u64, u64)| sample.0) as fn((u64, u64)) -> u64,
            ),
            (
                "tenon.queue.capacity",
                (|sample: (u64, u64)| sample.1) as fn((u64, u64)) -> u64,
            ),
        ] {
            let observed = Arc::clone(&queues);
            meter
                .u64_observable_gauge(name)
                .with_unit("By")
                .with_callback(move |observer| {
                    if let Some(queues) = observed.try_snapshot() {
                        for queue in queues {
                            if let Some(sample) = queue.queue.sample() {
                                observer.observe(select(sample), &queue.attributes);
                            }
                        }
                    }
                })
                .build();
        }
        Self(queues)
    }

    pub(crate) fn register(
        &self,
        attributes: Vec<KeyValue>,
        queue: QueueObserver,
    ) -> Arc<QueueMetric> {
        let registration = Arc::new(QueueMetric { attributes, queue });
        self.0.register(&registration);
        registration
    }
}

#[derive(Debug)]
pub(crate) struct QueueMetric {
    attributes: Vec<KeyValue>,
    queue: QueueObserver,
}
