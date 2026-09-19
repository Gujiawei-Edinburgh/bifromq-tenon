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

//! Explicit collection through the production cumulative reader.

#[path = "metric_contract.rs"]
mod metric_contract;

use crate::metrics::{CoreProcess, MetricsRuntime};
use opentelemetry::metrics::Meter;
use opentelemetry_proto::tonic::metrics::v1::{
    HistogramDataPoint, Metric, metric, number_data_point,
};
use std::io;

pub(crate) struct Capture {
    runtime: MetricsRuntime,
}

impl Capture {
    #[expect(
        clippy::expect_used,
        reason = "Pipeline metrics use a supplied identity without randomness"
    )]
    pub(crate) fn new() -> Self {
        Self {
            runtime: MetricsRuntime::start(
                None,
                CoreProcess::Pipeline {
                    document_id: "metrics-test",
                    launch_id: b"metrics-test",
                },
            )
            .expect("Pipeline identity is supplied"),
        }
    }

    pub(crate) fn meter(&self) -> Meter {
        self.runtime.meter()
    }

    pub(crate) fn collect(&self) -> io::Result<Snapshot> {
        let snapshot = self.runtime.collect(&[]);
        for resource in &snapshot.resource_metrics {
            metric_contract::validate(resource);
        }
        Ok(Snapshot(
            snapshot
                .resource_metrics
                .into_iter()
                .flat_map(|resource| resource.scope_metrics)
                .flat_map(|scope| scope.metrics)
                .collect(),
        ))
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.runtime.shutdown();
    }
}

pub(crate) struct Snapshot(Vec<Metric>);

impl Snapshot {
    pub(crate) fn number(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let metric = self.0.iter().find(|metric| metric.name == name)?;
        let points = match &metric.data {
            Some(metric::Data::Gauge(gauge)) => &gauge.data_points,
            Some(metric::Data::Sum(sum)) => &sum.data_points,
            _ => return None,
        };
        points
            .iter()
            .find(|point| matches_labels(&point.attributes, labels))
            .and_then(|point| match point.value {
                Some(number_data_point::Value::AsInt(value)) => Some(value as f64),
                Some(number_data_point::Value::AsDouble(value)) => Some(value),
                None => None,
            })
    }

    pub(crate) fn histogram(
        &self,
        name: &str,
        labels: &[(&str, &str)],
    ) -> Option<&HistogramDataPoint> {
        let metric = self.0.iter().find(|metric| metric.name == name)?;
        let Some(metric::Data::Histogram(histogram)) = &metric.data else {
            return None;
        };
        histogram
            .data_points
            .iter()
            .find(|point| matches_labels(&point.attributes, labels))
    }
}

fn matches_labels(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    labels: &[(&str, &str)],
) -> bool {
    labels.iter().all(|(key, expected)| {
        attributes
            .iter()
            .find(|attribute| attribute.key == *key)
            .and_then(|attribute| attribute.value.as_ref())
            .and_then(|value| value.value.as_ref())
            .is_some_and(|value| match value {
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(value) => {
                    value == expected
                }
                opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(value) => {
                    value.to_string() == *expected
                }
                _ => false,
            })
    })
}
