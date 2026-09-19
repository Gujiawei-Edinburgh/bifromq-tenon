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
use opentelemetry_proto::tonic::common::v1::any_value;
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};
use std::io;

mod resources;

#[test]
fn process_identity_is_stable_and_follows_its_source() -> io::Result<()> {
    let mut identities = Vec::new();
    for process in [
        CoreProcess::Runner,
        CoreProcess::Runner,
        CoreProcess::Pipeline {
            document_id: "document",
            launch_id: b"first-launch",
        },
        CoreProcess::Pipeline {
            document_id: "document",
            launch_id: b"second-launch",
        },
    ] {
        let runtime =
            MetricsRuntime::start(Some("test-node"), process).map_err(io::Error::other)?;
        let mut samples = Vec::new();
        for _ in 0..2 {
            let export = runtime.collect(&[]);
            let identity = export
                .resource_metrics
                .iter()
                .filter_map(|metrics| metrics.resource.as_ref())
                .flat_map(|resource| &resource.attributes)
                .find(|attribute| attribute.key == "service.instance.id")
                .and_then(|attribute| attribute.value.as_ref())
                .and_then(|value| value.value.as_ref());
            let Some(any_value::Value::StringValue(identity)) = identity else {
                return Err(io::Error::other("missing process identity"));
            };
            samples.push(identity.clone());
        }
        assert_eq!(samples[0], samples[1]);
        identities.push(samples.remove(0));
        runtime.shutdown();
    }
    assert_ne!(identities[0], identities[1]);
    for identity in &identities[..2] {
        assert_eq!(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(identity)
                .map_err(io::Error::other)?
                .len(),
            16
        );
    }
    assert_eq!(
        identities[2],
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"first-launch")
    );
    assert_eq!(
        identities[3],
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"second-launch")
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn time_and_shutdown_never_collect_without_a_request() -> io::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let runtime = MetricsRuntime::start(None, CoreProcess::Runner).map_err(io::Error::other)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    runtime
        .meter()
        .u64_observable_gauge("tenon.pipeline.state")
        .with_callback(move |observer| {
            observed.fetch_add(1, Ordering::Relaxed);
            observer.observe(1, &[]);
        })
        .build();
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    runtime.collect(&[]);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    runtime.shutdown();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn cardinality_overflow_is_visible_in_cumulative_snapshots() -> io::Result<()> {
    let runtime = MetricsRuntime::start(None, CoreProcess::Runner).map_err(io::Error::other)?;
    let counter = runtime
        .meter()
        .u64_counter("tenon.pipeline.restarts")
        .with_unit("{restart}")
        .build();
    for index in 0..2010 {
        counter.add(
            1,
            &[KeyValue::new(
                "tenon.pipeline.id",
                format!("pipeline-{index}"),
            )],
        );
    }
    let export = runtime.collect(&[]);
    let sum = export
        .resource_metrics
        .iter()
        .flat_map(|resource| &resource.scope_metrics)
        .flat_map(|scope| &scope.metrics)
        .find_map(|metric| match &metric.data {
            Some(metric::Data::Sum(sum)) if metric.name == "tenon.pipeline.restarts" => Some(sum),
            _ => None,
        })
        .ok_or_else(|| io::Error::other("restart counter was not exported"))?;
    assert!(sum.is_monotonic);
    assert_eq!(sum.aggregation_temporality, 2);
    assert_eq!(sum.data_points.len(), 2001);
    let overflow = sum
        .data_points
        .iter()
        .find(|point| {
            point
                .attributes
                .iter()
                .any(|attribute| attribute.key == "otel.metric.overflow")
        })
        .ok_or_else(|| io::Error::other("overflow was not exported"))?;
    assert_eq!(overflow.value, Some(number_data_point::Value::AsInt(10)));
    runtime.shutdown();
    Ok(())
}
