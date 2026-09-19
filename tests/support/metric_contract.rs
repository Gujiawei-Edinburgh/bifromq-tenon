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

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test contract assertions intentionally fail on invalid catalog or exported data"
)]

//! Checks real OTLP aggregation, attributes and values against the metric catalog.

use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, ResourceMetrics, metric, number_data_point,
};
use serde_json::Value;

pub(crate) fn validate(resource: &ResourceMetrics) {
    let catalog: Value =
        serde_json::from_slice(include_bytes!("../../contracts/metrics/catalog.json"))
            .expect("metric catalog is valid JSON");
    for metric in resource
        .scope_metrics
        .iter()
        .flat_map(|scope| &scope.metrics)
    {
        let definition = catalog["metrics"]
            .as_array()
            .expect("metric catalog has entries")
            .iter()
            .find(|entry| entry["name"] == metric.name)
            .expect("metric is registered in the catalog");
        assert_eq!(definition["unit"], metric.unit, "{} unit", metric.name);
        if let Some(resource) = &resource.resource {
            let service = string_attribute(&resource.attributes, "service.name").unwrap_or("");
            assert_eq!(
                format!("tenon.{}", definition["owner"].as_str().unwrap_or("")),
                service,
                "{} owner",
                metric.name
            );
        }
        let number_points = match &metric.data {
            Some(metric::Data::Gauge(gauge)) => {
                assert_eq!(definition["kind"], "gauge");
                gauge.data_points.as_slice()
            }
            Some(metric::Data::Sum(sum)) => {
                assert_eq!(definition["kind"], "counter");
                assert!(sum.is_monotonic);
                assert_eq!(
                    sum.aggregation_temporality,
                    AggregationTemporality::Cumulative as i32
                );
                sum.data_points.as_slice()
            }
            Some(metric::Data::Histogram(histogram)) => {
                assert_eq!(definition["kind"], "histogram");
                assert_eq!(
                    histogram.aggregation_temporality,
                    AggregationTemporality::Cumulative as i32
                );
                assert_eq!(
                    definition["statistics"],
                    serde_json::json!(["count", "sum", "min", "max"])
                );
                for point in &histogram.data_points {
                    validate_attributes(definition, &point.attributes);
                    assert!(
                        point.explicit_bounds.is_empty(),
                        "histograms must not define buckets"
                    );
                    assert!(
                        point.bucket_counts.is_empty(),
                        "default buckets must not be restored"
                    );
                    assert!(point.count > 0);
                    assert!(point.sum.is_some_and(|sum| sum.is_finite() && sum >= 0.0));
                    assert!(point.min.is_some_and(|min| min.is_finite() && min >= 0.0));
                    assert!(point.max.is_some_and(|max| max.is_finite() && max >= 0.0));
                    assert!(point.min <= point.max);
                    assert!(point.max <= point.sum);
                    assert!(point.start_time_unix_nano <= point.time_unix_nano);
                }
                &[]
            }
            _ => panic!("unregistered metric aggregation: {}", metric.name),
        };
        for point in number_points {
            validate_attributes(definition, &point.attributes);
            let number = match point.value {
                Some(number_data_point::Value::AsInt(value)) => value as f64,
                Some(number_data_point::Value::AsDouble(value)) => value,
                None => panic!("missing metric value"),
            };
            assert!(number.is_finite() && number >= 0.0, "{} value", metric.name);
            if !is_overflow(&point.attributes)
                && let Some(values) = definition["values"].as_object()
            {
                assert!(
                    values.values().any(|value| value.as_f64() == Some(number)),
                    "{} enum",
                    metric.name
                );
            }
            assert!(point.start_time_unix_nano <= point.time_unix_nano);
        }
    }
}

fn validate_attributes(definition: &Value, attributes: &[KeyValue]) {
    if is_overflow(attributes) {
        assert_eq!(attributes.len(), 1);
        return;
    }
    let mut expected: Vec<&str> = definition["attributes"]
        .as_array()
        .expect("metric has attributes")
        .iter()
        .map(|v| v.as_str().expect("attribute name"))
        .collect();
    if let Some(conditions) = definition["conditionalAttributes"].as_array() {
        for condition in conditions {
            let key = condition["when"]["attribute"]
                .as_str()
                .expect("condition key");
            if condition["when"]["values"]
                .as_array()
                .expect("condition values")
                .iter()
                .any(|v| v.as_str() == string_attribute(attributes, key))
            {
                expected.push(
                    condition["name"]
                        .as_str()
                        .expect("conditional attribute name"),
                );
            }
        }
    }
    assert_eq!(
        attributes.len(),
        expected.len(),
        "{} attributes: {attributes:?}",
        definition["name"]
    );
    for key in expected {
        assert_eq!(
            attributes
                .iter()
                .filter(|attribute| attribute.key == key)
                .count(),
            1,
            "missing or repeated attribute {key}"
        );
    }
    if let Some(enums) = definition["attributeValues"].as_object() {
        for (key, values) in enums {
            assert!(
                values
                    .as_array()
                    .expect("attribute enum")
                    .iter()
                    .any(|v| v.as_str() == string_attribute(attributes, key)),
                "unregistered attribute value: {key}"
            );
        }
    }
    if let Some(index) = attributes
        .iter()
        .find(|attribute| attribute.key == "tenon.channel.index")
    {
        assert!(
            matches!(index.value.as_ref().and_then(|value| value.value.as_ref()), Some(any_value::Value::IntValue(value)) if *value >= 0)
        );
    }
}

fn is_overflow(attributes: &[KeyValue]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.key == "otel.metric.overflow"
            && matches!(
                attribute
                    .value
                    .as_ref()
                    .and_then(|value| value.value.as_ref()),
                Some(any_value::Value::BoolValue(true))
            )
    })
}

pub(crate) fn string_attribute<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a str> {
    attributes
        .iter()
        .find(|attribute| attribute.key == key)
        .and_then(|attribute| attribute.value.as_ref())
        .and_then(|value| match &value.value {
            Some(any_value::Value::StringValue(value)) => Some(value.as_str()),
            _ => None,
        })
}
