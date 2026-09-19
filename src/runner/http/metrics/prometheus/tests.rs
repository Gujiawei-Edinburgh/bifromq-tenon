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
use opentelemetry_proto::tonic::metrics::v1::Sum;
use std::collections::HashSet;
use std::io;

#[test]
fn catalog_prometheus_names_and_histogram_expansions_do_not_collide() -> io::Result<()> {
    let catalog: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../../contracts/metrics/catalog.json"
    ))?;
    let mut names = HashSet::new();
    for definition in catalog["metrics"]
        .as_array()
        .ok_or_else(|| io::Error::other("Missing catalog"))?
    {
        let metric = Metric {
            name: definition["name"].as_str().unwrap_or_default().to_owned(),
            unit: definition["unit"].as_str().unwrap_or_default().to_owned(),
            data: (definition["kind"] == "counter").then(|| metric::Data::Sum(Sum::default())),
            ..Metric::default()
        };
        let name = name(&metric);
        assert!(names.insert(name.clone()), "{name}");
        if definition["kind"] == "histogram" {
            for suffix in ["count", "sum", "min", "max"] {
                assert!(names.insert(format!("{name}_{suffix}")), "{name}_{suffix}");
            }
        }
    }
    Ok(())
}
