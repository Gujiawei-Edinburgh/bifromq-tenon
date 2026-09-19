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

//! Reads metric names and process ownership from the authoritative catalog.

use serde::Deserialize;
use std::sync::OnceLock;

#[derive(Deserialize)]
pub(crate) struct Definition {
    pub(crate) name: String,
    owner: String,
}

#[allow(
    clippy::expect_used,
    reason = "the embedded metric catalog is validated with the repository contracts"
)]
pub(crate) fn definitions() -> &'static [Definition] {
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    &CATALOG
        .get_or_init(|| {
            serde_json::from_str(include_str!("../../contracts/metrics/catalog.json"))
                .expect("embedded metrics catalog must be valid")
        })
        .metrics
}

pub(crate) fn includes_owner(include: &[String], owner: &str) -> bool {
    include.is_empty()
        || definitions()
            .iter()
            .any(|metric| metric.owner == owner && include.contains(&metric.name))
}

#[derive(Deserialize)]
struct Catalog {
    metrics: Vec<Definition>,
}
