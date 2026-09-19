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

//! Exposes a fresh weak metrics view using the existing HTTP boundary.

mod json;
mod prometheus;
mod query;

use super::app::HttpServices;
use super::response::{ErrorEnvelope, RunnerHttpResponse};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use json::MetricsResponse;
use query::{Format, MetricsQuery};

const PROMETHEUS: &str = "text/plain; version=0.0.4; charset=utf-8";

#[utoipa::path(
    get,
    path = "/metrics",
    tag = "metrics",
    params(
        ("format" = Option<Format>, Query, description = "Output format; omitted value is json"),
        ("include" = Option<String>, Query, description = "Comma-separated exact original metric names; omitted value selects all"),
    ),
    responses(
        (status = 200, description = "Available process metrics; absent or busy sources are omitted",
            content(
                (MetricsResponse = "application/json"),
                (String = "text/plain; version=0.0.4; charset=utf-8"),
            )),
        (status = 400, description = "Invalid format, names or query parameters", body = ErrorEnvelope),
        (status = 401, description = "Authentication is required", body = ErrorEnvelope),
        (status = 403, description = "Access is forbidden", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
pub(super) async fn get_metrics(
    State(services): State<HttpServices>,
    query: Result<Query<Vec<(String, String)>>, QueryRejection>,
) -> Response {
    let query = query
        .map_err(|_| ())
        .and_then(|Query(pairs)| MetricsQuery::parse(pairs));
    let Ok(query) = query else {
        return RunnerHttpResponse::error(
            StatusCode::BAD_REQUEST,
            "invalid_metrics_query",
            "Metrics query is invalid",
        )
        .into_response();
    };
    let snapshot = services.metrics.collect(&query.include).await;
    let mut response = match query.format {
        Format::Json => {
            RunnerHttpResponse::serialized(StatusCode::OK, &MetricsResponse::from(&snapshot))
        }
        Format::Prometheus => RunnerHttpResponse::bytes(
            StatusCode::OK,
            PROMETHEUS,
            prometheus::render(&snapshot).into_bytes(),
        ),
    }
    .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests;
