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

//! Shared HTTP response and request-boundary helpers.

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::http::header::{CONTENT_TYPE, ETAG, LOCATION};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::ToSchema;

pub(super) const JSON: &str = "application/json";
pub(super) const JSONC: &str = "application/jsonc";
pub(super) const SCHEMA_JSON: &str = "application/schema+json";

/// The single shape of every Runner error response body.
#[derive(Serialize, ToSchema)]
pub(super) struct ErrorEnvelope {
    pub(super) error: ErrorBody,
}

#[derive(Serialize, ToSchema)]
pub(super) struct ErrorBody {
    pub(super) code: &'static str,
    pub(super) message: String,
    // Present only when an operation aggregates per-field validation issues.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) issues: Vec<ValidationIssue>,
}

#[derive(Serialize, ToSchema)]
pub(super) struct ValidationIssue {
    pub(super) code: &'static str,
    pub(super) path: String,
    pub(super) message: &'static str,
}

pub(super) struct RunnerHttpResponse {
    status: StatusCode,
    content_type: Option<&'static str>,
    etag: Option<String>,
    location: Option<String>,
    body: Vec<u8>,
}

impl RunnerHttpResponse {
    #[must_use]
    pub(super) fn empty(status: StatusCode) -> Self {
        Self {
            status,
            content_type: None,
            etag: None,
            location: None,
            body: Vec::new(),
        }
    }

    /// Serializes a typed response body that owns the single source of its shape.
    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "Runner response bodies contain only string, integer, bool, option, and sequence fields, which serialize infallibly"
    )]
    pub(super) fn serialized<T: Serialize>(status: StatusCode, body: &T) -> Self {
        Self {
            status,
            content_type: Some(JSON),
            etag: None,
            location: None,
            body: serde_json::to_vec(body).expect("a Runner response body must serialize"),
        }
    }

    #[must_use]
    pub(super) fn bytes(status: StatusCode, content_type: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: Some(content_type),
            etag: None,
            location: None,
            body,
        }
    }

    #[must_use]
    pub(super) fn error(status: StatusCode, code: &'static str, message: &str) -> Self {
        Self::serialized(
            status,
            &ErrorEnvelope {
                error: ErrorBody {
                    code,
                    message: message.to_owned(),
                    issues: Vec::new(),
                },
            },
        )
    }

    #[must_use]
    pub(super) fn with_etag(mut self, etag: String) -> Self {
        self.etag = Some(etag);
        self
    }

    #[must_use]
    pub(super) fn with_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }
}

impl IntoResponse for RunnerHttpResponse {
    #[allow(
        clippy::expect_used,
        reason = "Runner emits only canonical ETags and validated Plugin location characters"
    )]
    fn into_response(self) -> Response {
        let mut response = (self.status, self.body).into_response();
        let headers = response.headers_mut();
        match self.content_type {
            Some(content_type) => {
                headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
            }
            None => {
                headers.remove(CONTENT_TYPE);
            }
        }
        if let Some(etag) = self.etag {
            headers.insert(
                ETAG,
                HeaderValue::try_from(etag).expect("a canonical ETag must be a valid HTTP header"),
            );
        }
        if let Some(location) = self.location {
            headers.insert(
                LOCATION,
                HeaderValue::try_from(location)
                    .expect("a canonical Plugin location must be a valid HTTP header"),
            );
        }
        response
    }
}

pub(super) fn has_content_type(headers: &HeaderMap, expected: &str) -> bool {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let value = values.next();
    if values.next().is_some() {
        return false;
    }
    value
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(expected))
}

pub(super) fn request_body(
    body: Result<Bytes, BytesRejection>,
) -> Result<Bytes, RunnerHttpResponse> {
    body.map_err(|_| {
        RunnerHttpResponse::error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Runner could not complete the request",
        )
    })
}

pub(super) fn bad_path() -> Response {
    RunnerHttpResponse::error(
        StatusCode::BAD_REQUEST,
        "invalid_path",
        "Request path is invalid",
    )
    .into_response()
}

pub(super) fn invalid_precondition() -> Response {
    RunnerHttpResponse::error(
        StatusCode::BAD_REQUEST,
        "invalid_precondition",
        "Conditional request headers are invalid",
    )
    .into_response()
}

pub(super) async fn route_not_found() -> Response {
    RunnerHttpResponse::error(
        StatusCode::NOT_FOUND,
        "route_not_found",
        "Runner API route was not found",
    )
    .into_response()
}

pub(super) fn unsupported_media_type() -> Response {
    RunnerHttpResponse::error(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
        "Request Content-Type is unsupported",
    )
    .into_response()
}

pub(super) fn service_unavailable() -> Response {
    RunnerHttpResponse::error(
        StatusCode::SERVICE_UNAVAILABLE,
        "runner_shutting_down",
        "Runner is shutting down",
    )
    .into_response()
}
