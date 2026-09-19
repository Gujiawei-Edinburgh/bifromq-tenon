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

//! Assembles HTTP resources, authorization, admission, and response metadata.

use super::lifecycle::HttpRequestLifecycle;
use super::{diagnostics, documents, metrics, pipelines, plugins, response};
use crate::runner;
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::extensions::{HttpApiAuthorization, HttpAuthRejection};
use crate::runner::management::RunnerManagementClient;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use std::sync::Arc;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(Clone)]
pub(super) struct HttpServices {
    pub(super) management: RunnerManagementClient,
    pub(super) diagnostics: RunnerDiagnostics,
    pub(super) metrics: runner::metrics::RunnerMetrics,
}

pub(super) fn router(
    services: HttpServices,
    http_authorization: Arc<dyn HttpApiAuthorization>,
    request_lifecycle: HttpRequestLifecycle,
) -> Router {
    let (api_router, mut api) = OpenApiRouter::new()
        .routes(routes!(metrics::get_metrics))
        .routes(routes!(documents::list_documents))
        .routes(routes!(
            documents::get_document,
            documents::put_document,
            documents::delete_document
        ))
        .routes(routes!(documents::document_schema))
        .routes(routes!(pipelines::list_pipelines))
        .routes(routes!(pipelines::get_pipeline))
        .routes(routes!(diagnostics::stream_diagnostics))
        .routes(routes!(plugins::list_programs, plugins::install_program))
        .routes(routes!(plugins::get_program, plugins::delete_program))
        .routes(routes!(plugins::get_program_config_schema))
        .routes(routes!(plugins::get_program_payload_contract))
        .split_for_parts();
    api.info.title = String::from("Tenon Runner HTTP API");
    api.info.version = String::from(env!("CARGO_PKG_VERSION"));
    #[allow(
        clippy::expect_used,
        reason = "the OpenAPI document is composed of strings and enums that serialize infallibly"
    )]
    let document =
        Bytes::from(serde_json::to_vec(&api).expect("the Runner OpenAPI document must serialize"));

    api_router
        .route(
            "/openapi.json",
            get(move || {
                let document = document.clone();
                async move { openapi_document(document) }
            }),
        )
        .fallback(response::route_not_found)
        .layer(DefaultBodyLimit::disable())
        .layer(middleware::from_fn_with_state(
            http_authorization,
            authorize_request,
        ))
        .layer(middleware::from_fn_with_state(
            request_lifecycle,
            HttpRequestLifecycle::enforce,
        ))
        .layer(middleware::map_response(add_tenon_version))
        .with_state(services)
}

/// Serves the prebuilt OpenAPI description of the current release.
fn openapi_document(document: Bytes) -> Response {
    let mut response = document.into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

async fn authorize_request(
    State(authorization): State<Arc<dyn HttpApiAuthorization>>,
    request: Request,
    next: Next,
) -> Response {
    let rejection = match authorization
        .authorize(request.method(), request.uri().path(), request.headers())
        .await
    {
        Ok(()) => return next.run(request).await,
        Err(rejection) => rejection,
    };
    let (status, code, message, challenge) = match rejection {
        HttpAuthRejection::Unauthorized { challenge } => (
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Authentication is required",
            Some(challenge),
        ),
        HttpAuthRejection::Forbidden { challenge } => (
            StatusCode::FORBIDDEN,
            "forbidden",
            "Access is forbidden",
            challenge,
        ),
    };
    let mut response = response::RunnerHttpResponse::error(status, code, message).into_response();
    if let Some(challenge) = challenge {
        response
            .headers_mut()
            .insert(axum::http::header::WWW_AUTHENTICATE, challenge);
    }
    response
}

async fn add_tenon_version(mut response: Response) -> Response {
    response.headers_mut().insert(
        "tenon-version",
        HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
    );
    response
}

#[cfg(test)]
mod tests;
