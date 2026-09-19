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

//! Pipeline status HTTP transport handlers.

use super::app::HttpServices;
use super::response::{ErrorEnvelope, RunnerHttpResponse, bad_path, service_unavailable};
use crate::contracts::core::PluginInstanceState;
use crate::identifiers::TenonDocumentId;
use crate::runner::management::{
    PipelineConvergence, PipelineStatus, PluginInstanceView, ProcessErrorView,
};
use crate::runner::pipeline::RuntimeResolutionIssue;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::ToSchema;

/// The list response: one summary per Pipeline, sorted by Document id.
#[derive(Serialize, ToSchema)]
struct PipelineListBody {
    pipelines: Vec<PipelineStatusBody>,
}

/// The shared Pipeline summary carried by both the list and the detail response.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct PipelineStatusBody {
    id: String,
    document_etag: String,
    state: &'static str,
    // Present only once a live Pipeline has published a complete applied state.
    #[serde(skip_serializing_if = "Option::is_none")]
    applied_document_etag: Option<String>,
}

/// The single-Pipeline detail response: the summary plus runtime context.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct PipelineDetailBody {
    #[serde(flatten)]
    status: PipelineStatusBody,
    // Non-empty only while the latest desired Document is unready.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    runtime_issues: Vec<RuntimeResolutionIssue>,
    // Present (possibly empty) only while an applied state exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_instances: Option<Vec<PluginInstanceBody>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_limits: Option<ResourceLimitsBody>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<PipelineAttemptErrorBody>,
}

#[derive(Serialize, ToSchema)]
struct ResourceLimitsBody {
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct PipelineAttemptErrorBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    document_etag: Option<String>,
    code: &'static str,
    message: &'static str,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct PluginInstanceBody {
    id: String,
    program_name: String,
    exact_version: String,
    state: &'static str,
    // Always present; null when the Instance has no redacted error.
    last_error: Option<ProcessErrorBody>,
}

#[derive(Serialize, ToSchema)]
struct ProcessErrorBody {
    code: String,
    message: String,
}

#[utoipa::path(
    get,
    path = "/pipelines",
    tag = "pipelines",
    responses(
        (status = 200, description = "Pipeline status summaries, sorted by id", body = PipelineListBody),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
pub(super) async fn list_pipelines(State(state): State<HttpServices>) -> Response {
    let Ok(pipelines) = state.management.list_pipelines().await else {
        return service_unavailable();
    };
    let mut pipelines = pipelines.into_vec();
    pipelines.sort_unstable_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
    let body = PipelineListBody {
        pipelines: pipelines.iter().map(pipeline_status_body).collect(),
    };
    RunnerHttpResponse::serialized(StatusCode::OK, &body).into_response()
}

#[utoipa::path(
    get,
    path = "/pipelines/{id}",
    tag = "pipelines",
    params(("id" = String, Path, description = "Pipeline (Document) id")),
    responses(
        (status = 200, description = "Full Pipeline status with runtime issues and plugin instances", body = PipelineDetailBody),
        (status = 400, description = "The request path is invalid", body = ErrorEnvelope),
        (status = 404, description = "The Pipeline does not exist", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
pub(super) async fn get_pipeline(
    State(state): State<HttpServices>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let Ok(Path(id)) = path else {
        return bad_path();
    };
    let Ok(id) = TenonDocumentId::try_from(id) else {
        return bad_path();
    };
    let Ok(pipeline) = state.management.pipeline(id).await else {
        return service_unavailable();
    };
    let Some(pipeline) = pipeline else {
        return RunnerHttpResponse::error(
            StatusCode::NOT_FOUND,
            "pipeline_not_found",
            "Pipeline was not found",
        )
        .into_response();
    };
    let status = pipeline_status_body(&pipeline.status);
    let plugin_instances = pipeline
        .status
        .convergence
        .applied_document_etag()
        .is_some()
        .then(|| {
            pipeline
                .plugin_instances
                .iter()
                .map(plugin_instance_body)
                .collect()
        });
    let body = PipelineDetailBody {
        status,
        runtime_issues: pipeline.runtime_issues.into_vec(),
        plugin_instances,
        resource_limits: pipeline.resource_limits.map(|limits| ResourceLimitsBody {
            state: limits.name(),
            reason: limits.reason(),
        }),
        last_error: pipeline.last_error.map(|error| PipelineAttemptErrorBody {
            document_etag: error.document_etag.map(|etag| etag.strong_value()),
            code: error.code,
            message: error.message,
        }),
    };
    RunnerHttpResponse::serialized(StatusCode::OK, &body).into_response()
}

fn pipeline_status_body(pipeline: &PipelineStatus) -> PipelineStatusBody {
    PipelineStatusBody {
        id: pipeline.id.as_str().to_owned(),
        document_etag: pipeline.document_etag.strong_value(),
        state: convergence_state(&pipeline.convergence),
        applied_document_etag: pipeline
            .convergence
            .applied_document_etag()
            .map(|applied| applied.strong_value()),
    }
}

fn plugin_instance_body(instance: &PluginInstanceView) -> PluginInstanceBody {
    PluginInstanceBody {
        id: instance.id.as_str().to_owned(),
        program_name: instance.program_name.as_str().to_owned(),
        exact_version: instance.exact_version.as_str().to_owned(),
        state: instance_state_name(instance.state),
        last_error: instance.last_error.as_ref().map(process_error_body),
    }
}

fn process_error_body(error: &ProcessErrorView) -> ProcessErrorBody {
    ProcessErrorBody {
        code: error.code.as_ref().to_owned(),
        message: error.message.as_ref().to_owned(),
    }
}

const fn convergence_state(convergence: &PipelineConvergence) -> &'static str {
    match convergence {
        PipelineConvergence::Unready { .. } => "unready",
        PipelineConvergence::Starting => "starting",
        PipelineConvergence::Updating { .. } => "updating",
        PipelineConvergence::Running { .. } => "running",
        PipelineConvergence::RestartBackoff => "restart-backoff",
    }
}

const fn instance_state_name(state: PluginInstanceState) -> &'static str {
    match state {
        PluginInstanceState::Starting => "starting",
        PluginInstanceState::Running => "running",
        PluginInstanceState::StartFailed => "start-failed",
        PluginInstanceState::RestartBackoff => "restart-backoff",
    }
}
