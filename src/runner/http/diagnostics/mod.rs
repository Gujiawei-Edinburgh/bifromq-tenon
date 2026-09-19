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

//! HTTP SSE adapter for Runner live diagnostics.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt as _;

use super::app::HttpServices;
use super::response::{ErrorEnvelope, RunnerHttpResponse, bad_path, service_unavailable};
use crate::identifiers::{FlowId, PluginInstanceId, TenonDocumentId};
use crate::runner::diagnostics::{
    RunnerChannelDiagnosticKind, RunnerDiagnostic, RunnerDiagnosticFrame, RunnerDiagnosticSource,
    RunnerDiagnosticTarget, RunnerPluginDiagnosticStream,
};

const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// The `attached`/`closed` SSE event body: which target a stream is bound to.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AttachmentData {
    pipeline_id: String,
    target: TargetData,
}

/// The identified diagnostics target, discriminated by `kind`.
///
/// A flat struct with optional members (rather than a tagged enum or flatten)
/// keeps the numeric `channelIndex` a plain field: serde_json's
/// `arbitrary_precision` feature mis-serializes numbers inside flattened or
/// internally tagged content. Exactly one member group is set, chosen by the
/// source variant.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetData {
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    flow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_instance_id: Option<String>,
}

/// The `diagnostic` SSE event body: one observed record with its source.
///
/// Flat for the same `arbitrary_precision` reason as [`TargetData`]; the
/// source-specific members are populated together per source variant.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticData {
    pipeline_id: String,
    pipeline_instance_id: String,
    observed_at_unix_millis: u64,
    text: String,
    truncated: bool,
    invalid_utf8: bool,
    sequence: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    flow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel_instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lua_vm_instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_process_instance_id: Option<String>,
    stream: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiagnosticsQuery {
    target: String,
}

#[utoipa::path(
    get,
    path = "/pipelines/{id}/diagnostics",
    tag = "pipelines",
    params(
        ("id" = String, Path, description = "Pipeline (Document) id"),
        ("target" = String, Query, description = "One Flow Channel (flow:{id}/channel:{n}) or Plugin Instance (plugin:{id})"),
    ),
    responses(
        (status = 200, description = "A live Server-Sent Events stream of attached/diagnostic/closed events", content_type = "text/event-stream"),
        (status = 400, description = "Invalid path or diagnostics target", body = ErrorEnvelope),
        (status = 404, description = "The Pipeline does not exist", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
pub(super) async fn stream_diagnostics(
    State(state): State<HttpServices>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<DiagnosticsQuery>, QueryRejection>,
) -> Response {
    let Ok(Path(id)) = path else {
        return bad_path();
    };
    let Ok(pipeline_id) = TenonDocumentId::try_from(id) else {
        return bad_path();
    };
    let Ok(Query(query)) = query else {
        return invalid_diagnostics_target();
    };
    let Some(target) = parse_target(&query.target) else {
        return invalid_diagnostics_target();
    };
    let Ok(pipeline) = state.management.pipeline(pipeline_id.clone()).await else {
        return service_unavailable();
    };
    if pipeline.is_none() {
        return RunnerHttpResponse::error(
            StatusCode::NOT_FOUND,
            "pipeline_not_found",
            "Pipeline was not found",
        )
        .into_response();
    }
    let Ok(subscription) = state
        .diagnostics
        .subscribe(pipeline_id.clone(), target.clone())
    else {
        return service_unavailable();
    };
    let events = subscription
        .map(move |frame| Ok::<Event, Infallible>(sse_event(&pipeline_id, &target, frame)));
    Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(KEEP_ALIVE_INTERVAL)
                .text("keep-alive"),
        )
        .into_response()
}

fn parse_target(value: &str) -> Option<RunnerDiagnosticTarget> {
    if let Some(id) = value.strip_prefix("plugin:") {
        return PluginInstanceId::try_from(id)
            .ok()
            .map(RunnerDiagnosticTarget::Plugin);
    }
    let (flow_id, channel_index) = value.strip_prefix("flow:")?.rsplit_once("/channel:")?;
    if channel_index.is_empty() || !channel_index.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(RunnerDiagnosticTarget::FlowChannel {
        flow_id: FlowId::try_from(flow_id.to_owned()).ok()?,
        channel_index: channel_index.parse().ok()?,
    })
}

fn invalid_diagnostics_target() -> Response {
    RunnerHttpResponse::error(
        StatusCode::BAD_REQUEST,
        "invalid_diagnostics_target",
        "Diagnostics target must identify one Flow Channel or Plugin Instance",
    )
    .into_response()
}

fn sse_event(
    pipeline_id: &TenonDocumentId,
    target: &RunnerDiagnosticTarget,
    frame: RunnerDiagnosticFrame,
) -> Event {
    match frame {
        RunnerDiagnosticFrame::Attached => event(
            "attached",
            &AttachmentData {
                pipeline_id: pipeline_id.as_str().to_owned(),
                target: target_data(target),
            },
        ),
        RunnerDiagnosticFrame::Diagnostic(record) => {
            event("diagnostic", &diagnostic_data(pipeline_id, &record))
        }
        RunnerDiagnosticFrame::Closed => event(
            "closed",
            &AttachmentData {
                pipeline_id: pipeline_id.as_str().to_owned(),
                target: target_data(target),
            },
        ),
    }
}

#[allow(
    clippy::expect_used,
    reason = "diagnostic event bodies contain only strings, integers, bools, and options, which serialize infallibly"
)]
fn event<T: Serialize>(name: &'static str, data: &T) -> Event {
    Event::default()
        .event(name)
        .data(serde_json::to_string(data).expect("a Runner diagnostic event body must serialize"))
}

fn diagnostic_data(pipeline_id: &TenonDocumentId, record: &RunnerDiagnostic) -> DiagnosticData {
    let (
        flow_id,
        channel_index,
        channel_instance_id,
        lua_vm_instance_id,
        plugin_instance_id,
        plugin_process_instance_id,
        stream,
        phase,
        code,
    ) = match &record.source {
        RunnerDiagnosticSource::FlowChannel {
            flow_id,
            channel_index,
            channel_instance_id,
            lua_vm_instance_id,
            kind,
            phase,
            code,
        } => (
            Some(flow_id.as_str().to_owned()),
            Some(*channel_index),
            Some(channel_instance_id.to_string()),
            Some(lua_vm_instance_id.to_string()),
            None,
            None,
            channel_stream(*kind),
            phase.as_ref().map(|phase| phase.to_string()),
            code.as_ref().map(|code| code.to_string()),
        ),
        RunnerDiagnosticSource::Plugin {
            plugin_instance_id,
            plugin_process_instance_id,
            stream,
        } => (
            None,
            None,
            None,
            None,
            Some(plugin_instance_id.as_str().to_owned()),
            Some(plugin_process_instance_id.to_string()),
            plugin_stream(*stream),
            None,
            None,
        ),
    };
    DiagnosticData {
        pipeline_id: pipeline_id.as_str().to_owned(),
        pipeline_instance_id: record.pipeline_instance_id.as_ref().to_owned(),
        observed_at_unix_millis: record.observed_at_unix_millis,
        text: record.text.as_ref().to_owned(),
        truncated: record.truncated,
        invalid_utf8: record.invalid_utf8,
        sequence: record.sequence.to_string(),
        flow_id,
        channel_index,
        channel_instance_id,
        lua_vm_instance_id,
        plugin_instance_id,
        plugin_process_instance_id,
        stream,
        phase,
        code,
    }
}

fn target_data(target: &RunnerDiagnosticTarget) -> TargetData {
    match target {
        RunnerDiagnosticTarget::FlowChannel {
            flow_id,
            channel_index,
        } => TargetData {
            kind: "flow-channel",
            flow_id: Some(flow_id.as_str().to_owned()),
            channel_index: Some(*channel_index),
            plugin_instance_id: None,
        },
        RunnerDiagnosticTarget::Plugin(id) => TargetData {
            kind: "plugin",
            flow_id: None,
            channel_index: None,
            plugin_instance_id: Some(id.as_str().to_owned()),
        },
    }
}

const fn plugin_stream(stream: RunnerPluginDiagnosticStream) -> &'static str {
    match stream {
        RunnerPluginDiagnosticStream::Stdout => "stdout",
        RunnerPluginDiagnosticStream::Stderr => "stderr",
    }
}

const fn channel_stream(kind: RunnerChannelDiagnosticKind) -> &'static str {
    match kind {
        RunnerChannelDiagnosticKind::Print => "lua-print",
        RunnerChannelDiagnosticKind::Error => "lua-error",
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use axum::body::to_bytes;
    use tokio_stream::iter;

    use super::*;
    use crate::runner::diagnostics::{RunnerDiagnostic, RunnerDiagnosticSource};

    #[test]
    fn target_accepts_only_exact_flow_or_instance_targets() -> io::Result<()> {
        for (text, index) in [
            ("flow:telemetry/channel:0", 0),
            ("flow:telemetry/channel:4294967295", u32::MAX),
        ] {
            assert_eq!(
                parse_target(text),
                Some(RunnerDiagnosticTarget::FlowChannel {
                    flow_id: FlowId::try_from(String::from("telemetry"))
                        .map_err(io::Error::other)?,
                    channel_index: index,
                })
            );
        }
        assert_eq!(
            parse_target("plugin:archive"),
            Some(RunnerDiagnosticTarget::Plugin(
                PluginInstanceId::try_from("archive").map_err(io::Error::other)?,
            ))
        );
        assert_eq!(
            parse_target("flow:a/channel:b/channel:2"),
            Some(RunnerDiagnosticTarget::FlowChannel {
                flow_id: FlowId::try_from(String::from("a/channel:b")).map_err(io::Error::other)?,
                channel_index: 2,
            })
        );
        for rejected in [
            "channel:0",
            "source",
            "sink:archive",
            "plugin:",
            "flow:/channel:0",
            "flow:telemetry/channel:",
            "flow:telemetry/channel:+1",
            "flow:telemetry/channel:-1",
            "flow:telemetry/channel: 1",
            "flow:telemetry/channel:1 ",
            "flow:telemetry/channel:4294967296",
        ] {
            assert_eq!(parse_target(rejected), None, "accepted {rejected}");
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn diagnostic_event_exposes_runtime_identity_and_text_flags() -> io::Result<()> {
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        let frame = RunnerDiagnosticFrame::Diagnostic(Arc::new(RunnerDiagnostic {
            pipeline_instance_id: Arc::from("process-a"),
            source: RunnerDiagnosticSource::FlowChannel {
                flow_id: FlowId::try_from(String::from("telemetry")).map_err(io::Error::other)?,
                channel_index: 2,
                channel_instance_id: 3,
                lua_vm_instance_id: 4,
                kind: RunnerChannelDiagnosticKind::Print,
                phase: None,
                code: None,
            },
            observed_at_unix_millis: 5,
            text: String::from("hello").into_boxed_str(),
            truncated: true,
            invalid_utf8: true,
            sequence: 6,
        }));

        let encoded = render_event(
            &pipeline_id,
            RunnerDiagnosticTarget::FlowChannel {
                flow_id: FlowId::try_from(String::from("telemetry")).map_err(io::Error::other)?,
                channel_index: 2,
            },
            frame,
        )
        .await?;

        assert_eq!(encoded.0, "diagnostic");
        assert_eq!(encoded.1["pipelineId"], "pipeline-a");
        assert_eq!(encoded.1["pipelineInstanceId"], "process-a");
        assert_eq!(encoded.1["flowId"], "telemetry");
        assert_eq!(encoded.1["channelIndex"], 2);
        assert_eq!(encoded.1["channelInstanceId"], "3");
        assert_eq!(encoded.1["luaVmInstanceId"], "4");
        assert_eq!(encoded.1["stream"], "lua-print");
        assert_eq!(encoded.1["observedAtUnixMillis"], 5);
        assert_eq!(encoded.1["text"], "hello");
        assert_eq!(encoded.1["truncated"], true);
        assert_eq!(encoded.1["invalidUtf8"], true);
        assert_eq!(encoded.1["sequence"], "6");
        assert!(encoded.1.get("phase").is_none());
        assert!(encoded.1.get("code").is_none());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn channel_error_event_exposes_phase_and_code() -> io::Result<()> {
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        let frame = RunnerDiagnosticFrame::Diagnostic(Arc::new(RunnerDiagnostic {
            pipeline_instance_id: Arc::from("process-a"),
            source: RunnerDiagnosticSource::FlowChannel {
                flow_id: FlowId::try_from(String::from("telemetry")).map_err(io::Error::other)?,
                channel_index: 2,
                channel_instance_id: 3,
                lua_vm_instance_id: 4,
                kind: RunnerChannelDiagnosticKind::Error,
                phase: Some(String::from("lua_main").into_boxed_str()),
                code: Some(String::from("process.lua_main_failed").into_boxed_str()),
            },
            observed_at_unix_millis: 5,
            text: String::from("runtime error: boom").into_boxed_str(),
            truncated: false,
            invalid_utf8: false,
            sequence: 6,
        }));

        let encoded = render_event(
            &pipeline_id,
            RunnerDiagnosticTarget::FlowChannel {
                flow_id: FlowId::try_from(String::from("telemetry")).map_err(io::Error::other)?,
                channel_index: 2,
            },
            frame,
        )
        .await?;

        assert_eq!(encoded.0, "diagnostic");
        assert_eq!(encoded.1["stream"], "lua-error");
        assert_eq!(encoded.1["phase"], "lua_main");
        assert_eq!(encoded.1["code"], "process.lua_main_failed");
        assert_eq!(encoded.1["luaVmInstanceId"], "4");
        assert_eq!(encoded.1["text"], "runtime error: boom");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plugin_event_exposes_process_identity_and_standard_stream() -> io::Result<()> {
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        let frame = RunnerDiagnosticFrame::Diagnostic(Arc::new(RunnerDiagnostic {
            pipeline_instance_id: Arc::from("process-a"),
            source: RunnerDiagnosticSource::Plugin {
                plugin_instance_id: PluginInstanceId::try_from("gateway")
                    .map_err(io::Error::other)?,
                plugin_process_instance_id: 9,
                stream: RunnerPluginDiagnosticStream::Stderr,
            },
            observed_at_unix_millis: 5,
            text: String::from("failed").into_boxed_str(),
            truncated: false,
            invalid_utf8: false,
            sequence: 6,
        }));

        let encoded = render_event(
            &pipeline_id,
            RunnerDiagnosticTarget::Plugin(
                PluginInstanceId::try_from("gateway").map_err(io::Error::other)?,
            ),
            frame,
        )
        .await?;

        assert_eq!(encoded.0, "diagnostic");
        assert_eq!(encoded.1["pluginInstanceId"], "gateway");
        assert_eq!(encoded.1["pluginProcessInstanceId"], "9");
        assert_eq!(encoded.1["stream"], "stderr");
        assert_eq!(encoded.1["text"], "failed");
        assert_eq!(encoded.1["sequence"], "6");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn attached_event_exposes_target_kind_and_numeric_channel_index() -> io::Result<()> {
        let pipeline_id = TenonDocumentId::try_from("pipeline-a").map_err(io::Error::other)?;
        let target = RunnerDiagnosticTarget::FlowChannel {
            flow_id: FlowId::try_from(String::from("telemetry")).map_err(io::Error::other)?,
            channel_index: 2,
        };

        let encoded = render_event(&pipeline_id, target, RunnerDiagnosticFrame::Attached).await?;

        assert_eq!(encoded.0, "attached");
        assert_eq!(encoded.1["pipelineId"], "pipeline-a");
        assert_eq!(encoded.1["target"]["kind"], "flow-channel");
        assert_eq!(encoded.1["target"]["flowId"], "telemetry");
        assert_eq!(encoded.1["target"]["channelIndex"], 2);
        Ok(())
    }

    async fn render_event(
        pipeline_id: &TenonDocumentId,
        target: RunnerDiagnosticTarget,
        frame: RunnerDiagnosticFrame,
    ) -> io::Result<(String, serde_json::Value)> {
        let response = Sse::new(iter([Ok::<Event, Infallible>(sse_event(
            pipeline_id,
            &target,
            frame,
        ))]))
        .into_response();
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .map_err(io::Error::other)?;
        let body = std::str::from_utf8(&body).map_err(io::Error::other)?;
        let event = body
            .lines()
            .find_map(|line| line.strip_prefix("event: "))
            .ok_or_else(|| io::Error::other("SSE event name is missing"))?;
        let data = body
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .ok_or_else(|| io::Error::other("SSE event data is missing"))?;
        let data = serde_json::from_str(data).map_err(io::Error::other)?;
        Ok((event.to_owned(), data))
    }
}
