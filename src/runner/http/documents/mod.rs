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

//! Tenon Document HTTP transport handlers.

use super::app::HttpServices;
use super::response::{
    ErrorBody, ErrorEnvelope, JSONC, RunnerHttpResponse, SCHEMA_JSON, ValidationIssue, bad_path,
    has_content_type, invalid_precondition, request_body, service_unavailable,
    unsupported_media_type,
};
use crate::contracts::tenon_document::v1_schema_bytes;
use crate::identifiers::TenonDocumentId;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::management::{
    DeleteDocumentPrecondition, DocumentContent, DocumentSummary, DocumentValidationFailure,
    DocumentWriteFailure, PutDocumentFailure, PutDocumentOutcome, PutDocumentPrecondition,
};
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::header::{IF_MATCH, IF_NONE_MATCH};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::ToSchema;

/// The document list response: one summary per Document, sorted by id.
#[derive(Serialize, ToSchema)]
struct DocumentListBody {
    documents: Vec<DocumentSummaryBody>,
}

#[derive(Serialize, ToSchema)]
struct DocumentSummaryBody {
    id: String,
    etag: String,
}

#[utoipa::path(
    get,
    path = "/documents",
    tag = "documents",
    responses(
        (status = 200, description = "Document identities and ETags, sorted by id", body = DocumentListBody),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
pub(super) async fn list_documents(State(state): State<HttpServices>) -> Response {
    let Ok(documents) = state.management.list_documents().await else {
        return service_unavailable();
    };
    let mut documents = documents.into_vec();
    documents.sort_unstable_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
    let body = DocumentListBody {
        documents: documents.iter().map(document_summary_body).collect(),
    };
    RunnerHttpResponse::serialized(StatusCode::OK, &body).into_response()
}

#[utoipa::path(
    get,
    path = "/documents/{id}",
    tag = "documents",
    params(("id" = String, Path, description = "Tenon Document id")),
    responses(
        (status = 200, description = "The original JSONC source with its strong ETag", content_type = "application/jsonc"),
        (status = 400, description = "The request path is invalid", body = ErrorEnvelope),
        (status = 404, description = "The Document does not exist", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
pub(super) async fn get_document(
    State(state): State<HttpServices>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let Ok(Path(id)) = path else {
        return bad_path();
    };
    let Ok(id) = TenonDocumentId::try_from(id) else {
        return bad_path();
    };
    let Ok(document) = state.management.document(id).await else {
        return service_unavailable();
    };
    document.map_or_else(
        || {
            RunnerHttpResponse::error(
                StatusCode::NOT_FOUND,
                "tenon_document_not_found",
                "Tenon Document was not found",
            )
            .into_response()
        },
        |document| document_content_response(document).into_response(),
    )
}

#[utoipa::path(
    put,
    path = "/documents/{id}",
    tag = "documents",
    params(("id" = String, Path, description = "Tenon Document id; must equal the body id")),
    request_body(content = String, description = "Tenon Document JSONC source", content_type = "application/jsonc"),
    responses(
        (status = 201, description = "Created; the new resource ETag is returned"),
        (status = 204, description = "Replaced or unchanged; the resource ETag is returned"),
        (status = 400, description = "Invalid path or conditional header", body = ErrorEnvelope),
        (status = 403, description = "The execution policy denied the document", body = ErrorEnvelope),
        (status = 412, description = "The If-Match ETag does not match", body = ErrorEnvelope),
        (status = 415, description = "The Content-Type is not application/jsonc", body = ErrorEnvelope),
        (status = 422, description = "The Document failed static validation", body = ErrorEnvelope),
        (status = 428, description = "A conditional request header is required", body = ErrorEnvelope),
        (status = 500, description = "The Document could not be committed", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
pub(super) async fn put_document(
    State(state): State<HttpServices>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    if !has_content_type(&headers, JSONC) {
        return unsupported_media_type();
    }
    let body = match request_body(body) {
        Ok(body) => body,
        Err(response) => return response.into_response(),
    };
    let Ok(Path(path_id)) = path else {
        return bad_path();
    };
    let Ok(path_id) = TenonDocumentId::try_from(path_id) else {
        return bad_path();
    };
    let Ok(precondition) = put_document_precondition(&headers) else {
        return invalid_precondition();
    };
    let etag = TenonDocumentEtag::for_source(&body).strong_value();
    let Ok(result) = state
        .management
        .put_document(path_id, precondition, body.to_vec().into_boxed_slice())
        .await
    else {
        return service_unavailable();
    };
    document_write_response(result, etag)
}

#[utoipa::path(
    delete,
    path = "/documents/{id}",
    tag = "documents",
    params(("id" = String, Path, description = "Tenon Document id")),
    responses(
        (status = 204, description = "Deleted, or idempotently absent"),
        (status = 400, description = "The request path is invalid", body = ErrorEnvelope),
        (status = 412, description = "The If-Match ETag does not match", body = ErrorEnvelope),
        (status = 428, description = "If-Match is required for an existing resource", body = ErrorEnvelope),
        (status = 500, description = "The Document could not be deleted", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
pub(super) async fn delete_document(
    State(state): State<HttpServices>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
) -> Response {
    let Ok(Path(id)) = path else {
        return bad_path();
    };
    let Ok(id) = TenonDocumentId::try_from(id) else {
        return bad_path();
    };
    let Ok(precondition) = delete_document_precondition(&headers) else {
        return invalid_precondition();
    };
    let Ok(result) = state.management.delete_document(id, precondition).await else {
        return service_unavailable();
    };
    document_delete_response(result)
}

#[utoipa::path(
    get,
    path = "/document-schema",
    tag = "documents",
    responses(
        (status = 200, description = "The built-in Draft 2020-12 Document Schema", content_type = "application/schema+json"),
    )
)]
pub(super) async fn document_schema() -> Response {
    RunnerHttpResponse::bytes(StatusCode::OK, SCHEMA_JSON, v1_schema_bytes().to_vec())
        .into_response()
}

fn document_summary_body(document: &DocumentSummary) -> DocumentSummaryBody {
    DocumentSummaryBody {
        id: document.id.as_str().to_owned(),
        etag: document.etag.strong_value(),
    }
}

fn document_content_response(document: DocumentContent) -> RunnerHttpResponse {
    RunnerHttpResponse::bytes(StatusCode::OK, JSONC, document.source.into_vec())
        .with_etag(document.etag.strong_value())
}

fn document_write_response(
    result: Result<PutDocumentOutcome, PutDocumentFailure>,
    etag: String,
) -> Response {
    match result {
        Ok(PutDocumentOutcome::Created) => RunnerHttpResponse::empty(StatusCode::CREATED)
            .with_etag(etag)
            .into_response(),
        Ok(PutDocumentOutcome::Replaced | PutDocumentOutcome::Unchanged) => {
            RunnerHttpResponse::empty(StatusCode::NO_CONTENT)
                .with_etag(etag)
                .into_response()
        }
        Err(PutDocumentFailure::ExecutionDenied(error)) => {
            RunnerHttpResponse::error(StatusCode::FORBIDDEN, error.code(), error.message())
                .into_response()
        }
        Err(PutDocumentFailure::Validation(error)) => validation_error(error),
        Err(PutDocumentFailure::IdMismatch) => RunnerHttpResponse::error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "tenon_document_id_mismatch",
            "Tenon Document id does not match the request path",
        )
        .into_response(),
        Err(PutDocumentFailure::Write(DocumentWriteFailure::PreconditionRequired)) => {
            RunnerHttpResponse::error(
                StatusCode::PRECONDITION_REQUIRED,
                "precondition_required",
                "A conditional request header is required",
            )
            .into_response()
        }
        Err(PutDocumentFailure::Write(DocumentWriteFailure::PreconditionFailed)) => {
            RunnerHttpResponse::error(
                StatusCode::PRECONDITION_FAILED,
                "precondition_failed",
                "The resource ETag does not match",
            )
            .into_response()
        }
        Err(PutDocumentFailure::Write(DocumentWriteFailure::Store)) => RunnerHttpResponse::error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "tenon_document_store_failed",
            "Tenon Document could not be committed",
        )
        .into_response(),
    }
}

fn document_delete_response(result: Result<(), DocumentWriteFailure>) -> Response {
    match result {
        Ok(()) => RunnerHttpResponse::empty(StatusCode::NO_CONTENT).into_response(),
        Err(DocumentWriteFailure::PreconditionRequired) => RunnerHttpResponse::error(
            StatusCode::PRECONDITION_REQUIRED,
            "precondition_required",
            "If-Match is required",
        )
        .into_response(),
        Err(DocumentWriteFailure::PreconditionFailed) => RunnerHttpResponse::error(
            StatusCode::PRECONDITION_FAILED,
            "precondition_failed",
            "The resource ETag does not match",
        )
        .into_response(),
        Err(DocumentWriteFailure::Store) => RunnerHttpResponse::error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "tenon_document_store_failed",
            "Tenon Document could not be deleted",
        )
        .into_response(),
    }
}

fn put_document_precondition(headers: &HeaderMap) -> Result<PutDocumentPrecondition, ()> {
    let (Ok(if_none_match), Ok(if_match)) = (
        one_header(headers, &IF_NONE_MATCH),
        one_header(headers, &IF_MATCH),
    ) else {
        return Err(());
    };
    match (if_none_match, if_match) {
        (Some(value), None) if value.as_bytes() == b"*" => Ok(PutDocumentPrecondition::Create),
        (None, Some(value)) => value
            .to_str()
            .ok()
            .and_then(TenonDocumentEtag::from_strong_value)
            .map(PutDocumentPrecondition::Replace)
            .ok_or(()),
        (None, None) => Ok(PutDocumentPrecondition::Missing),
        _ => Err(()),
    }
}

fn delete_document_precondition(headers: &HeaderMap) -> Result<DeleteDocumentPrecondition, ()> {
    let (Ok(if_none_match), Ok(if_match)) = (
        one_header(headers, &IF_NONE_MATCH),
        one_header(headers, &IF_MATCH),
    ) else {
        return Err(());
    };
    match (if_none_match, if_match) {
        (None, Some(value)) => value
            .to_str()
            .ok()
            .and_then(TenonDocumentEtag::from_strong_value)
            .map(DeleteDocumentPrecondition::Match)
            .ok_or(()),
        (None, None) => Ok(DeleteDocumentPrecondition::Missing),
        _ => Err(()),
    }
}

fn one_header<'a>(
    headers: &'a HeaderMap,
    name: &HeaderName,
) -> Result<Option<&'a HeaderValue>, ()> {
    let mut values = headers.get_all(name).iter();
    let value = values.next();
    if values.next().is_some() {
        Err(())
    } else {
        Ok(value)
    }
}

fn validation_error(error: DocumentValidationFailure) -> Response {
    let issues = match &error {
        DocumentValidationFailure::Syntax(error) => vec![ValidationIssue {
            code: error.code(),
            path: String::new(),
            message: "Tenon Document syntax is invalid",
        }],
        DocumentValidationFailure::Verification(error) => error
            .issues()
            .iter()
            .map(|issue| ValidationIssue {
                code: issue.code(),
                path: issue.instance_path().to_owned(),
                message: "Tenon Document value is invalid",
            })
            .collect(),
    };
    RunnerHttpResponse::serialized(
        StatusCode::UNPROCESSABLE_ENTITY,
        &ErrorEnvelope {
            error: ErrorBody {
                code: "invalid_tenon_document",
                message: "Tenon Document failed static validation".to_owned(),
                issues,
            },
        },
    )
    .into_response()
}
