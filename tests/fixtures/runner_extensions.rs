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

//! A real distribution binary exercising only Tenon's public extension API.

use axum::http::{HeaderMap, HeaderValue, Method};
use serde_json::Value;
use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{self, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::process::ExitCode;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tenon::{
    DocumentProtection, ExecutionDenied, ExecutionPermit, ExecutionPolicy, ExecutionScope,
    HttpApiAuthorization, HttpAuthRejection, NoHttpAuth, RunnerHooks,
};

fn main() -> ExitCode {
    tenon::run_main_with(|config| {
        let extra = config.extra();
        if extra.get("failInitialization").and_then(Value::as_bool) == Some(true) {
            return Err(io::Error::other("Fixture initialization rejected").into());
        }
        if let Some(marker) = extra.get("initializeMarker").and_then(Value::as_str) {
            File::create_new(marker)?;
        }
        Ok(RunnerHooks::new(
            Policy {
                maximum_documents: extra.get("maximumDocuments").and_then(Value::as_u64),
                maximum_authorizations: extra.get("maximumAuthorizations").and_then(Value::as_u64),
                denied_document: extra
                    .get("deniedDocument")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                scope_log: extra
                    .get("scopeLog")
                    .and_then(Value::as_str)
                    .map(PathBuf::from),
                authorizations: Rc::new(Cell::new(0)),
                expires_after: extra
                    .get("expiresAfterMs")
                    .and_then(Value::as_u64)
                    .map(Duration::from_millis),
                expires_from_authorization: extra
                    .get("expiresFromAuthorization")
                    .and_then(Value::as_u64)
                    .unwrap_or(1),
            },
            Protection,
            HttpAuthorization {
                enabled: extra
                    .get("httpAuth")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            },
        ))
    })
}

struct Policy {
    maximum_documents: Option<u64>,
    maximum_authorizations: Option<u64>,
    denied_document: Option<String>,
    scope_log: Option<PathBuf>,
    // A thread-local counter also verifies that policy hooks need no Send/Sync.
    authorizations: Rc<Cell<u64>>,
    expires_after: Option<Duration>,
    expires_from_authorization: u64,
}

impl ExecutionPolicy for Policy {
    fn authorize(&self, scope: ExecutionScope<'_>) -> Result<ExecutionPermit, ExecutionDenied> {
        let calls = self.authorizations.get() + 1;
        self.authorizations.set(calls);
        let entitlement_until = self
            .expires_after
            .filter(|_| calls >= self.expires_from_authorization)
            .map(|duration| Instant::now() + duration);
        if let Some(path) = &self.scope_log {
            let scopes = scope.documents.iter().map(|document| {
                serde_json::json!({
                    "id": document.id().as_str(),
                    "endpoint": document.plugin_instances().get("source").map(|instance| &instance.config()["endpoint"])
                })
            }).collect::<Vec<_>>();
            let write_scope = || -> io::Result<()> {
                let mut file = OpenOptions::new().create(true).append(true).open(path)?;
                serde_json::to_writer(&mut file, &scopes)?;
                writeln!(file)
            };
            write_scope().map_err(|error| {
                ExecutionDenied::new("fixture.log_failed", error.to_string(), entitlement_until)
            })?;
        }
        if self
            .maximum_authorizations
            .is_some_and(|limit| calls > limit)
        {
            return Err(ExecutionDenied::new(
                "fixture.extra_authorization",
                "Desired state was authorized again",
                entitlement_until,
            ));
        }
        if self.denied_document.as_ref().is_some_and(|id| {
            scope
                .documents
                .iter()
                .any(|document| document.id().as_str() == id)
        }) {
            return Err(ExecutionDenied::new(
                "fixture.document_denied",
                "Document is not allowed",
                entitlement_until,
            ));
        }
        if self
            .maximum_documents
            .is_some_and(|limit| scope.documents.len() as u64 > limit)
        {
            return Err(ExecutionDenied::new(
                "fixture.capacity",
                "Document capacity is exhausted",
                entitlement_until,
            ));
        }
        Ok(ExecutionPermit { entitlement_until })
    }
}

struct Protection;

impl DocumentProtection for Protection {
    fn protect(&self, source: &[u8], output: &mut dyn Write) -> io::Result<()> {
        output.write_all(b"fixture:")?;
        if source
            .windows(b"fail-protection".len())
            .any(|part| part == b"fail-protection")
        {
            return Err(io::Error::other(
                "Fixture protection failed after partial output",
            ));
        }
        // Reversible test framing, deliberately not an encryption algorithm.
        for byte in source {
            output.write_all(&[byte ^ 0x80])?;
        }
        Ok(())
    }

    fn unprotect(&self, stored: &[u8], output: &mut dyn Write) -> io::Result<()> {
        let source = stored
            .strip_prefix(b"fixture:")
            .ok_or_else(|| io::Error::other("Fixture protected header is missing"))?;
        for byte in source {
            output.write_all(&[byte ^ 0x80])?;
        }
        Ok(())
    }
}

struct HttpAuthorization {
    enabled: bool,
}

impl HttpApiAuthorization for HttpAuthorization {
    fn authorize<'a>(
        &'a self,
        method: &'a Method,
        path: &'a str,
        headers: &'a HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpAuthRejection>> + Send + 'a>> {
        Box::pin(async move {
            if !self.enabled {
                return NoHttpAuth.authorize(method, path, headers).await;
            }
            // Cross an actual suspension point before inspecting the borrowed request.
            tokio::task::yield_now().await;
            match headers.get("authorization").map(HeaderValue::as_bytes) {
                Some(b"Bearer writer") => Ok(()),
                Some(b"Bearer reader") if *method == Method::GET || *method == Method::HEAD => {
                    Ok(())
                }
                Some(b"Bearer reader") => Err(HttpAuthRejection::Forbidden {
                    challenge: Some(HeaderValue::from_static(
                        "Bearer error=\"insufficient_scope\"",
                    )),
                }),
                _ => Err(HttpAuthRejection::Unauthorized {
                    challenge: HeaderValue::from_static("Bearer realm=\"fixture\""),
                }),
            }
        })
    }
}
