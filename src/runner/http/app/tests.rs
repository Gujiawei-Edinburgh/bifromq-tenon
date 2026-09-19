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

use super::HttpServices;
use super::*;
use crate::runner::extensions::{HttpAuthRejection, NoHttpAuth};
use crate::runner::http::lifecycle::HttpRequestCancellation;
use axum::body::{Body, Bytes, to_bytes};
use axum::http::{HeaderMap, Method, StatusCode};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;
use tonic::codegen::Service as _;

struct Authorization<F>(F);

impl<F> HttpApiAuthorization for Authorization<F>
where
    F: Fn(&Method, &str, &HeaderMap) -> Result<(), HttpAuthRejection> + Send + Sync,
{
    fn authorize<'a>(
        &'a self,
        method: &'a Method,
        path: &'a str,
        headers: &'a HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpAuthRejection>> + Send + 'a>> {
        Box::pin(async move { (self.0)(method, path, headers) })
    }
}

fn app(
    authorization: impl HttpApiAuthorization + 'static,
) -> io::Result<(Router, HttpRequestLifecycle)> {
    let (management, _receiver) = crate::runner::management::test_support::interface();
    let lifecycle = HttpRequestLifecycle::new();
    Ok((
        router(
            HttpServices {
                management,
                diagnostics: RunnerDiagnostics::new(),
                metrics: crate::runner::metrics::test_support::empty()?,
            },
            Arc::new(authorization),
            lifecycle.clone(),
        ),
        lifecycle,
    ))
}
#[tokio::test]
#[allow(
    clippy::panic,
    reason = "panic probes prove forbidden callbacks and body polling never occur"
)]
async fn rejection_precedes_body_extraction_for_every_body_handler() -> io::Result<()> {
    let authorization = Authorization(|_: &Method, _: &str, _: &HeaderMap| {
        Err(HttpAuthRejection::Unauthorized {
            challenge: HeaderValue::from_static("Bearer realm=\"test\""),
        })
    });
    let (mut app, _lifecycle) = app(authorization)?;
    for (method, path) in [(Method::PUT, "/documents/test"), (Method::POST, "/plugins")] {
        let unread = tokio_stream::iter(std::iter::from_fn(
            || -> Option<Result<Bytes, io::Error>> { panic!("Rejected request body was polled") },
        ));
        let request = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::from_stream(unread))
            .map_err(io::Error::other)?;
        let response = app.call(request).await.map_err(io::Error::other)?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        assert_eq!(
            response.headers()["www-authenticate"],
            "Bearer realm=\"test\""
        );
        assert_eq!(
            response.headers()["tenon-version"],
            env!("CARGO_PKG_VERSION")
        );
        let bytes = to_bytes(response.into_body(), 1024)
            .await
            .map_err(io::Error::other)?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes)?,
            serde_json::json!({"error":{"code":"unauthorized","message":"Authentication is required"}})
        );
    }
    Ok(())
}

#[tokio::test]
async fn actual_methods_distinguish_reads_from_writes_and_keep_head_bodyless() -> io::Result<()> {
    let authorization = Authorization(|method: &Method, _: &str, _: &HeaderMap| match *method {
        Method::GET | Method::HEAD => Ok(()),
        _ => Err(HttpAuthRejection::Forbidden {
            challenge: Some(HeaderValue::from_static(
                "Bearer error=\"insufficient_scope\"",
            )),
        }),
    });
    let (mut app, _lifecycle) = app(authorization)?;
    for method in [Method::GET, Method::HEAD, Method::PUT, Method::DELETE] {
        let response = app
            .call(
                Request::builder()
                    .method(method.clone())
                    .uri("/document-schema")
                    .body(Body::empty())
                    .map_err(io::Error::other)?,
            )
            .await
            .map_err(io::Error::other)?;
        if method == Method::GET || method == Method::HEAD {
            assert_eq!(response.status(), StatusCode::OK);
            if method == Method::HEAD {
                assert!(
                    to_bytes(response.into_body(), 1024)
                        .await
                        .map_err(io::Error::other)?
                        .is_empty()
                );
            }
        } else {
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert_eq!(
                response.headers()["www-authenticate"],
                "Bearer error=\"insufficient_scope\""
            );
            let bytes = to_bytes(response.into_body(), 1024)
                .await
                .map_err(io::Error::other)?;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bytes)?,
                serde_json::json!({"error":{"code":"forbidden","message":"Access is forbidden"}})
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn every_route_including_fallback_and_sse_passes_authorization_once() -> io::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let authorization = Authorization(move |_: &Method, _: &str, _: &HeaderMap| {
        observed.fetch_add(1, Ordering::Relaxed);
        Err(HttpAuthRejection::Forbidden { challenge: None })
    });
    let (mut app, _lifecycle) = app(authorization)?;
    for (index, (method, path)) in [
        (Method::GET, "/missing"),
        (Method::GET, "/openapi.json"),
        (Method::GET, "/metrics"),
        (Method::OPTIONS, "/document-schema"),
        (
            Method::GET,
            "/pipelines/test/diagnostics?target=plugin:source",
        ),
        (Method::HEAD, "/document-schema"),
    ]
    .into_iter()
    .enumerate()
    {
        let response = app
            .call(
                Request::builder()
                    .method(method.clone())
                    .uri(path)
                    .body(Body::empty())
                    .map_err(io::Error::other)?,
            )
            .await
            .map_err(io::Error::other)?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(!response.headers().contains_key("www-authenticate"));
        assert_eq!(calls.load(Ordering::Relaxed), index + 1);
        if method == Method::HEAD {
            assert!(
                to_bytes(response.into_body(), 1024)
                    .await
                    .map_err(io::Error::other)?
                    .is_empty()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn authorization_borrows_encoded_path_without_query_and_preserves_header_values()
-> io::Result<()> {
    let authorization = Authorization(|method: &Method, path: &str, headers: &HeaderMap| {
        assert_eq!(method, Method::DELETE);
        assert_eq!(path, "/documents/%74est");
        let values: Vec<_> = headers
            .get_all("x-proof")
            .iter()
            .map(HeaderValue::as_bytes)
            .collect();
        assert_eq!(values, [b"one".as_slice(), b"two".as_slice()]);
        assert_eq!(headers["x-binary"].as_bytes(), b"\x80");
        Err(HttpAuthRejection::Forbidden { challenge: None })
    });
    let (mut app, _lifecycle) = app(authorization)?;
    let request = Request::delete("/documents/%74est?ignored=yes")
        .header("x-proof", "one")
        .header("x-proof", "two")
        .header(
            "x-binary",
            HeaderValue::from_bytes(b"\x80").map_err(io::Error::other)?,
        )
        .body(Body::empty())
        .map_err(io::Error::other)?;
    assert_eq!(
        app.call(request).await.map_err(io::Error::other)?.status(),
        StatusCode::FORBIDDEN
    );
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::panic,
    reason = "panic probes prove forbidden callbacks and body polling never occur"
)]
async fn closed_admission_and_cancellation_skip_authorization() -> io::Result<()> {
    for shutdown in [
        HttpRequestCancellation::Running,
        HttpRequestCancellation::Cancelled,
    ] {
        let authorization = Authorization(|_: &Method, _: &str, _: &HeaderMap| {
            panic!("Closed HTTP admission called authorization")
        });
        let (mut app, lifecycle) = app(authorization)?;
        match shutdown {
            HttpRequestCancellation::Running => lifecycle.close_admission(),
            HttpRequestCancellation::Cancelled => {
                lifecycle.cancel();
            }
        }
        let response = app
            .call(
                Request::get("/document-schema")
                    .body(Body::empty())
                    .map_err(io::Error::other)?,
            )
            .await
            .map_err(io::Error::other)?;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(())
}

#[tokio::test]
async fn default_authorization_preserves_success_and_not_found() -> io::Result<()> {
    let (mut app, _lifecycle) = app(NoHttpAuth)?;
    for (path, status) in [
        ("/document-schema", StatusCode::OK),
        ("/missing", StatusCode::NOT_FOUND),
    ] {
        let response = app
            .call(
                Request::get(path)
                    .body(Body::empty())
                    .map_err(io::Error::other)?,
            )
            .await
            .map_err(io::Error::other)?;
        assert_eq!(response.status(), status);
    }
    Ok(())
}

struct WaitingAuthorization {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}

impl HttpApiAuthorization for WaitingAuthorization {
    fn authorize<'a>(
        &'a self,
        _method: &'a Method,
        path: &'a str,
        _headers: &'a HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpAuthRejection>> + Send + 'a>> {
        Box::pin(async move {
            if path == "/document-schema" {
                return Ok(());
            }
            let _resource = AuthorizationResource(&self.dropped);
            self.entered.notify_one();
            self.release.notified().await;
            Err(HttpAuthRejection::Unauthorized {
                challenge: HeaderValue::from_static("Bearer"),
            })
        })
    }
}

struct AuthorizationResource<'a>(&'a AtomicUsize);

impl Drop for AuthorizationResource<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
#[allow(
    clippy::panic,
    reason = "panic probes prove forbidden callbacks and body polling never occur"
)]
async fn waiting_authorization_keeps_body_unread_and_allows_other_requests() -> io::Result<()> {
    // Exercise completion, cancellation, and both becoming ready in the same poll.
    for (complete, cancellation, expected) in [
        (
            true,
            HttpRequestCancellation::Running,
            StatusCode::UNAUTHORIZED,
        ),
        (
            false,
            HttpRequestCancellation::Cancelled,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            true,
            HttpRequestCancellation::Cancelled,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let (mut app, lifecycle) = app(WaitingAuthorization {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            dropped: Arc::clone(&dropped),
        })?;
        let unread = tokio_stream::iter(std::iter::from_fn(
            || -> Option<Result<Bytes, io::Error>> {
                panic!("Pending authorization read the body")
            },
        ));
        let mut blocked_app = app.clone();
        let response = blocked_app.call(
            Request::post("/plugins")
                .body(Body::from_stream(unread))
                .map_err(io::Error::other)?,
        );
        tokio::pin!(response);
        tokio::select! {
            result = &mut response => panic!("Authorization completed before release: {result:?}"),
            () = entered.notified() => {},
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        let concurrent = app
            .call(
                Request::get("/document-schema")
                    .body(Body::empty())
                    .map_err(io::Error::other)?,
            )
            .await
            .map_err(io::Error::other)?;
        assert_eq!(concurrent.status(), StatusCode::OK);
        if complete {
            release.notify_one();
        }
        if cancellation == HttpRequestCancellation::Cancelled {
            lifecycle.cancel();
        }
        let result = response.await.map_err(io::Error::other)?;
        assert_eq!(result.status(), expected);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }
    Ok(())
}

#[tokio::test]
async fn metrics_route_defaults_filters_and_openapi_agree() -> io::Result<()> {
    let (mut app, lifecycle) = app(NoHttpAuth)?;
    for (path, expected) in [
        ("/metrics", StatusCode::OK),
        (
            "/metrics?format=json&include=tenon.process.memory",
            StatusCode::OK,
        ),
        (
            "/metrics?include=tenon.process.memory,%20tenon.process.memory",
            StatusCode::OK,
        ),
        (
            "/metrics?format=prometheus&include=tenon.process.memory",
            StatusCode::OK,
        ),
        ("/metrics?format=otlp", StatusCode::BAD_REQUEST),
        ("/metrics?include=", StatusCode::BAD_REQUEST),
        (
            "/metrics?include=tenon_process_memory_bytes",
            StatusCode::BAD_REQUEST,
        ),
        ("/metrics?format=json&format=json", StatusCode::BAD_REQUEST),
        (
            "/metrics?include=tenon.process.memory&include=tenon.process.cpu",
            StatusCode::BAD_REQUEST,
        ),
        ("/metrics?unknown=yes", StatusCode::BAD_REQUEST),
    ] {
        let response = app
            .call(
                Request::get(path)
                    .body(Body::empty())
                    .map_err(io::Error::other)?,
            )
            .await
            .map_err(io::Error::other)?;
        assert_eq!(response.status(), expected, "{path}");
        assert_eq!(
            response.headers()["tenon-version"],
            env!("CARGO_PKG_VERSION")
        );
        assert!(!response.headers().contains_key("etag"));
        if expected == StatusCode::OK {
            assert_eq!(response.headers()["cache-control"], "no-store");
            if path.contains("format=prometheus") {
                assert_eq!(
                    response.headers()["content-type"],
                    "text/plain; version=0.0.4; charset=utf-8"
                );
            } else {
                assert_eq!(response.headers()["content-type"], "application/json");
            }
        }
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(io::Error::other)?;
        if expected == StatusCode::BAD_REQUEST {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bytes)?["error"]["code"],
                "invalid_metrics_query"
            );
        } else if path.contains("include=") && !path.contains("format=prometheus") {
            let body: serde_json::Value = serde_json::from_slice(&bytes)?;
            assert_eq!(
                body["processes"][0]["metrics"].as_array().map(Vec::len),
                Some(1)
            );
            assert_eq!(
                body["processes"][0]["metrics"][0]["name"],
                "tenon.process.memory"
            );
        }
    }
    let response = app
        .call(
            Request::get("/openapi.json")
                .body(Body::empty())
                .map_err(io::Error::other)?,
        )
        .await
        .map_err(io::Error::other)?;
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(io::Error::other)?;
    let api: serde_json::Value = serde_json::from_slice(&bytes)?;
    let operation = &api["paths"]["/metrics"]["get"];
    assert_eq!(operation["parameters"].as_array().map(Vec::len), Some(2));
    assert!(
        operation["responses"]["200"]["content"]
            .get("application/json")
            .is_some()
    );
    assert!(
        operation["responses"]["200"]["content"]
            .get("text/plain; version=0.0.4; charset=utf-8")
            .is_some()
    );
    let response = app
        .call(
            Request::get("/metrics")
                .body(Body::empty())
                .map_err(io::Error::other)?,
        )
        .await
        .map_err(io::Error::other)?;
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(io::Error::other)?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    let mut schema = operation["responses"]["200"]["content"]["application/json"]["schema"].clone();
    schema["components"] = api["components"].clone();
    let validator = jsonschema::draft202012::new(&schema).map_err(io::Error::other)?;
    assert!(
        validator.is_valid(&body),
        "Metrics response does not satisfy its OpenAPI schema"
    );
    lifecycle.close_admission();
    let response = app
        .call(
            Request::get("/metrics")
                .body(Body::empty())
                .map_err(io::Error::other)?,
        )
        .await
        .map_err(io::Error::other)?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}
