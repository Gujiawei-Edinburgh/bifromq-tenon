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

//! Owns admission, admitted handlers, and forced cancellation for one HTTP server.
//!
//! Closing admission leaves admitted handlers running. Cancellation drops their
//! futures; response streams remain owned by the transport and diagnostics.

use super::response;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, watch};

#[derive(Clone)]
pub(super) struct HttpRequestLifecycle {
    state: Arc<RequestState>,
    cancellation: watch::Sender<HttpRequestCancellation>,
}

impl HttpRequestLifecycle {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(RequestState {
                admission: AtomicBool::new(true),
                active: AtomicUsize::new(0),
                idle: Notify::new(),
            }),
            cancellation: watch::channel(HttpRequestCancellation::Running).0,
        }
    }

    pub(super) fn cancellation(&self) -> watch::Receiver<HttpRequestCancellation> {
        self.cancellation.subscribe()
    }

    pub(super) fn close_admission(&self) {
        self.state.admission.store(false, Ordering::Release);
    }

    pub(super) fn cancel(&self) {
        self.close_admission();
        self.cancellation
            .send_replace(HttpRequestCancellation::Cancelled);
    }

    pub(super) async fn wait_for_handlers(&self) {
        loop {
            let idle = self.state.idle.notified();
            if self.state.active.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    pub(super) async fn enforce(
        State(lifecycle): State<Self>,
        request: Request,
        next: Next,
    ) -> Response {
        let _handler = lifecycle.enter();
        let mut cancellation = lifecycle.cancellation();
        if !lifecycle.state.admission.load(Ordering::Acquire)
            || *cancellation.borrow() == HttpRequestCancellation::Cancelled
        {
            return response::service_unavailable();
        }
        tokio::select! {
            biased;
            changed = cancellation.changed() => {
                let _ = changed;
                response::service_unavailable()
            }
            response = next.run(request) => response,
        }
    }

    pub(super) fn enter(&self) -> HttpHandlerGuard {
        self.state.active.fetch_add(1, Ordering::AcqRel);
        HttpHandlerGuard(Arc::clone(&self.state))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpRequestCancellation {
    Running,
    Cancelled,
}

struct RequestState {
    admission: AtomicBool,
    active: AtomicUsize,
    idle: Notify,
}

pub(super) struct HttpHandlerGuard(Arc<RequestState>);

impl Drop for HttpHandlerGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}
