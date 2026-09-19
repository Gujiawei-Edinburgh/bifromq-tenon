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

//! Owns accepted connections from handshake through HTTP and TLS shutdown.
//!
//! The listener task joins every connection on graceful exit and cancels and
//! joins the whole set on failure. Dropping the set also aborts its children.

use super::lifecycle::HttpRequestCancellation;
use super::tls::TlsServerConfig;
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

pub(super) async fn serve(
    mut listener: TcpListener,
    tls: Option<TlsServerConfig>,
    app: Router,
    mut shutdown: oneshot::Receiver<()>,
    mut cancellation: watch::Receiver<HttpRequestCancellation>,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    // Closing this channel stops handshakes and starts HTTP connection drain.
    let (drain, draining) = watch::channel(());
    let failure = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break None,
            _ = cancellation.changed() => break None,
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    break Some(io::Error::other(error));
                }
            }
            (stream, _) = axum::serve::Listener::accept(&mut listener) => {
                let tls = tls.clone();
                let app = app.clone();
                let draining = draining.clone();
                connections.spawn(async move {
                    match tls {
                        Some(tls) => serve_tls_connection(stream, tls, app, draining).await,
                        None => serve_connection(stream, app, draining).await,
                    }
                });
            }
        }
    };
    drop(listener);
    drop(drain);
    if failure.is_some() || *cancellation.borrow() == HttpRequestCancellation::Cancelled {
        connections.shutdown().await;
    } else {
        loop {
            tokio::select! {
                biased;
                _ = cancellation.changed() => {
                    connections.shutdown().await;
                    break;
                }
                result = connections.join_next() => match result {
                    Some(Ok(())) => {},
                    Some(Err(error)) => {
                        connections.shutdown().await;
                        return Err(io::Error::other(error));
                    }
                    None => break,
                }
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

async fn serve_tls_connection(
    stream: tokio::net::TcpStream,
    tls: TlsServerConfig,
    app: Router,
    mut draining: watch::Receiver<()>,
) {
    let acceptor = TlsAcceptor::from(tls.identity);
    let accepted = tokio::select! {
        biased;
        _ = draining.changed() => return,
        result = tokio::time::timeout(tls.handshake_timeout, acceptor.accept(stream)) => result,
    };
    if let Ok(Ok(stream)) = accepted {
        serve_connection(stream, app, draining).await;
    }
}

async fn serve_connection(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    app: Router,
    mut draining: watch::Receiver<()>,
) {
    {
        let connection = http1::Builder::new()
            .serve_connection(TokioIo::new(&mut stream), TowerToHyperService::new(app));
        tokio::pin!(connection);
        tokio::select! {
            biased;
            _ = draining.changed() => {
                connection.as_mut().graceful_shutdown();
                let _ = connection.await;
            }
            _ = &mut connection => {},
        }
    }
    // Tokio-Rustls flushes close_notify before closing the underlying socket.
    // Peer I/O failures terminate only this connection.
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests;
