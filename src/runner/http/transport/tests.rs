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

use super::*;
use crate::config::HttpTlsConfig;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncReadExt as _;
use tokio::net::TcpStream;

fn tls_config(handshake_timeout: Duration) -> io::Result<TlsServerConfig> {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    super::super::tls::load(&HttpTlsConfig {
        client_ca_file: None,
        certificate_chain_file: fixtures.join("server-a.pem"),
        private_key_file: fixtures.join("server-a-key.pem"),
        handshake_timeout,
    })
    .map_err(io::Error::other)
}

#[tokio::test(flavor = "current_thread")]
async fn partial_handshake_obeys_the_configured_nonrenewable_deadline() -> io::Result<()> {
    tokio::time::pause();
    for deadline in [Duration::from_millis(250), Duration::from_secs(10)] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = TcpStream::connect(listener.local_addr()?).await?;
        let (stream, _) = listener.accept().await?;
        let (_drain, draining) = watch::channel(());
        let tls = tls_config(deadline)?;
        let task = tokio::spawn(serve_tls_connection(stream, tls, Router::new(), draining));
        tokio::task::yield_now().await;
        tokio::time::advance(deadline - Duration::from_millis(1)).await;
        client.write_all(&[0x16]).await?;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        task.await.map_err(io::Error::other)?;
        assert_cancelled_connection(&mut client).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_cancels_handshakes_before_or_after_their_first_poll() -> io::Result<()> {
    for shutdown_before_poll in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = TcpStream::connect(listener.local_addr()?).await?;
        let (stream, _) = listener.accept().await?;
        let (drain, draining) = watch::channel(());
        let tls = tls_config(Duration::from_secs(10))?;
        let task = if shutdown_before_poll {
            drop(drain);
            tokio::spawn(serve_tls_connection(stream, tls, Router::new(), draining))
        } else {
            let task = tokio::spawn(serve_tls_connection(stream, tls, Router::new(), draining));
            tokio::task::yield_now().await;
            drop(drain);
            task
        };
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)?;
        assert_eq!(client.read(&mut [0; 1]).await?, 0);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_joins_running_handlers_and_pending_handshakes() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (entered, mut entries) = tokio::sync::mpsc::channel(1);
    let (dropped, mut drops) = tokio::sync::mpsc::channel(1);
    let app = Router::new().route(
        "/",
        axum::routing::get(move || {
            let entered = entered.clone();
            let dropped = dropped.clone();
            async move {
                let _guard = HandlerDrop(dropped);
                let _ = entered.send(()).await;
                std::future::pending::<()>().await;
                "unreachable"
            }
        }),
    );
    let (_shutdown, shutdown_requested) = oneshot::channel();
    let (cancel, cancellation) = watch::channel(HttpRequestCancellation::Running);
    let server = tokio::spawn(serve(listener, None, app, shutdown_requested, cancellation));
    let mut client = TcpStream::connect(address).await?;
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    entries
        .recv()
        .await
        .ok_or_else(|| io::Error::other("Handler did not start"))?;
    cancel.send_replace(HttpRequestCancellation::Cancelled);
    server.await.map_err(io::Error::other)??;
    assert_eq!(drops.try_recv(), Ok(()));
    assert_cancelled_connection(&mut client).await?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (_shutdown, shutdown_requested) = oneshot::channel();
    let (cancel, cancellation) = watch::channel(HttpRequestCancellation::Running);
    let server = tokio::spawn(serve(
        listener,
        Some(tls_config(Duration::from_secs(10))?),
        Router::new(),
        shutdown_requested,
        cancellation,
    ));
    let mut pending = TcpStream::connect(address).await?;
    tokio::task::yield_now().await;
    cancel.send_replace(HttpRequestCancellation::Cancelled);
    server.await.map_err(io::Error::other)??;
    assert_cancelled_connection(&mut pending).await?;
    Ok(())
}

struct HandlerDrop(tokio::sync::mpsc::Sender<()>);

impl Drop for HandlerDrop {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

async fn assert_cancelled_connection(stream: &mut TcpStream) -> io::Result<()> {
    match stream.read(&mut [0; 1]).await {
        Ok(0) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => Ok(()),
        result => Err(io::Error::other(format!(
            "Cancelled connection remained open: {result:?}"
        ))),
    }
}
