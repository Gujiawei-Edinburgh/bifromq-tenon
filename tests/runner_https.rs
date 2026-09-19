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

use rustls::pki_types::{CertificateDer, pem::PemObject as _};
use std::fs;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[path = "support/runner_http.rs"]
mod runner_http_support;

use runner_http_support::{
    DEADLINE, TestConnection, TestRunner, TestTransport, available_address, request,
    set_tls_identity, tls_client_config, wait_for_server, write_config,
};

#[test]
fn tls12_and_tls13_both_validate_the_complete_certificate_chain() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    TestTransport::Https.configure(&config)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_server(&mut runner, address, TestTransport::Https)?;
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let socket = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        socket.set_read_timeout(Some(DEADLINE))?;
        let connection = rustls::ClientConnection::new(
            Arc::new(tls_client_config(&[version])?),
            "localhost".try_into().map_err(io::Error::other)?,
        )
        .map_err(io::Error::other)?;
        let mut stream = rustls::StreamOwned::new(connection, socket);
        let response = runner_http_support::request_on_stream(
            &mut stream,
            address,
            "GET",
            "/document-schema",
            &[],
            &[],
        )?;
        assert_eq!(response.status, 200);
        assert_eq!(stream.conn.protocol_version(), Some(version.version));
        assert_eq!(stream.conn.alpn_protocol(), Some(b"http/1.1".as_slice()));
        assert_eq!(stream.conn.peer_certificates().map(<[_]>::len), Some(2));
    }
    runner.terminate()
}

#[test]
fn https_requires_trust_and_the_correct_server_name_and_never_serves_plaintext() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    TestTransport::Https.configure(&config)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_server(&mut runner, address, TestTransport::Https)?;

    for (client, name) in [
        (
            tls_client_config(rustls::DEFAULT_VERSIONS)?,
            "wrong.example",
        ),
        (
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
            "localhost",
        ),
    ] {
        let mut socket = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        socket.set_read_timeout(Some(DEADLINE))?;
        let mut connection = rustls::ClientConnection::new(
            Arc::new(client),
            name.try_into().map_err(io::Error::other)?,
        )
        .map_err(io::Error::other)?;
        let error = connection
            .complete_io(&mut socket)
            .err()
            .ok_or_else(|| io::Error::other("Untrusted TLS identity was accepted"))?;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    }
    assert!(request(address, "GET", "/document-schema", &[], &[]).is_err());
    let response = TestTransport::Https.request(address, "GET", "/document-schema", &[], &[])?;
    assert_eq!(response.status, 200);
    assert_eq!(response.headers["tenon-version"], env!("CARGO_PKG_VERSION"));
    runner.terminate()
}

#[test]
fn incomplete_handshakes_do_not_block_requests_or_runner_shutdown() -> io::Result<()> {
    for transport in [TestTransport::Https, TestTransport::MutualTls] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;
        let mut pending = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        pending.set_read_timeout(Some(Duration::from_secs(2)))?;
        pending.write_all(&[0x16])?;
        let response = transport.request(address, "GET", "/document-schema", &[], &[])?;
        assert_eq!(response.status, 200);
        let started = Instant::now();
        runner.terminate()?;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "Shutdown waited for the handshake deadline"
        );
        assert_eq!(pending.read(&mut [0; 1])?, 0);
    }
    Ok(())
}

#[test]
fn configured_handshake_timeout_closes_a_stalled_connection() -> io::Result<()> {
    for transport in [TestTransport::Https, TestTransport::MutualTls] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut source: serde_json::Value = serde_json::from_slice(&fs::read(&config)?)?;
        source["http"]["tls"]["handshakeTimeoutMs"] = 100.into();
        fs::write(&config, serde_json::to_vec(&source)?)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;
        let mut pending = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        pending.set_read_timeout(Some(Duration::from_secs(2)))?;
        pending.write_all(&[0x16])?;
        assert_eq!(pending.read(&mut [0; 1])?, 0);
        let response = transport.request(address, "GET", "/document-schema", &[], &[])?;
        assert_eq!(response.status, 200);
        runner.terminate()?;
    }
    Ok(())
}

#[test]
fn certificate_replacement_takes_effect_only_after_restart() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let certificate_file = directory.path().join("certificate.pem");
    let key_file = directory.path().join("key.pem");
    fs::write(
        &certificate_file,
        include_bytes!("fixtures/tls/server-a.pem"),
    )?;
    fs::write(&key_file, include_bytes!("fixtures/tls/server-a-key.pem"))?;
    set_tls_identity(&config, &certificate_file, &key_file)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_server(&mut runner, address, TestTransport::Https)?;
    let original = presented_certificate(address)?;
    assert_eq!(
        original,
        CertificateDer::from_pem_slice(include_bytes!("fixtures/tls/server-a.pem"))
            .map_err(io::Error::other)?
    );
    fs::write(
        &certificate_file,
        include_bytes!("fixtures/tls/server-b.pem"),
    )?;
    fs::write(&key_file, include_bytes!("fixtures/tls/server-b-key.pem"))?;
    assert_eq!(presented_certificate(address)?, original);
    runner.terminate()?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_server(&mut runner, address, TestTransport::Https)?;
    assert_eq!(
        presented_certificate(address)?,
        CertificateDer::from_pem_slice(include_bytes!("fixtures/tls/server-b.pem"))
            .map_err(io::Error::other)?
    );
    runner.terminate()
}

#[test]
fn invalid_identity_stops_startup_before_state_recovery_without_disclosing_key_bytes()
-> io::Result<()> {
    let certificate = include_bytes!("fixtures/tls/server-a.pem").as_slice();
    let key = include_bytes!("fixtures/tls/server-a-key.pem").as_slice();
    let confidential = b"invalid private key with confidential test marker";
    for (certificate, key) in [
        (None, Some(key)),
        (Some(certificate), None),
        (Some(b"invalid certificate".as_slice()), Some(key)),
        (Some(b"".as_slice()), Some(key)),
        (Some(certificate), Some(confidential.as_slice())),
        (
            Some(certificate),
            Some(include_bytes!("fixtures/tls/server-b-key.pem").as_slice()),
        ),
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        let certificate_file = directory.path().join("certificate.pem");
        let key_file = directory.path().join("key.pem");
        if let Some(certificate) = certificate {
            fs::write(&certificate_file, certificate)?;
        }
        if let Some(key) = key {
            fs::write(&key_file, key)?;
        }
        set_tls_identity(&config, &certificate_file, &key_file)?;
        let mut runner = TestRunner::spawn(&config)?;
        let error = runner
            .wait_for_exit()
            .err()
            .ok_or_else(|| io::Error::other("Invalid TLS identity was accepted"))?
            .to_string();
        assert!(error.contains("runner.tls_identity_invalid"), "{error}");
        assert!(!error.contains("confidential test marker"), "{error}");
        assert!(!directory.path().join("tenon-documents").exists());
        assert!(!directory.path().join("plugins").exists());
        assert!(TcpStream::connect(address).is_err());
    }
    Ok(())
}

fn presented_certificate(address: std::net::SocketAddr) -> io::Result<CertificateDer<'static>> {
    let TestConnection::Https(mut stream) = TestTransport::Https.connect(address)? else {
        return Err(io::Error::other(
            "HTTPS fixture created a plaintext connection",
        ));
    };
    let rustls::StreamOwned { conn, sock } = &mut *stream;
    conn.complete_io(sock)?;
    let certificate = conn
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .ok_or_else(|| io::Error::other("Server sent no certificate"))?
        .clone()
        .into_owned();
    conn.send_close_notify();
    stream.flush()?;
    Ok(certificate)
}
