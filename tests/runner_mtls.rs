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
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

#[path = "support/runner_http.rs"]
mod runner_http_support;

use runner_http_support::{
    HttpResponse, TestRunner, TestTransport, available_address, connect_tls, request_on_stream,
    set_client_ca, sha256_hex, tls_client_config, tls_client_config_with_identity,
    wait_for_response, wait_for_server, write_config,
};

const CLIENT_A: &[u8] = include_bytes!("fixtures/tls/client-a.pem");
const CLIENT_B: &[u8] = include_bytes!("fixtures/tls/client-b.pem");
const CA_A: &[u8] = include_bytes!("fixtures/tls/client-ca-a.pem");
const CA_B: &[u8] = include_bytes!("fixtures/tls/client-ca-b.pem");

#[test]
fn client_authentication_precedes_http_writes_for_both_tls_versions() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    TestTransport::MutualTls.configure(&config)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_server(&mut runner, address, TestTransport::MutualTls)?;
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let trusted = Arc::new(tls_client_config_with_identity(&[version], CLIENT_A)?);
        // Reuse the client cache and read all response bytes, including TLS 1.3 tickets.
        for expected in [rustls::HandshakeKind::Full, rustls::HandshakeKind::Resumed] {
            let mut stream = connect_tls(address, Arc::clone(&trusted))?;
            assert_eq!(
                request_on_stream(&mut stream, address, "GET", "/document-schema", &[], &[])?
                    .status,
                200
            );
            assert_eq!(stream.conn.handshake_kind(), Some(expected));
            assert_eq!(stream.conn.protocol_version(), Some(version.version));
        }
        let id = if version.version == rustls::ProtocolVersion::TLSv1_2 {
            "tls12"
        } else {
            "tls13"
        };
        let path = format!("/documents/{id}");
        let source = serde_json::json!({
            "specVersion": "1", "id": id,
            "pluginInstances": {"gateway": {"programName": "com.example.gateway", "exactVersion": "1.0.0", "config": {}}},
            "flows": {"loop": {"source": "gateway", "process": {"script": "function main(event) emit() end"}, "sinks": ["gateway"]}}
        }).to_string();
        let headers = [
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ];
        for certificate in [
            None,
            Some(CLIENT_B),
            Some(include_bytes!("fixtures/tls/client-expired.pem").as_slice()),
            Some(include_bytes!("fixtures/tls/client-future.pem").as_slice()),
            Some(include_bytes!("fixtures/tls/client-server-only.pem").as_slice()),
        ] {
            let client = Arc::new(match certificate {
                Some(certificate) => tls_client_config_with_identity(&[version], certificate)?,
                None => tls_client_config(&[version])?,
            });
            let error = request_on_stream(
                &mut connect_tls(address, client)?,
                address,
                "PUT",
                &path,
                &headers,
                source.as_bytes(),
            )
            .err()
            .ok_or_else(|| io::Error::other("Unauthenticated request entered HTTP"))?;
            assert!(
                matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                ),
                "{error}"
            );
            assert_eq!(get(address, Arc::clone(&trusted), &path)?.status, 404);
            assert!(
                !directory
                    .path()
                    .join("tenon-documents")
                    .join(format!("{}.jsonc", sha256_hex(id.as_bytes())))
                    .exists()
            );
        }
        let created = request_on_stream(
            &mut connect_tls(address, Arc::clone(&trusted))?,
            address,
            "PUT",
            &path,
            &headers,
            source.as_bytes(),
        )?;
        assert_eq!(created.status, 201, "{}", created.body_text());
        assert!(
            directory
                .path()
                .join("tenon-documents")
                .join(format!("{}.jsonc", sha256_hex(id.as_bytes())))
                .exists()
        );
        assert_eq!(get(address, trusted, &path)?.body, source.as_bytes());
        let no_eku = Arc::new(tls_client_config_with_identity(
            &[version],
            include_bytes!("fixtures/tls/client-no-eku.pem"),
        )?);
        assert_eq!(get(address, no_eku, "/document-schema")?.status, 200);
    }
    runner.terminate()
}

#[test]
fn multiple_client_trust_anchors_accept_direct_and_intermediate_chains() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    TestTransport::MutualTls.configure(&config)?;
    let ca_file = directory.path().join("client-ca.pem");
    fs::write(&ca_file, [CA_A, CA_B].concat())?;
    set_client_ca(&config, &ca_file)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_server(&mut runner, address, TestTransport::MutualTls)?;
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        for certificate in [CLIENT_A, CLIENT_B] {
            let client = Arc::new(tls_client_config_with_identity(&[version], certificate)?);
            assert_eq!(get(address, client, "/document-schema")?.status, 200);
        }
    }
    runner.terminate()
}

#[test]
fn replacing_client_ca_requires_restart_and_invalidates_old_sessions() -> io::Result<()> {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        TestTransport::MutualTls.configure(&config)?;
        let ca_file = directory.path().join("client-ca.pem");
        fs::write(&ca_file, CA_A)?;
        set_client_ca(&config, &ca_file)?;
        let client_a = Arc::new(tls_client_config_with_identity(&[version], CLIENT_A)?);
        let client_b = Arc::new(tls_client_config_with_identity(&[version], CLIENT_B)?);
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_response(&mut runner, || {
            get(address, Arc::clone(&client_a), "/document-schema")
        })?;
        assert!(get(address, Arc::clone(&client_b), "/document-schema").is_err());
        fs::write(&ca_file, CA_B)?;
        for expected in [rustls::HandshakeKind::Resumed, rustls::HandshakeKind::Full] {
            let client = if expected == rustls::HandshakeKind::Full {
                Arc::new(tls_client_config_with_identity(&[version], CLIENT_A)?)
            } else {
                Arc::clone(&client_a)
            };
            let mut stream = connect_tls(address, client)?;
            assert_eq!(
                request_on_stream(&mut stream, address, "GET", "/document-schema", &[], &[])?
                    .status,
                200
            );
            assert_eq!(stream.conn.handshake_kind(), Some(expected));
        }
        assert!(get(address, Arc::clone(&client_b), "/document-schema").is_err());
        runner.terminate()?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_response(&mut runner, || {
            get(address, Arc::clone(&client_b), "/document-schema")
        })?;
        let error = get(address, client_a, "/document-schema")
            .err()
            .ok_or_else(|| io::Error::other("Old client session bypassed the new trust anchors"))?;
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::InvalidData
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
            ),
            "{error}"
        );
        runner.terminate()?;
    }
    Ok(())
}

#[test]
fn invalid_client_ca_stops_startup_before_state_recovery() -> io::Result<()> {
    let bad_pem = b"-----BEGIN CERTIFICATE-----\n!\n-----END CERTIFICATE-----\n";
    let bad_der = b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n";
    // Distinguish PEM decoding from DER validation, and never ignore bad entries.
    assert!(CertificateDer::from_pem_slice(bad_der).is_ok());
    for contents in [
        None,
        Some(Vec::new()),
        Some(b"not a certificate".to_vec()),
        Some(bad_pem.to_vec()),
        Some(bad_der.to_vec()),
        Some([CA_A, bad_pem].concat()),
        Some([CA_A, bad_der].concat()),
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        TestTransport::Https.configure(&config)?;
        let ca_file = directory.path().join("client-ca.pem");
        if let Some(contents) = contents {
            fs::write(&ca_file, contents)?;
        }
        set_client_ca(&config, &ca_file)?;
        let mut runner = TestRunner::spawn(&config)?;
        let error = runner
            .wait_for_exit()
            .err()
            .ok_or_else(|| io::Error::other("Invalid client CA was accepted"))?
            .to_string();
        assert!(error.contains("runner.tls_identity_invalid"), "{error}");
        assert!(!directory.path().join("tenon-documents").exists());
        assert!(!directory.path().join("plugins").exists());
        assert!(TcpStream::connect(address).is_err());
    }
    Ok(())
}

fn get(
    address: SocketAddr,
    config: Arc<rustls::ClientConfig>,
    path: &str,
) -> io::Result<HttpResponse> {
    request_on_stream(
        &mut connect_tls(address, config)?,
        address,
        "GET",
        path,
        &[],
        &[],
    )
}
