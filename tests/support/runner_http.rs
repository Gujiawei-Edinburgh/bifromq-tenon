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

#![allow(
    dead_code,
    reason = "each integration-test binary uses a different subset of this shared fixture"
)]

use rustix::process::{Pid, Signal, kill_process};
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, str, thread};

pub(crate) const DEADLINE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
pub(crate) enum TestTransport {
    Http,
    Https,
    MutualTls,
}

impl TestTransport {
    pub(crate) fn configure(self, config_path: &Path) -> io::Result<()> {
        if let Self::Https | Self::MutualTls = self {
            let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
            set_tls_identity(
                config_path,
                &fixtures.join("server-a.pem"),
                &fixtures.join("server-a-key.pem"),
            )?;
            if let Self::MutualTls = self {
                set_client_ca(config_path, &fixtures.join("client-ca-a.pem"))?;
            }
        }
        Ok(())
    }

    pub(crate) fn connect(self, address: SocketAddr) -> io::Result<TestConnection> {
        match self {
            Self::Http => Ok(TestConnection::Http(connect_socket(address)?)),
            Self::Https | Self::MutualTls => {
                let config = match self {
                    Self::MutualTls => tls_client_config_with_identity(
                        rustls::DEFAULT_VERSIONS,
                        include_bytes!("../fixtures/tls/client-a.pem"),
                    )?,
                    _ => tls_client_config(rustls::DEFAULT_VERSIONS)?,
                };
                Ok(TestConnection::Https(Box::new(connect_tls(
                    address,
                    Arc::new(config),
                )?)))
            }
        }
    }

    pub(crate) fn request(
        self,
        address: SocketAddr,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> io::Result<HttpResponse> {
        request_on_stream(
            &mut self.connect(address)?,
            address,
            method,
            path,
            headers,
            body,
        )
    }
}

fn connect_socket(address: SocketAddr) -> io::Result<TcpStream> {
    let socket = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    socket.set_read_timeout(Some(DEADLINE))?;
    socket.set_write_timeout(Some(DEADLINE))?;
    Ok(socket)
}

pub(crate) fn connect_tls(
    address: SocketAddr,
    config: Arc<rustls::ClientConfig>,
) -> io::Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    let socket = connect_socket(address)?;
    let connection =
        rustls::ClientConnection::new(config, "localhost".try_into().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    Ok(rustls::StreamOwned::new(connection, socket))
}

pub(crate) fn tls_client_config(
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> io::Result<rustls::ClientConfig> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject as _;
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(
            CertificateDer::from_pem_slice(include_bytes!("../fixtures/tls/ca.pem"))
                .map_err(io::Error::other)?,
        )
        .map_err(io::Error::other)?;
    let mut config = rustls::ClientConfig::builder_with_protocol_versions(versions)
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

pub(crate) fn tls_client_config_with_identity(
    versions: &[&'static rustls::SupportedProtocolVersion],
    certificate: &[u8],
) -> io::Result<rustls::ClientConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
    let mut config = tls_client_config(versions)?;
    let certificates = CertificateDer::pem_slice_iter(certificate)
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)?;
    let key = PrivateKeyDer::from_pem_slice(include_bytes!("../fixtures/tls/client-key.pem"))
        .map_err(io::Error::other)?;
    let identity =
        rustls::sign::CertifiedKey::from_der(certificates, key, config.crypto_provider())
            .map_err(io::Error::other)?;
    config.client_auth_cert_resolver = Arc::new(rustls::sign::SingleCertAndKey::from(identity));
    Ok(config)
}

pub(crate) fn set_client_ca(config_path: &Path, client_ca: &Path) -> io::Result<()> {
    let mut config: serde_json::Value = serde_json::from_slice(&fs::read(config_path)?)?;
    config["http"]["tls"]["clientCaFile"] = client_ca.to_string_lossy().as_ref().into();
    fs::write(config_path, serde_json::to_vec(&config)?)
}

pub(crate) enum TestConnection {
    Http(TcpStream),
    Https(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl TestConnection {
    pub(crate) fn socket(&self) -> &TcpStream {
        match self {
            Self::Http(socket) => socket,
            Self::Https(stream) => &stream.sock,
        }
    }
}

impl Read for TestConnection {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Http(stream) => stream.read(bytes),
            Self::Https(stream) => stream.read(bytes),
        }
    }
}

impl Write for TestConnection {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Http(stream) => stream.write(bytes),
            Self::Https(stream) => stream.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Http(stream) => stream.flush(),
            Self::Https(stream) => stream.flush(),
        }
    }
}

pub(crate) fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> io::Result<HttpResponse> {
    request_on_stream(
        &mut TcpStream::connect_timeout(&address, Duration::from_secs(2))?,
        address,
        method,
        path,
        headers,
        body,
    )
}

pub(crate) fn request_on_stream(
    stream: &mut (impl Read + Write),
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> io::Result<HttpResponse> {
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    )?;
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(body)?;
    stream.flush()?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    HttpResponse::parse(bytes)
}

pub(crate) fn read_continue_response(stream: &mut impl Read) -> io::Result<()> {
    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    while !response.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte)?;
        response.push(byte[0]);
        if response.len() > 1024 {
            return Err(io::Error::other("HTTP interim response is too large"));
        }
    }
    if response != b"HTTP/1.1 100 Continue\r\n\r\n" {
        return Err(io::Error::other(format!(
            "Plugin upload was not admitted: {}",
            String::from_utf8_lossy(&response),
        )));
    }
    Ok(())
}

pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: Vec<u8>,
}

impl HttpResponse {
    pub(crate) fn parse(bytes: Vec<u8>) -> io::Result<Self> {
        let boundary = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or_else(|| io::Error::other("HTTP response headers are incomplete"))?;
        let head = str::from_utf8(&bytes[..boundary]).map_err(io::Error::other)?;
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split_ascii_whitespace().nth(1))
            .ok_or_else(|| io::Error::other("HTTP status is missing"))?
            .parse::<u16>()
            .map_err(io::Error::other)?;
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
            .collect();
        Ok(Self {
            status,
            headers,
            body: bytes[boundary + 4..].to_vec(),
        })
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    pub(crate) fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

pub(crate) struct TestRunner {
    child: Child,
}

impl TestRunner {
    pub(crate) fn spawn(config: &Path) -> io::Result<Self> {
        Self::spawn_command(Self::command(config)?)
    }

    pub(crate) fn spawn_with_executable(config: &Path, executable: &Path) -> io::Result<Self> {
        let mut command = Command::new(executable);
        command.arg("--config").arg(config);
        Self::spawn_command(command)
    }

    pub(crate) fn wait_for_failure(&mut self, code: &str) -> io::Result<()> {
        let Err(failure) = self.wait_for_exit() else {
            return Err(io::Error::other("Runner unexpectedly succeeded"));
        };
        assert!(failure.to_string().contains(code), "{failure}");
        Ok(())
    }

    pub(crate) fn spawn_with_path(config: &Path, executable_path: &Path) -> io::Result<Self> {
        let mut command = Self::command(config)?;
        command
            .env_remove("JAVA_HOME")
            .env_remove("JDK_HOME")
            .env("PATH", executable_path);
        Self::spawn_command(command)
    }

    pub(crate) fn spawn_command(mut command: Command) -> io::Result<Self> {
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        Ok(Self { child })
    }

    pub(crate) fn terminate(&mut self) -> io::Result<()> {
        self.signal_terminate()?;
        self.wait_for_exit()
    }

    pub(crate) fn signal_terminate(&self) -> io::Result<()> {
        let pid = Pid::from_raw(i32::try_from(self.child.id()).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("Runner process id is invalid"))?;
        kill_process(pid, Signal::TERM).map_err(io::Error::from)
    }

    pub(crate) fn wait_for_exit(&mut self) -> io::Result<()> {
        let deadline = Instant::now() + DEADLINE;
        loop {
            if let Some(status) = self.child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                return Err(self.exit_failure("Runner failed", status)?);
            }
            if Instant::now() >= deadline {
                self.child.kill()?;
                return Err(io::Error::other("Runner did not stop before the deadline"));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub(crate) fn read_stderr(&mut self) -> io::Result<String> {
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            pipe.read_to_string(&mut stderr)?;
        }
        Ok(stderr)
    }

    fn command(config: &Path) -> io::Result<Command> {
        let executable = std::env::var_os("TENON_TEST_RUNNER_BINARY")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_tenon").into());
        let mut command = Command::new(executable);
        command.args([
            "--config",
            config
                .to_str()
                .ok_or_else(|| io::Error::other("Config path is not UTF-8"))?,
        ]);
        Ok(command)
    }

    fn exit_failure(&mut self, context: &str, status: ExitStatus) -> io::Result<io::Error> {
        let stderr = self.read_stderr()?;
        Ok(io::Error::other(format!(
            "{context} with {status}; stderr:\n{stderr}"
        )))
    }
}

impl Drop for TestRunner {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

pub(crate) fn wait_for_http(runner: &mut TestRunner, address: SocketAddr) -> io::Result<()> {
    wait_for_server(runner, address, TestTransport::Http)
}

pub(crate) fn wait_for_server(
    runner: &mut TestRunner,
    address: SocketAddr,
    transport: TestTransport,
) -> io::Result<()> {
    wait_for_response(runner, || {
        transport.request(address, "GET", "/document-schema", &[], &[])
    })
}

pub(crate) fn wait_for_response(
    runner: &mut TestRunner,
    mut probe: impl FnMut() -> io::Result<HttpResponse>,
) -> io::Result<()> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if matches!(
            probe(),
            Ok(response) if response.status == 200
        ) {
            return Ok(());
        }
        if let Some(status) = runner.child.try_wait()? {
            return Err(runner.exit_failure("Runner exited before HTTP became ready", status)?);
        }
        if Instant::now() >= deadline {
            runner.child.kill()?;
            let status = runner.child.wait()?;
            return Err(runner.exit_failure(
                "Runner did not make HTTP ready before the deadline and was killed",
                status,
            )?);
        }
        thread::sleep(Duration::from_millis(20));
    }
}

pub(crate) fn wait_until(mut operation: impl FnMut() -> io::Result<Option<()>>) -> io::Result<()> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if operation()?.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(
                "Condition did not become true before the deadline",
            ));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

pub(crate) fn available_address() -> io::Result<SocketAddr> {
    // Run these process tests with --test-threads=1, as in verify-rust.sh.
    TcpListener::bind("127.0.0.1:0")?.local_addr()
}

pub(crate) fn write_config(state_directory: &Path, address: SocketAddr) -> io::Result<PathBuf> {
    let path = state_directory.join("runner.jsonc");
    let state_directory = serde_json::to_string(&state_directory.to_string_lossy())?;
    fs::write(
        &path,
        format!(
            r#"{{"stateDirectory":{state_directory},"http":{{"listenAddress":"{address}"}},"pipeline":{{"startupTimeoutMs":2000,"shutdownTimeoutMs":2000,"retryBackoff":{{"initialDelayMs":10,"maximumDelayMs":20}}}},"lua":{{"cpuTimeLimitMs":50,"memoryLimitBytes":16777216}}}}"#
        ),
    )?;
    Ok(path)
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn set_tls_identity(
    config_path: &Path,
    certificate_file: &Path,
    key_file: &Path,
) -> io::Result<()> {
    let mut config: serde_json::Value = serde_json::from_slice(&fs::read(config_path)?)?;
    config["http"]["tls"] = serde_json::json!({
        "certificateChainFile": certificate_file,
        "privateKeyFile": key_file,
    });
    fs::write(config_path, serde_json::to_vec(&config)?)
}
