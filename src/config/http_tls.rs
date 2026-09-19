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

//! Validates HTTPS identity paths and the handshake deadline.

use super::RunnerConfigError;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

/// HTTPS identity paths and handshake deadline, validated without filesystem access.
#[derive(Debug)]
pub struct HttpTlsConfig {
    /// Absolute path to the server certificate chain in PEM format.
    pub certificate_chain_file: PathBuf,
    /// Absolute path to the matching unencrypted private key in PEM format.
    pub private_key_file: PathBuf,
    /// Optional absolute path to the client trust anchors in PEM format.
    pub client_ca_file: Option<PathBuf>,
    /// Total time allowed for one TLS handshake.
    pub handshake_timeout: Duration,
}

impl TryFrom<RawHttpTlsConfig> for HttpTlsConfig {
    type Error = RunnerConfigError;

    fn try_from(raw: RawHttpTlsConfig) -> Result<Self, Self::Error> {
        for (path, pointer) in [
            (
                &raw.certificate_chain_file,
                "/http/tls/certificateChainFile",
            ),
            (&raw.private_key_file, "/http/tls/privateKeyFile"),
        ]
        .into_iter()
        .chain(
            raw.client_ca_file
                .as_ref()
                .map(|path| (path, "/http/tls/clientCaFile")),
        ) {
            if !path.is_absolute() {
                return Err(RunnerConfigError::PathNotAbsolute { path: pointer });
            }
        }
        Ok(Self {
            certificate_chain_file: raw.certificate_chain_file,
            private_key_file: raw.private_key_file,
            client_ca_file: raw.client_ca_file,
            handshake_timeout: Duration::from_millis(raw.handshake_timeout_ms.unwrap_or(10_000)),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RawHttpTlsConfig {
    certificate_chain_file: PathBuf,
    private_key_file: PathBuf,
    client_ca_file: Option<PathBuf>,
    handshake_timeout_ms: Option<u64>,
}
