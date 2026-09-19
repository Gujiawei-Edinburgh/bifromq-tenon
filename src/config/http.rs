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

//! Validates the HTTP listener and its optional TLS settings.

use super::RunnerConfigError;
use super::http_tls::{HttpTlsConfig, RawHttpTlsConfig};
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug)]
pub(super) struct HttpConfig {
    pub(super) listen_address: SocketAddr,
    pub(super) tls: Option<HttpTlsConfig>,
}

impl TryFrom<RawHttpConfig> for HttpConfig {
    type Error = RunnerConfigError;

    fn try_from(raw: RawHttpConfig) -> Result<Self, Self::Error> {
        let listen_address: SocketAddr = raw
            .listen_address
            .parse()
            .map_err(|_| RunnerConfigError::value_invalid("/http/listenAddress"))?;
        if listen_address.port() == 0 {
            return Err(RunnerConfigError::value_invalid("/http/listenAddress"));
        }
        Ok(Self {
            listen_address,
            tls: raw.tls.map(HttpTlsConfig::try_from).transpose()?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RawHttpConfig {
    listen_address: String,
    tls: Option<RawHttpTlsConfig>,
}
