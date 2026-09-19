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

//! Loads immutable server identity and client trust before Runner state recovery.

use crate::config::HttpTlsConfig;
use pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Frozen HTTPS settings passed from startup to the connection owner.
#[derive(Clone)]
pub(crate) struct TlsServerConfig {
    pub(super) identity: Arc<ServerConfig>,
    pub(super) handshake_timeout: Duration,
}

pub(crate) fn load(settings: &HttpTlsConfig) -> Result<TlsServerConfig, TlsIdentityError> {
    let certificates = CertificateDer::pem_file_iter(&settings.certificate_chain_file)
        .and_then(Iterator::collect)
        .map_err(TlsIdentityError::Certificate)?;
    let key = PrivateKeyDer::from_pem_file(&settings.private_key_file)
        .map_err(TlsIdentityError::PrivateKey)?;
    let verifier = match &settings.client_ca_file {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            for certificate in
                CertificateDer::pem_file_iter(path).map_err(TlsIdentityError::ClientCaFile)?
            {
                roots
                    .add(certificate.map_err(TlsIdentityError::ClientCaFile)?)
                    .map_err(TlsIdentityError::ClientCaCertificate)?;
            }
            WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(TlsIdentityError::ClientVerifier)?
        }
        None => WebPkiClientVerifier::no_client_auth(),
    };
    let mut config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificates, key)
        .map_err(TlsIdentityError::Identity)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsServerConfig {
        identity: Arc::new(config),
        handshake_timeout: settings.handshake_timeout,
    })
}

#[derive(Debug)]
pub(crate) enum TlsIdentityError {
    Certificate(pem::Error),
    PrivateKey(pem::Error),
    Identity(rustls::Error),
    ClientCaFile(pem::Error),
    ClientCaCertificate(rustls::Error),
    ClientVerifier(rustls::server::VerifierBuilderError),
}

impl fmt::Display for TlsIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Certificate(_) => "Runner TLS certificate chain could not be loaded",
            Self::PrivateKey(_) => "Runner TLS private key could not be loaded",
            Self::Identity(_) => "Runner TLS certificate and private key are not a valid identity",
            Self::ClientCaFile(_) => "Runner TLS client CA file could not be loaded",
            Self::ClientCaCertificate(_) => "Runner TLS client CA certificate is invalid",
            Self::ClientVerifier(_) => "Runner TLS client certificate verifier could not be built",
        })
    }
}

impl Error for TlsIdentityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Certificate(source) | Self::PrivateKey(source) | Self::ClientCaFile(source) => {
                Some(source)
            }
            Self::Identity(source) | Self::ClientCaCertificate(source) => Some(source),
            Self::ClientVerifier(source) => Some(source),
        }
    }
}

#[cfg(test)]
mod tests;
