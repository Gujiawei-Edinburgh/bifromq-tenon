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
use std::fs;
use std::io;

#[test]
fn identity_files_are_loaded_and_validated_as_one_snapshot() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let files = HttpTlsConfig {
        client_ca_file: None,
        certificate_chain_file: directory.path().join("certificate.pem"),
        private_key_file: directory.path().join("key.pem"),
        handshake_timeout: Duration::from_secs(10),
    };
    let certificate = include_bytes!("../../../../tests/fixtures/tls/server-a.pem");
    let key = include_bytes!("../../../../tests/fixtures/tls/server-a-key.pem");

    assert!(matches!(
        load(&files),
        Err(TlsIdentityError::Certificate(_))
    ));
    fs::write(&files.certificate_chain_file, certificate)?;
    assert!(matches!(load(&files), Err(TlsIdentityError::PrivateKey(_))));
    fs::write(&files.private_key_file, b"not a PEM private key")?;
    assert!(matches!(load(&files), Err(TlsIdentityError::PrivateKey(_))));
    fs::write(
        &files.private_key_file,
        include_bytes!("../../../../tests/fixtures/tls/server-b-key.pem"),
    )?;
    assert!(matches!(load(&files), Err(TlsIdentityError::Identity(_))));
    fs::write(&files.private_key_file, key)?;
    let loaded = load(&files).map_err(io::Error::other)?;
    assert_eq!(loaded.identity.alpn_protocols, [b"http/1.1"]);
    for invalid in [
        b"".as_slice(),
        b"-----BEGIN CERTIFICATE-----\n!\n-----END CERTIFICATE-----",
    ] {
        fs::write(&files.certificate_chain_file, invalid)?;
        assert!(load(&files).is_err());
    }
    fs::remove_file(&files.private_key_file)?;
    // The loaded snapshot owns its parsed key and never needs the files again.
    rustls::ServerConnection::new(loaded.identity).map_err(io::Error::other)?;
    Ok(())
}
