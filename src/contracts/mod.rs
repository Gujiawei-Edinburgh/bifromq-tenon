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

//! Data contracts shared by Tenon processes and external callers.

/// Internal contracts shared by Runner and Pipeline processes.
pub mod core {
    include!(concat!(env!("OUT_DIR"), "/tenon.core.rs"));

    /// Fixed UTF-8 byte boundary shared by every live diagnostic record.
    pub(crate) const DIAGNOSTIC_TEXT_MAXIMUM_BYTES: usize = 16 * 1_024;

    /// Bounds one diagnostic text to the shared boundary without splitting a
    /// UTF-8 character.
    #[must_use]
    pub(crate) fn bound_diagnostic_text(text: &str) -> (Box<str>, bool) {
        if text.len() <= DIAGNOSTIC_TEXT_MAXIMUM_BYTES {
            return (text.into(), false);
        }
        let mut boundary = DIAGNOSTIC_TEXT_MAXIMUM_BYTES;
        while !text.is_char_boundary(boundary) {
            boundary -= 1;
        }
        (text[..boundary].into(), true)
    }
}

/// Language-neutral contracts shared with Sink SDK implementations.
pub mod sink;

/// Language-neutral contracts shared with Source SDK implementations.
pub mod source {
    include!(concat!(env!("OUT_DIR"), "/tenon.source.rs"));
}

/// Machine-readable contracts for Tenon Documents.
pub mod tenon_document {
    static V1_SCHEMA_BYTES: &[u8] = include_bytes!("../../contracts/tenon-document/v1.schema.json");

    /// Returns the exact Tenon Document v1 Schema bytes embedded at compile time.
    ///
    /// The returned slice borrows immutable data from the library artifact without
    /// reading a runtime file or copying the Schema.
    #[must_use]
    pub fn v1_schema_bytes() -> &'static [u8] {
        V1_SCHEMA_BYTES
    }
}

/// Machine-readable contract for Source and Sink Plugin manifests.
pub(crate) mod plugin {
    include!(concat!(env!("OUT_DIR"), "/tenon.plugin.rs"));

    static MANIFEST_SCHEMA_BYTES: &[u8] =
        include_bytes!("../../contracts/plugin/manifest.schema.json");

    /// Returns the exact Plugin manifest Schema bytes embedded at compile time.
    #[must_use]
    pub(crate) fn manifest_schema_bytes() -> &'static [u8] {
        MANIFEST_SCHEMA_BYTES
    }
}

/// Machine-readable contract for the Runner startup configuration.
pub(crate) mod runner {
    static CONFIG_SCHEMA_BYTES: &[u8] = include_bytes!("../../contracts/runner/config.schema.json");

    /// Returns the exact Runner configuration Schema bytes embedded at compile time.
    #[must_use]
    pub(crate) fn config_schema_bytes() -> &'static [u8] {
        CONFIG_SCHEMA_BYTES
    }
}
