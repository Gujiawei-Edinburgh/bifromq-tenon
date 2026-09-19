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

//! Canonical package platform pairs and the Runner compilation target.

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// One complete GOOS/GOARCH pair; the manifest Schema owns the vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
pub(crate) struct Platform {
    os: Cow<'static, str>,
    architecture: Cow<'static, str>,
}

impl Platform {
    /// The binary target, independent of the build host and runtime environment.
    pub(crate) const CURRENT: Self = Self {
        #[cfg(target_os = "linux")]
        os: Cow::Borrowed("linux"),
        #[cfg(target_os = "macos")]
        os: Cow::Borrowed("darwin"),
        #[cfg(target_arch = "x86_64")]
        architecture: Cow::Borrowed("amd64"),
        #[cfg(target_arch = "aarch64")]
        architecture: Cow::Borrowed("arm64"),
    };
}

#[cfg(not(all(
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
compile_error!("Runner requires a supported Linux or macOS AMD64 or ARM64 target");

impl std::fmt::Display for Platform {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.os, self.architecture)
    }
}
