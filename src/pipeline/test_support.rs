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

//! Cross-module fixtures for Pipeline unit tests.

pub(crate) use super::diagnostics::PipelineDiagnostics;
pub(crate) use super::plugin::test_support::controlled_program_command;
#[cfg(not(feature = "loom-model"))]
pub(crate) use super::plugin::test_support::{TEST_DEADLINE, wait_for_file};

pub(crate) use super::reconfigure::revision::PipelineRevision;

use crate::identifiers::PluginInstanceId;
use std::path::{Path, PathBuf};

pub(crate) fn instance_directory(
    root: &Path,
    id: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(super::reconfigure::test_support::instance_directory(
        root,
        &PluginInstanceId::try_from(id.to_owned())?,
    ))
}
