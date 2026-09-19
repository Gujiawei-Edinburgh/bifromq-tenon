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

//! Retains exact unified Store entries and projects their Pipeline runtime material.
//!
//! Runtime Integrity supplies already selected Entries from one Store borrow.
//! Each exact identity retains its original immutable Entry until this snapshot
//! is dropped; wire material is built on demand rather than cached.

use crate::contracts::core::{PluginInterface as ProtocolPluginInterface, PluginProgramRuntime};
use crate::identifiers::{ExactVersion, ProgramName};
use crate::payload_contract::PluginInterface;
use crate::runner::plugin::store::PluginProgramEntry;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Owns one reference per exact Program selected for Pipeline runtime material.
#[derive(Debug)]
pub(super) struct PipelineProgramSnapshot {
    programs: HashMap<(ProgramName, ExactVersion), Arc<PluginProgramEntry>>,
}

impl PipelineProgramSnapshot {
    /// Retains the resolver's already selected, deduplicated Entry references.
    pub(super) fn retain<'a>(
        entries: impl IntoIterator<
            Item = (
                (&'a ProgramName, &'a ExactVersion),
                &'a Arc<PluginProgramEntry>,
            ),
        >,
    ) -> Self {
        let programs = entries
            .into_iter()
            .map(|((name, version), entry)| ((name.clone(), version.clone()), Arc::clone(entry)))
            .collect();
        Self { programs }
    }

    /// Projects retained Entries in canonical Program name and exact-version order.
    #[must_use]
    pub(super) fn runtimes(&self) -> Vec<PluginProgramRuntime> {
        let mut runtimes: Vec<_> = self
            .programs
            .iter()
            .map(
                |((program_name, exact_version), program)| PluginProgramRuntime {
                    program_name: program_name.as_str().to_owned(),
                    exact_version: exact_version.as_str().to_owned(),
                    program_directory: revision_absolute_utf8_path(program.directory()).to_owned(),
                    command: program.command().to_vec(),
                    plugin_interface: match program.interface() {
                        PluginInterface::Source => ProtocolPluginInterface::Source,
                        PluginInterface::Sink => ProtocolPluginInterface::Sink,
                        PluginInterface::SourceAndSink => ProtocolPluginInterface::SourceAndSink,
                    } as i32,
                    payload_descriptor_set: program.payload_descriptor_bytes().to_vec(),
                },
            )
            .collect();
        runtimes.sort_unstable_by(|left, right| {
            (&left.program_name, &left.exact_version)
                .cmp(&(&right.program_name, &right.exact_version))
        });
        runtimes
    }
}

#[allow(
    clippy::expect_used,
    reason = "installed Plugin paths descend from the validated UTF-8 Runner state root"
)]
pub(super) fn revision_absolute_utf8_path(path: &Path) -> &str {
    assert!(
        path.is_absolute(),
        "Runner-owned Plugin directories must remain absolute"
    );
    path.to_str()
        .expect("Runner-owned Plugin directories must remain UTF-8")
}

#[cfg(test)]
mod tests;
