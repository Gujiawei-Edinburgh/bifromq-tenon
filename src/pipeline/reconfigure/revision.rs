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

//! Reconstructs the same-build Runner's complete revision for Pipeline use.
//!
//! Runner owns Document, Program-set, config, and Payload Contract validation.
//! This boundary consumes the wire message and rebuilds its Document, Program
//! lookup, and descriptor graphs without rereading files or repeating that work.
//! It owns no process, Queue, task, or apply state.

use crate::contracts::core::PipelineRevisionPlan;
use crate::identifiers::{ExactVersion, PluginInstanceId, PluginProgramIdentity, ProgramName};
use crate::payload_contract::PluginProgramPayloadContract;
use crate::tenon_document::verified::VerifiedTenonDocument;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// One revision's reconstructed Document and exact Program materials.
#[derive(Debug)]
pub(crate) struct PipelineRevision {
    // Runner hashes authored JSONC bytes, which this semantic Document cannot recover.
    document_etag: Box<str>,
    document: VerifiedTenonDocument,
    programs: HashMap<PluginProgramIdentity, ProgramRuntime>,
}

impl PipelineRevision {
    /// Reconstructs the sole same-build producer's already verified material.
    pub(crate) fn from_runner(revision: PipelineRevisionPlan) -> Self {
        Self {
            document_etag: revision.document_etag.into_boxed_str(),
            document: VerifiedTenonDocument::from_runner_json(&revision.tenon_document_json),
            programs: revision
                .plugin_programs
                .into_iter()
                .map(|program| {
                    (
                        PluginProgramIdentity::from_parts(
                            ProgramName::from_verified(program.program_name),
                            ExactVersion::from_verified(program.exact_version),
                        ),
                        ProgramRuntime {
                            program_directory: PathBuf::from(program.program_directory),
                            command: program.command.into_boxed_slice(),
                            payload_contract: PluginProgramPayloadContract::from_runner_bytes(
                                program.payload_descriptor_set,
                            ),
                        },
                    )
                })
                .collect(),
        }
    }

    pub(crate) fn document_etag(&self) -> &str {
        &self.document_etag
    }

    pub(crate) const fn document(&self) -> &VerifiedTenonDocument {
        &self.document
    }

    pub(crate) fn programs(&self) -> &HashMap<PluginProgramIdentity, ProgramRuntime> {
        &self.programs
    }

    #[allow(
        clippy::expect_used,
        reason = "the same-build received revision retains every referenced Program material"
    )]
    pub(in crate::pipeline::reconfigure) fn program_for_instance(
        &self,
        instance_id: &PluginInstanceId,
    ) -> &ProgramRuntime {
        let instance = &self.document.plugin_instances()[instance_id];
        self.programs
            .get(instance.program_identity())
            .expect("a received Plugin Instance must retain its Program material")
    }
}

/// One Program's immutable material; its identity exists only in the owning map.
#[derive(Debug)]
pub(crate) struct ProgramRuntime {
    program_directory: PathBuf,
    command: Box<[String]>,
    payload_contract: PluginProgramPayloadContract,
}

impl ProgramRuntime {
    pub(crate) fn program_directory(&self) -> &Path {
        &self.program_directory
    }

    pub(crate) fn command(&self) -> &[String] {
        &self.command
    }

    pub(crate) const fn payload_contract(&self) -> &PluginProgramPayloadContract {
        &self.payload_contract
    }
}
