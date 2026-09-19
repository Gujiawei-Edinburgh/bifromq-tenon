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

//! Program Store projections and persistent Document references.

use super::RunnerManagementState;
use crate::identifiers::{ExactVersion, ProgramName, TenonDocumentId};
use crate::runner::management::{
    PluginProgramInterfaceFilter, PluginProgramResource, PluginProgramView, PluginResourceKind,
};

impl RunnerManagementState {
    /// Projects sorted summaries from the Store without reading package files.
    #[must_use]
    pub(super) fn program_entries(
        &self,
        interface: Option<PluginProgramInterfaceFilter>,
    ) -> Box<[PluginProgramView]> {
        let mut programs = self
            .program_store()
            .programs()
            .filter(|(_, _, entry)| {
                interface.is_none_or(|filter| filter.matches(entry.interface()))
            })
            .map(|(program_name, exact_version, entry)| PluginProgramView {
                program_name: program_name.clone(),
                exact_version: exact_version.clone(),
                display_name: entry.display_name().to_owned(),
                description: entry.description().to_owned(),
                interface: entry.interface(),
                platforms: entry.platforms().into(),
            })
            .collect::<Vec<_>>();
        programs.sort_unstable_by(|left, right| {
            left.program_name
                .as_str()
                .cmp(right.program_name.as_str())
                .then_with(|| {
                    left.exact_version
                        .as_str()
                        .cmp(right.exact_version.as_str())
                })
        });
        programs.into_boxed_slice()
    }

    /// Copies an available summary or contract material for the requesting adapter.
    #[must_use]
    pub(super) fn program_resource(
        &self,
        program_name: &ProgramName,
        exact_version: &ExactVersion,
        resource: PluginResourceKind,
    ) -> Option<PluginProgramResource> {
        let entry = self.program_store().lookup(program_name, exact_version)?;
        Some(match resource {
            PluginResourceKind::Entry => PluginProgramResource::Entry(PluginProgramView {
                program_name: program_name.clone(),
                exact_version: exact_version.clone(),
                display_name: entry.display_name().to_owned(),
                description: entry.description().to_owned(),
                interface: entry.interface(),
                platforms: entry.platforms().into(),
            }),
            PluginResourceKind::ConfigSchema => {
                PluginProgramResource::ConfigSchema(entry.config_schema_bytes().into())
            }
            PluginResourceKind::PayloadContract => {
                PluginProgramResource::PayloadContract(entry.payload_descriptor_bytes().into())
            }
        })
    }

    /// Lists each persistent Document referencing this exact Program once.
    pub(super) fn documents_referencing_program(
        &self,
        program_name: &ProgramName,
        exact_version: &ExactVersion,
    ) -> Vec<TenonDocumentId> {
        let mut references = self
            .documents
            .iter()
            .filter(|(_, state)| {
                state.document.plugin_instances().values().any(|instance| {
                    instance.program_name() == program_name
                        && instance.exact_version() == exact_version
                })
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        references.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        references
    }
}
