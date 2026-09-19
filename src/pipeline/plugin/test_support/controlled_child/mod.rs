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

//! Launch owners and process observations for the shared controlled child fixture.

use rustix::io::Errno;
use rustix::process::{Pid, WaitOptions, waitpid};
use serde_json::Value;
use std::io;
use std::path::{Path, PathBuf};

use super::super::control::PluginControlLauncher;
use super::super::lifecycle::PluginBells;
use super::super::{ControlledPluginLaunch, PluginLaunch};
use crate::payload_contract::PluginInterface;

#[path = "../../../../../tests/support/controlled_plugin.rs"]
mod controlled_plugin;

pub(crate) fn controlled_program_command(interface: PluginInterface) -> io::Result<Vec<String>> {
    use crate::contracts::core::PluginInterface as WireInterface;
    controlled_plugin::controlled_program_command(match interface {
        PluginInterface::Source => WireInterface::Source,
        PluginInterface::Sink => WireInterface::Sink,
        PluginInterface::SourceAndSink => WireInterface::SourceAndSink,
    })
}

pub(in crate::pipeline::plugin) struct ControlledTestLaunch {
    program_directory: PathBuf,
    command: Vec<String>,
    working_directory: PathBuf,
    config: Value,
    interface: PluginInterface,
}

impl ControlledTestLaunch {
    pub(in crate::pipeline::plugin) fn new(
        root: &Path,
        name: &str,
        interface: PluginInterface,
        config: Value,
    ) -> io::Result<Self> {
        let working_directory = root.join(name);
        std::fs::create_dir_all(&working_directory)?;
        Ok(Self {
            program_directory: root.to_owned(),
            command: controlled_program_command(interface)?,
            working_directory,
            config,
            interface,
        })
    }

    pub(in crate::pipeline::plugin) fn borrowed<'a>(
        &'a self,
        control: &'a PluginControlLauncher,
    ) -> ControlledPluginLaunch<'a> {
        ControlledPluginLaunch::new(
            PluginLaunch {
                program_directory: &self.program_directory,
                command: &self.command,
                working_directory: self.working_directory.clone(),
                config: &self.config,
                extra_args: None,
                env: None,
                // The child reads one Channel-region record whenever it serves
                // as a Source, and one input list whenever it serves as a Sink,
                // so every launch names exactly the directions it will use.
                bells: PluginBells {
                    source_channel_region: matches!(
                        self.interface,
                        PluginInterface::Source | PluginInterface::SourceAndSink
                    )
                    .then(|| self.working_directory.join("channels.bells")),
                    sink_inputs: matches!(
                        self.interface,
                        PluginInterface::Sink | PluginInterface::SourceAndSink
                    )
                    .then(Vec::new),
                },
            },
            self.interface,
            control,
        )
    }

    pub(in crate::pipeline::plugin) fn working_directory(&self) -> &Path {
        &self.working_directory
    }
}

pub(in crate::pipeline) fn recorded_pid(working_directory: &Path) -> io::Result<Pid> {
    let raw = std::fs::read_to_string(working_directory.join("process.pid"))?;
    Pid::from_raw(
        raw.trim()
            .parse()
            .map_err(|_| io::Error::other("Controlled child PID is invalid"))?,
    )
    .ok_or_else(|| io::Error::other("Controlled child PID is invalid"))
}

pub(in crate::pipeline) fn assert_reaped(process_id: Pid) -> io::Result<()> {
    if waitpid(Some(process_id), WaitOptions::NOHANG).err() == Some(Errno::CHILD) {
        Ok(())
    } else {
        Err(io::Error::other("Controlled child was not reaped"))
    }
}

pub(in crate::pipeline) fn lifecycle_events(working_directory: &Path) -> io::Result<String> {
    std::fs::read_to_string(working_directory.join("lifecycle.received"))
}
