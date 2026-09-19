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

//! Owns the private runtime directory and executable image for one Runner.

use crate::runner::executable::{CapturedRunnerExecutable, EXECUTABLE_FILE_NAME};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const RUNNER_RUNTIME_PREFIX: &str = ".tenon-runner-";
const INCOMPLETE_RUNTIME_PREFIX: &str = ".tenon-cleanup-incomplete-";

/// Owns every file that must survive while a Pipeline process may survive.
pub(super) struct RunnerRuntimeResources {
    directory: TempDir,
}

impl RunnerRuntimeResources {
    pub(super) fn prepare(
        pipeline_runtime_directory: &Path,
        executable_image: CapturedRunnerExecutable,
    ) -> Result<Self, RunnerRuntimeResourcesError> {
        let mut directory = create_runtime_directory(pipeline_runtime_directory)?;
        executable_image
            .materialize(directory.path())
            .map_err(RunnerRuntimeResourcesError::Executable)?;
        // Only explicit owner cleanup proves that process resources can be removed.
        directory.disable_cleanup(true);
        Ok(Self { directory })
    }

    pub(super) fn directory(&self) -> &Path {
        self.directory.path()
    }

    pub(super) fn executable(&self) -> PathBuf {
        self.directory.path().join(EXECUTABLE_FILE_NAME)
    }

    pub(super) fn cleanup(
        self,
        disposition: RunnerRuntimeCleanup,
    ) -> Result<(), RunnerRuntimeResourcesError> {
        match disposition {
            RunnerRuntimeCleanup::Remove => {
                let path = self.directory.path().to_path_buf();
                self.directory
                    .close()
                    .map_err(|source| RunnerRuntimeResourcesError::Directory { path, source })
            }
            RunnerRuntimeCleanup::RecoverLater => {
                drop(self.directory.keep());
                Ok(())
            }
            RunnerRuntimeCleanup::MarkOwnerIncomplete => {
                let path = self.directory.path();
                let suffix = runner_runtime_suffix(path)?;
                let preserved = path
                    .parent()
                    .ok_or_else(|| RunnerRuntimeResourcesError::Invalid(path.to_path_buf()))?
                    .join(format!("{INCOMPLETE_RUNTIME_PREFIX}{suffix}"));
                let rename = fs::rename(path, &preserved);
                drop(self.directory.keep());
                rename.map_err(|source| RunnerRuntimeResourcesError::Directory {
                    path: preserved,
                    source,
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum RunnerRuntimeCleanup {
    Remove,
    RecoverLater,
    MarkOwnerIncomplete,
}

pub(super) fn remove_stale_runtime_directory(
    path: PathBuf,
) -> Result<(), RunnerRuntimeResourcesError> {
    fs::remove_dir_all(&path)
        .map_err(|source| RunnerRuntimeResourcesError::Directory { path, source })
}

pub(super) fn stale_runner_directories(
    parent: &Path,
) -> Result<Vec<PathBuf>, RunnerRuntimeResourcesError> {
    let entries = fs::read_dir(parent)
        .map_err(|source| RunnerRuntimeResourcesError::Directory {
            path: parent.to_path_buf(),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| RunnerRuntimeResourcesError::Directory {
            path: parent.to_path_buf(),
            source,
        })?;
    for entry in &entries {
        if is_incomplete_runtime_name(&entry.file_name()) {
            return Err(RunnerRuntimeResourcesError::IncompletePreviousCleanup(
                entry.path(),
            ));
        }
        if !is_runner_runtime_name(&entry.file_name()) {
            continue;
        }
        let path = entry.path();
        let file_type =
            entry
                .file_type()
                .map_err(|source| RunnerRuntimeResourcesError::Directory {
                    path: path.clone(),
                    source,
                })?;
        if !file_type.is_dir() {
            return Err(RunnerRuntimeResourcesError::Invalid(path));
        }
    }
    Ok(entries
        .into_iter()
        .filter(|entry| is_runner_runtime_name(&entry.file_name()))
        .map(|entry| entry.path())
        .collect())
}

fn is_runner_runtime_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with(RUNNER_RUNTIME_PREFIX))
}

fn is_incomplete_runtime_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with(INCOMPLETE_RUNTIME_PREFIX))
}

pub(super) fn runner_runtime_suffix(
    runtime_directory: &Path,
) -> Result<&str, RunnerRuntimeResourcesError> {
    runtime_directory
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .and_then(|name| name.strip_prefix(RUNNER_RUNTIME_PREFIX))
        .filter(|suffix| !suffix.is_empty())
        .ok_or_else(|| RunnerRuntimeResourcesError::Invalid(runtime_directory.to_path_buf()))
}

#[derive(Debug)]
pub(super) enum RunnerRuntimeResourcesError {
    Executable(io::Error),
    Directory { path: PathBuf, source: io::Error },
    Invalid(PathBuf),
    StalePreviousRuntime(PathBuf),
    IncompletePreviousCleanup(PathBuf),
}

impl RunnerRuntimeResourcesError {
    pub(super) const fn code(&self) -> &'static str {
        match self {
            Self::Executable(_) => "runner.executable_image_unavailable",
            Self::IncompletePreviousCleanup(_) => "runner.previous_cleanup_incomplete",
            Self::Directory { .. } | Self::Invalid(_) | Self::StalePreviousRuntime(_) => {
                "runner.runtime_directory_invalid"
            }
        }
    }
}

impl fmt::Display for RunnerRuntimeResourcesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Executable(_) => {
                formatter.write_str("Runner executable image could not be materialized")
            }
            Self::Directory { path, .. } => write!(
                formatter,
                "Runner runtime directory operation failed: {}",
                path.display()
            ),
            Self::Invalid(path) => write!(
                formatter,
                "Runner runtime path is not a directory: {}",
                path.display()
            ),
            Self::StalePreviousRuntime(path) => write!(
                formatter,
                "Runner runtime directory was not recovered before replacement: {}",
                path.display()
            ),
            Self::IncompletePreviousCleanup(path) => write!(
                formatter,
                "Runner refused to discard resources from an incomplete previous cleanup: {}",
                path.display()
            ),
        }
    }
}

impl Error for RunnerRuntimeResourcesError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Executable(source) | Self::Directory { source, .. } => Some(source),
            Self::Invalid(_)
            | Self::StalePreviousRuntime(_)
            | Self::IncompletePreviousCleanup(_) => None,
        }
    }
}

fn create_runtime_directory(parent: &Path) -> Result<TempDir, RunnerRuntimeResourcesError> {
    if let Some(path) = stale_runner_directories(parent)?.into_iter().next() {
        return Err(RunnerRuntimeResourcesError::StalePreviousRuntime(path));
    }
    let directory = tempfile::Builder::new()
        .prefix(RUNNER_RUNTIME_PREFIX)
        .tempdir_in(parent)
        .map_err(|source| RunnerRuntimeResourcesError::Directory {
            path: parent.to_path_buf(),
            source,
        })?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).map_err(|source| {
        RunnerRuntimeResourcesError::Directory {
            path: directory.path().to_path_buf(),
            source,
        }
    })?;
    Ok(directory)
}

#[cfg(test)]
mod tests;
