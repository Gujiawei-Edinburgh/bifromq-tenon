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

//! Prepares the complete fixed filesystem layout owned by one Runner.
//!
//! Fixed directories are created and validated exactly once, before recovery
//! reads private state or any HTTP, control, or Pipeline work can start. Runtime
//! mutations may create transaction-specific descendants, but they never repair
//! or recreate these roots.

use crate::runner::private_filesystem::{
    has_owner_only_directory_permission, set_owner_only_directory_permission,
};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

pub(crate) const TENON_DOCUMENT_STORE_DIRECTORY_NAME: &str = "tenon-documents";
pub(crate) const PLUGIN_STORE_DIRECTORY_NAME: &str = "plugins";
pub(crate) const PROGRAM_PLUGIN_DIRECTORY_NAME: &str = "programs";
pub(crate) const PIPELINE_RUNTIME_DIRECTORY_NAME: &str = "pipelines";

const PRIVATE_DIRECTORY_PATHS: &[&[&str]] = &[
    &[TENON_DOCUMENT_STORE_DIRECTORY_NAME],
    &[PLUGIN_STORE_DIRECTORY_NAME],
    &[PLUGIN_STORE_DIRECTORY_NAME, PROGRAM_PLUGIN_DIRECTORY_NAME],
    &[PIPELINE_RUNTIME_DIRECTORY_NAME],
];

/// The root whose fixed Runner-private descendants were prepared together.
///
/// This value freezes the startup-time preparation fact for the exact path;
/// callers must derive every fixed child from this one root.
#[derive(Debug)]
pub(crate) struct RunnerStateLayout(PathBuf);

impl RunnerStateLayout {
    /// Returns the exact prepared state root.
    #[must_use]
    pub(crate) fn state_directory(&self) -> &Path {
        &self.0
    }

    /// Returns the prepared root for committed Tenon Documents.
    #[must_use]
    pub(crate) fn tenon_document_store_directory(&self) -> PathBuf {
        self.0.join(TENON_DOCUMENT_STORE_DIRECTORY_NAME)
    }

    /// Returns the prepared root for installed Plugin Programs.
    #[must_use]
    pub(crate) fn plugin_store_directory(&self) -> PathBuf {
        self.0.join(PLUGIN_STORE_DIRECTORY_NAME)
    }

    /// Returns the prepared root for current unified Plugin Programs.
    #[must_use]
    pub(crate) fn plugin_program_store_directory(&self) -> PathBuf {
        self.plugin_store_directory()
            .join(PROGRAM_PLUGIN_DIRECTORY_NAME)
    }

    /// Returns the prepared root for per-process Pipeline runtime trees.
    #[must_use]
    pub(crate) fn pipeline_runtime_directory(&self) -> PathBuf {
        self.0.join(PIPELINE_RUNTIME_DIRECTORY_NAME)
    }
}

/// Creates and validates every fixed Runner-private directory.
///
/// Missing directories are created with owner-only permission. Existing
/// directories must already have the exact expected type and permission;
/// startup never silently repairs environmental drift.
///
/// # Errors
///
/// Returns [`RunnerStateDirectoryError`] when the configured state path or any
/// fixed private directory cannot be created, inspected, or synced.
pub(crate) fn prepare_runner_state_directory(
    state_directory: &Path,
) -> Result<RunnerStateLayout, RunnerStateDirectoryError> {
    create_state_directory(state_directory)?;
    for components in PRIVATE_DIRECTORY_PATHS {
        let path = components
            .iter()
            .fold(state_directory.to_path_buf(), |path, component| {
                path.join(component)
            });
        prepare_private_directory(&path)?;
    }
    Ok(RunnerStateLayout(state_directory.to_path_buf()))
}

#[derive(Debug)]
pub(crate) enum RunnerStateDirectoryError {
    OperationFailed { path: PathBuf, source: io::Error },
    PrivateDirectoryInvalid { path: PathBuf },
}

impl RunnerStateDirectoryError {
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::OperationFailed { .. } => "runner.state_directory_operation_failed",
            Self::PrivateDirectoryInvalid { .. } => "runner.private_directory_invalid",
        }
    }
}

impl fmt::Display for RunnerStateDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperationFailed { path, .. } => {
                write!(
                    formatter,
                    "Runner state directory operation failed: {}",
                    path.display()
                )
            }
            Self::PrivateDirectoryInvalid { path } => write!(
                formatter,
                "Runner private directory has an invalid type or permission: {}",
                path.display()
            ),
        }
    }
}

impl Error for RunnerStateDirectoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::OperationFailed { source, .. } => Some(source),
            Self::PrivateDirectoryInvalid { .. } => None,
        }
    }
}

fn create_state_directory(path: &Path) -> Result<(), RunnerStateDirectoryError> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|source| RunnerStateDirectoryError::OperationFailed {
            path: path.to_path_buf(),
            source,
        })?;
    let metadata =
        fs::metadata(path).map_err(|source| RunnerStateDirectoryError::OperationFailed {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_dir() {
        return Err(RunnerStateDirectoryError::PrivateDirectoryInvalid {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn prepare_private_directory(path: &Path) -> Result<(), RunnerStateDirectoryError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && has_owner_only_directory_permission(&metadata) => {
            Ok(())
        }
        Ok(_) => Err(RunnerStateDirectoryError::PrivateDirectoryInvalid {
            path: path.to_path_buf(),
        }),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt as _;
                builder.mode(0o700);
            }
            builder
                .create(path)
                .map_err(|source| RunnerStateDirectoryError::OperationFailed {
                    path: path.to_path_buf(),
                    source,
                })?;
            set_owner_only_directory_permission(path).map_err(|source| {
                RunnerStateDirectoryError::OperationFailed {
                    path: path.to_path_buf(),
                    source,
                }
            })?;
            sync_directory(path)?;
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
            Ok(())
        }
        Err(source) => Err(RunnerStateDirectoryError::OperationFailed {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn sync_directory(path: &Path) -> Result<(), RunnerStateDirectoryError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| RunnerStateDirectoryError::OperationFailed {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::{
        PIPELINE_RUNTIME_DIRECTORY_NAME, PLUGIN_STORE_DIRECTORY_NAME,
        PROGRAM_PLUGIN_DIRECTORY_NAME, RunnerStateDirectoryError,
        TENON_DOCUMENT_STORE_DIRECTORY_NAME, prepare_runner_state_directory,
    };
    use std::fs;
    use std::io;

    #[test]
    fn startup_creates_the_complete_fixed_private_layout() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let state_directory = parent.path().join("missing/state");

        prepare_runner_state_directory(&state_directory).map_err(io::Error::other)?;

        for path in [
            state_directory.join(TENON_DOCUMENT_STORE_DIRECTORY_NAME),
            state_directory.join(PLUGIN_STORE_DIRECTORY_NAME),
            state_directory
                .join(PLUGIN_STORE_DIRECTORY_NAME)
                .join(PROGRAM_PLUGIN_DIRECTORY_NAME),
            state_directory.join(PIPELINE_RUNTIME_DIRECTORY_NAME),
        ] {
            assert_private_directory(&path)?;
        }
        assert!(!state_directory.join("plugins/sources").exists());
        assert!(!state_directory.join("plugins/sinks").exists());
        Ok(())
    }

    #[test]
    fn startup_does_not_inspect_or_modify_retired_role_paths() -> io::Result<()> {
        let state = tempfile::tempdir()?;
        let plugins = state.path().join(PLUGIN_STORE_DIRECTORY_NAME);
        super::prepare_private_directory(&plugins).map_err(io::Error::other)?;
        for name in ["sources", "sinks"] {
            fs::write(plugins.join(name), b"retired role data")?;
        }

        prepare_runner_state_directory(state.path()).map_err(io::Error::other)?;

        for name in ["sources", "sinks"] {
            assert_eq!(fs::read(plugins.join(name))?, b"retired role data");
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn startup_rejects_permission_drift_without_repairing_it() -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;

        let state_directory = tempfile::tempdir()?;
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
        let store = state_directory
            .path()
            .join(TENON_DOCUMENT_STORE_DIRECTORY_NAME);
        fs::set_permissions(&store, fs::Permissions::from_mode(0o755))?;

        let error = prepare_runner_state_directory(state_directory.path())
            .err()
            .ok_or_else(|| io::Error::other("private permission drift was repaired"))?;

        assert!(matches!(
            error,
            RunnerStateDirectoryError::PrivateDirectoryInvalid { .. }
        ));
        assert_eq!(fs::metadata(store)?.permissions().mode() & 0o7777, 0o755);
        Ok(())
    }

    #[test]
    fn startup_rejects_a_fixed_path_with_the_wrong_type() -> io::Result<()> {
        let state_directory = tempfile::tempdir()?;
        fs::write(
            state_directory
                .path()
                .join(TENON_DOCUMENT_STORE_DIRECTORY_NAME),
            b"not a directory",
        )?;

        let error = prepare_runner_state_directory(state_directory.path())
            .err()
            .ok_or_else(|| io::Error::other("fixed private file was accepted"))?;

        assert!(matches!(
            error,
            RunnerStateDirectoryError::PrivateDirectoryInvalid { .. }
        ));
        Ok(())
    }

    #[cfg(unix)]
    fn assert_private_directory(path: &std::path::Path) -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;

        assert!(path.is_dir());
        assert_eq!(fs::metadata(path)?.permissions().mode() & 0o7777, 0o700);
        Ok(())
    }

    #[cfg(not(unix))]
    fn assert_private_directory(path: &std::path::Path) -> io::Result<()> {
        assert!(path.is_dir());
        Ok(())
    }
}
