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

//! Removes one Pipeline directory after its process resources are released.

use std::error::Error;
use std::path::PathBuf;
use std::{fmt, fs, io};
use tokio::task::{self, JoinError};

/// Removes an owned Pipeline directory after its processes and resources are released.
/// A missing directory is already clean.
///
/// # Errors
///
/// Returns the blocking task or filesystem failure if removal cannot complete.
pub(in crate::runner) async fn cleanup_pipeline_directory(
    path: PathBuf,
) -> Result<(), PipelineDirectoryCleanupError> {
    task::spawn_blocking(move || match fs::remove_dir_all(&path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(PipelineDirectoryCleanupError::Filesystem { path, source }),
    })
    .await
    .map_err(PipelineDirectoryCleanupError::Task)?
}

/// Failure to remove one Pipeline directory after process cleanup.
#[derive(Debug)]
pub(in crate::runner) enum PipelineDirectoryCleanupError {
    ResourceGroup(io::Error),
    Task(JoinError),
    Filesystem { path: PathBuf, source: io::Error },
}

impl fmt::Display for PipelineDirectoryCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceGroup(_) => formatter.write_str("Pipeline resource group cleanup failed"),
            Self::Task(_) => formatter.write_str("Pipeline directory cleanup task failed"),
            Self::Filesystem { path, .. } => write!(
                formatter,
                "Pipeline directory cleanup failed: {}",
                path.display()
            ),
        }
    }
}

impl Error for PipelineDirectoryCleanupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ResourceGroup(source) => Some(source),
            Self::Task(source) => Some(source),
            Self::Filesystem { source, .. } => Some(source),
        }
    }
}
