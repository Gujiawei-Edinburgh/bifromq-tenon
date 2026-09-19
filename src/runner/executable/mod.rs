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

//! Stable executable image owned by one Runner process.
//!
//! The deployment path may be atomically replaced while a Runner is alive.
//! Runner startup therefore opens its own executable once, then materializes
//! those captured bytes inside the recoverable private runtime directory.
//! Every Pipeline launched by that Runner uses the same private image.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::Path;

#[cfg(target_os = "linux")]
#[path = "platform/linux.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "platform/macos.rs"]
mod platform;

pub(crate) const EXECUTABLE_FILE_NAME: &str = "tenon";

/// An open file description that pins the executable bytes for this Runner.
#[must_use = "the captured image must be materialized before Pipeline launch"]
pub(crate) struct CapturedRunnerExecutable {
    source: File,
}

impl CapturedRunnerExecutable {
    /// Opens the executable image used by the current Runner process.
    ///
    /// # Errors
    ///
    /// Returns an error when the operating system cannot expose or open the
    /// current executable as a regular file.
    pub(crate) fn capture_current() -> io::Result<Self> {
        Self::from_file(platform::open_current_executable()?)
    }

    /// Adopts an already-open regular file as the stable executable capability.
    ///
    /// # Errors
    ///
    /// Returns an error when the file metadata is unavailable or the opened
    /// object is not a regular file.
    pub(crate) fn from_file(source: File) -> io::Result<Self> {
        if !source.metadata()?.is_file() {
            return Err(io::Error::other(
                "Runner executable image source is not a regular file",
            ));
        }
        Ok(Self { source })
    }

    /// Copies the pinned bytes into the Runner-owned runtime directory.
    ///
    /// The returned path remains valid until the runtime directory is removed.
    /// If a Pipeline owner cannot be recovered, the runtime directory and this
    /// image are deliberately preserved together.
    ///
    /// # Errors
    ///
    /// Returns an error when the private file cannot be created, copied,
    /// synchronized, or permissioned.
    pub(crate) fn materialize(mut self, runtime_directory: &Path) -> io::Result<()> {
        let path = runtime_directory.join(EXECUTABLE_FILE_NAME);
        let mut destination = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o500)
            .open(&path)?;
        io::copy(&mut self.source, &mut destination)?;
        destination.flush()?;
        destination.sync_all()?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o500))?;
        Ok(())
    }
}

impl fmt::Debug for CapturedRunnerExecutable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapturedRunnerExecutable")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_file_description_survives_source_replacement() -> io::Result<()> {
        let source_directory = tempfile::tempdir()?;
        let source = source_directory.path().join("tenon-source");
        let original = b"#!/bin/sh\nprintf 'old-image\\n'\n";
        fs::write(&source, original)?;
        fs::set_permissions(&source, fs::Permissions::from_mode(0o500))?;
        let captured = CapturedRunnerExecutable::from_file(File::open(&source)?)?;

        let replacement = source_directory.path().join("replacement");
        fs::write(&replacement, b"#!/bin/sh\nprintf 'new-image\\n'\n")?;
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o500))?;
        fs::rename(replacement, source)?;

        let runtime_directory = tempfile::tempdir()?;
        captured.materialize(runtime_directory.path())?;
        assert_eq!(
            fs::read(runtime_directory.path().join(EXECUTABLE_FILE_NAME))?,
            original
        );
        Ok(())
    }

    #[test]
    fn materialized_image_uses_private_permissions() -> io::Result<()> {
        let source_directory = tempfile::tempdir()?;
        let source = source_directory.path().join("tenon-source");
        fs::write(&source, b"image")?;
        let runtime_directory = tempfile::tempdir()?;

        CapturedRunnerExecutable::from_file(File::open(&source)?)?
            .materialize(runtime_directory.path())?;

        assert_eq!(
            runtime_directory
                .path()
                .join(EXECUTABLE_FILE_NAME)
                .metadata()?
                .permissions()
                .mode()
                & 0o777,
            0o500
        );
        Ok(())
    }
}
