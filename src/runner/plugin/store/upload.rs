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

//! Owns the complete compressed upload before the Store begins decoding it.
//!
//! Input failure rejects only the upload. Local spool I/O is tagged at its
//! source so gzip/tar decoding cannot disguise disk failure as an invalid body.

use super::{PluginPackageError, PluginStoreError};
use std::fs::File;
use std::io::{self, Read, Seek as _, Write};
use std::path::Path;

/// Receives the complete input into an unnamed file under the Store root.
///
/// # Errors
///
/// Input read failure is an invalid upload; local file I/O requires Runner exit.
pub(super) fn receive(
    mut input: impl Read,
    directory: &Path,
) -> Result<LocalArchiveReader<File>, PluginStoreError> {
    let filesystem_error = |source| PluginStoreError::FilesystemOperationFailed {
        path: directory.to_path_buf(),
        source,
    };
    let mut spool = tempfile::tempfile_in(directory).map_err(filesystem_error)?;
    receive_into(&mut input, &mut spool, directory)?;
    spool.rewind().map_err(filesystem_error)?;
    Ok(LocalArchiveReader(spool))
}

/// Preserves local read provenance through the shared streaming decoder.
#[derive(Debug)]
pub(super) struct LocalArchiveReader<R>(R);

impl<R: Read> Read for LocalArchiveReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.0.read(output).map_err(|source| {
            io::Error::new(
                source.kind(),
                PluginPackageError::FilesystemOperationFailed { source },
            )
        })
    }
}

fn receive_into(
    input: &mut impl Read,
    spool: &mut impl Write,
    directory: &Path,
) -> Result<(), PluginStoreError> {
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = input
            .read(&mut buffer)
            .map_err(|source| PluginStoreError::PackageInvalid {
                source: PluginPackageError::ArchiveInvalid { source },
            })?;
        if count == 0 {
            return Ok(());
        }
        spool.write_all(&buffer[..count]).map_err(|source| {
            PluginStoreError::FilesystemOperationFailed {
                path: directory.to_path_buf(),
                source,
            }
        })?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::plugin::package::{
        stage_plugin_program_package, tests::valid_source_program_package,
    };
    use std::io::Cursor;

    #[test]
    fn input_failure_after_a_complete_archive_still_rejects_the_upload() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let input = Cursor::new(valid_source_program_package()?)
            .chain(FailingIo(io::ErrorKind::ConnectionReset));
        let error = receive(input, directory.path())
            .err()
            .ok_or_else(|| io::Error::other("Interrupted body was accepted"))?;
        assert!(matches!(error, PluginStoreError::PackageInvalid {
            source: PluginPackageError::ArchiveInvalid { source }
        } if source.kind() == io::ErrorKind::ConnectionReset));
        assert!(directory.path().read_dir()?.next().is_none());
        Ok(())
    }

    #[test]
    fn spool_write_failure_is_not_an_invalid_upload() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let error = receive_into(
            &mut Cursor::new(b"body"),
            &mut FailingIo(io::ErrorKind::StorageFull),
            directory.path(),
        )
        .err()
        .ok_or_else(|| io::Error::other("Disk write failure was ignored"))?;
        assert!(
            matches!(error, PluginStoreError::FilesystemOperationFailed { path, source }
            if path == directory.path() && source.kind() == io::ErrorKind::StorageFull)
        );
        Ok(())
    }

    #[test]
    fn local_read_failures_survive_gzip_tar_and_trailer_decoding() -> io::Result<()> {
        let package = valid_source_program_package()?;
        for (offset, kind) in [
            (0, io::ErrorKind::Other),
            (10, io::ErrorKind::Other),
            (package.len() / 2, io::ErrorKind::Other),
            (package.len() - 4, io::ErrorKind::Other),
            (package.len(), io::ErrorKind::Other),
            (0, io::ErrorKind::FileTooLarge),
        ] {
            let directory = tempfile::tempdir()?;
            let input = Cursor::new(&package[..offset]).chain(FailingIo(kind));
            let error = stage_plugin_program_package(LocalArchiveReader(input), directory.path())
                .err()
                .ok_or_else(|| io::Error::other("Local read failure was ignored"))?;
            assert!(
                matches!(error, PluginPackageError::FilesystemOperationFailed { source }
                if source.kind() == kind),
                "offset {offset}, kind {kind}"
            );
            assert!(directory.path().read_dir()?.next().is_none());
        }
        Ok(())
    }

    struct FailingIo(io::ErrorKind);

    impl Read for FailingIo {
        fn read(&mut self, _output: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(self.0, "Injected read failure"))
        }
    }

    impl Write for FailingIo {
        fn write(&mut self, _input: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(self.0, "Injected write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
