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

//! Shared exact permission checks for Runner-private filesystem objects.

use std::fs;
use std::io;
use std::path::Path;

#[cfg(unix)]
pub(crate) fn has_owner_only_directory_permission(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o7777 == 0o700
}

#[cfg(not(unix))]
pub(crate) fn has_owner_only_directory_permission(_metadata: &fs::Metadata) -> bool {
    true
}

#[cfg(unix)]
pub(crate) fn has_owner_only_file_permission(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o7777 == 0o500
}

#[cfg(not(unix))]
pub(crate) fn has_owner_only_file_permission(_metadata: &fs::Metadata) -> bool {
    true
}

#[cfg(unix)]
pub(crate) fn set_owner_only_directory_permission(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
pub(crate) fn set_owner_only_directory_permission(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
pub(crate) fn set_owner_only_file_permission(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o500))
}

#[cfg(not(unix))]
pub(crate) fn set_owner_only_file_permission(_path: &Path) -> io::Result<()> {
    Ok(())
}
