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

//! Claims one delegated scope before changing membership or recovering workloads.

use super::{create_subtree, kill_and_remove, unescape_mount_path};
use rustix::fs::{FlockOperation, flock, getxattr};
use std::fs::{self, File};
use std::io;
use std::os::fd::AsRawFd as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const RUNNER_GROUP: &str = "tenon.runner";
const WORKLOAD_GROUP: &str = "tenon.pipelines";

/// A Runner keeps its claim until its last Pipeline owner has finished cleanup.
#[derive(Debug)]
pub(in crate::runner) enum RunnerResources {
    Available(Arc<OwnedScope>),
    Unavailable {
        failure: io::Error,
        _claim: Option<File>,
    },
}

#[derive(Debug)]
pub(in crate::runner) struct OwnedScope {
    pub(super) path: PathBuf,
    // File::open sets CLOEXEC. Pipeline/Plugin executables cannot retain the claim.
    _claim: File,
}

struct ScopeLocation {
    path: PathBuf,
    container_root: bool,
    mount_readonly: bool,
    super_readonly: bool,
}

impl RunnerResources {
    /// Runs before extension initialization or spawning any child processes.
    /// Missing delegation leaves unlimited Documents usable. A competing owner
    /// or a failure after membership changes aborts startup before state recovery.
    pub(in crate::runner) fn initialize() -> io::Result<Self> {
        let location = match discover_scope() {
            Ok(location) => location,
            Err(error) => return Ok(Self::unavailable(error, None)),
        };
        let access = writable_scope(&location.path);
        let prepare_mount = access
            .as_ref()
            .is_err_and(|error| error.kind() == io::ErrorKind::ReadOnlyFilesystem)
            && location.container_root
            && location.mount_readonly
            && !location.super_readonly
            && has_mount_capability()?;
        if let Err(error) = &access
            && !prepare_mount
        {
            // No writable delegation and no mount grant: do not claim an
            // ordinary read-only container shared by unlimited Runners.
            if location.path.join(WORKLOAD_GROUP).exists() {
                return Err(io::Error::new(error.kind(), error.to_string()));
            }
            return Ok(Self::unavailable(
                io::Error::new(error.kind(), error.to_string()),
                None,
            ));
        }
        let claim = File::open(&location.path)?;
        flock(&claim, FlockOperation::NonBlockingLockExclusive).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "Cannot exclusively claim cgroup scope {} (another Runner may own it): {error}",
                    location.path.display()
                ),
            )
        })?;
        let members = fs::read_to_string(location.path.join("cgroup.procs"))?;
        let members = movable_members(
            &members,
            std::process::id(),
            location.container_root
                && rustix::process::getppid().is_some_and(|pid| pid.as_raw_pid() == 1),
        )?;
        for entry in fs::read_dir(&location.path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && entry.file_name() != RUNNER_GROUP
                && entry.file_name() != WORKLOAD_GROUP
                && !(entry.file_name() == ".control" && !location.container_root)
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "Cgroup scope contains another manager's subtree: {}",
                        entry.path().display()
                    ),
                ));
            }
        }
        if prepare_mount
            && let Err(error) =
                make_container_mount_writable(&claim).and_then(|()| writable_scope(&location.path))
        {
            if location.path.join(WORKLOAD_GROUP).exists() {
                return Err(error);
            }
            return Ok(Self::unavailable(error, Some(claim)));
        }
        let runner = location.path.join(RUNNER_GROUP);
        if let Err(error) = create_subtree(&runner) {
            if location.path.join(WORKLOAD_GROUP).exists() {
                return Err(error);
            }
            return Ok(Self::unavailable(
                io::Error::new(
                    error.kind(),
                    format!(
                        "Cannot create the Runner cgroup {}: {error}",
                        runner.display()
                    ),
                ),
                Some(claim),
            ));
        }
        for pid in members {
            fs::write(runner.join("cgroup.procs"), pid.to_string()).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "Cannot move process {pid} into {}: {error}",
                        runner.display()
                    ),
                )
            })?;
        }
        Ok(Self::Available(Arc::new(OwnedScope {
            path: location.path,
            _claim: claim,
        })))
    }

    fn unavailable(failure: io::Error, claim: Option<File>) -> Self {
        eprintln!("runner.resource_limits_unavailable: {failure}");
        Self::Unavailable {
            failure,
            _claim: claim,
        }
    }

    pub(super) fn scope(&self) -> io::Result<Arc<OwnedScope>> {
        match self {
            Self::Available(scope) => Ok(Arc::clone(scope)),
            Self::Unavailable { failure, .. } => {
                Err(io::Error::new(failure.kind(), failure.to_string()))
            }
        }
    }

    pub(in crate::runner) async fn recover_stale_groups(&self) -> io::Result<()> {
        let Self::Available(scope) = self else {
            return Ok(());
        };
        let entries = match fs::read_dir(scope.path.join(WORKLOAD_GROUP)) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                kill_and_remove(&entry.path()).await?;
            }
        }
        Ok(())
    }
}

fn writable_scope(path: &Path) -> io::Result<()> {
    for name in ["cgroup.procs", "cgroup.subtree_control"] {
        File::options().write(true).open(path.join(name)).map_err(|error| {
            io::Error::new(error.kind(), format!(
                "Resource limits require a writable delegated cgroup v2 scope; cannot open {}/{name}: {error}", path.display()))
        })?;
    }
    Ok(())
}

fn has_mount_capability() -> io::Result<bool> {
    let status = fs::read_to_string("/proc/self/status")?;
    let capabilities = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing effective process capabilities",
            )
        })?;
    let capabilities = u64::from_str_radix(capabilities.trim(), 16)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(capabilities & (1 << 21) != 0) // Linux CAP_SYS_ADMIN, explicitly granted by the container starter.
}

fn make_container_mount_writable(claim: &File) -> io::Result<()> {
    let attributes = libc::mount_attr {
        attr_set: 0,
        attr_clr: libc::MOUNT_ATTR_RDONLY,
        propagation: 0,
        userns_fd: 0,
    };
    // SAFETY: claim is the live directory descriptor of the verified private
    // container cgroup mount root. The empty path and initialized attributes
    // remain valid for this synchronous syscall. Only this mount's read-only
    // attribute is cleared: no recursion, new mount, superblock changes, or
    // removal of nosuid/nodev/noexec/atime protections is requested.
    let result = unsafe {
        libc::syscall(
            libc::SYS_mount_setattr,
            claim.as_raw_fd(),
            c"".as_ptr(),
            libc::AT_EMPTY_PATH,
            &attributes,
            std::mem::size_of::<libc::mount_attr>(),
        )
    };
    if result == 0 {
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        Err(io::Error::new(
            error.kind(),
            format!(
                "Cannot prepare the private container cgroup mount; the container must grant CAP_SYS_ADMIN and permit mount_setattr: {error}"
            ),
        ))
    }
}

fn movable_members(members: &str, runner: u32, container_root: bool) -> io::Result<Vec<u32>> {
    members.lines().map(|line| {
        let pid: u32 = line.parse().map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        // PID 1 in a verified container view and our own PID cannot be reused
        // while this process is alive. Never migrate unrelated processes.
        if pid == runner || (container_root && pid == 1) {
            Ok(pid)
        } else {
            Err(io::Error::new(io::ErrorKind::PermissionDenied,
                format!("Cgroup scope is shared with process {pid}; give each Runner its own delegated scope")))
        }
    }).collect()
}

fn discover_scope() -> io::Result<ScopeLocation> {
    let membership = fs::read_to_string("/proc/self/cgroup")?;
    let current = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "Resource limits require cgroup v2",
            )
        })?;
    let mounts = fs::read_to_string("/proc/self/mountinfo")?;
    let membership_is_container_root = current == "/" || current == "/tenon.runner";
    let resolved = resolve_mount(Path::new(current), &mounts)?;
    let mount = resolved.mount;
    let current = resolved.current;
    // Only inspect our current group or our own internal supervisor subgroup.
    // An ancestor's delegation (e.g. user@UID.service) is not delegation to us.
    let candidate = if current.file_name().is_some_and(|name| name == RUNNER_GROUP) {
        current
            .parent()
            .filter(|parent| parent.starts_with(&mount))
            .unwrap_or(&current)
    } else {
        &current
    };
    let mut delegation = [0_u8; 8];
    let delegated = matches!(getxattr(candidate, "user.delegate", &mut delegation), Ok(1))
        && delegation[0] == b'1';
    let container_root = membership_is_container_root
        && resolved.root == Path::new("/")
        && candidate == mount
        && candidate.join("cgroup.kill").exists()
        && (Path::new("/.dockerenv").exists() || Path::new("/run/.containerenv").exists())
        && fs::read_to_string("/proc/1/cgroup")?
            .lines()
            .any(|line| line == "0::/" || line == "0::/tenon.runner");
    if !delegated && !container_root {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Resource limits require a dedicated delegated cgroup v2 scope (systemd Delegate=cpu memory), or a container with a private writable cgroup namespace",
        ));
    }
    // The real hierarchy root has no cgroup.kill. Never manage the host root.
    if !candidate.join("cgroup.kill").exists() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "The host cgroup root cannot be used as a Runner scope",
        ));
    }
    Ok(ScopeLocation {
        path: candidate.to_path_buf(),
        container_root,
        mount_readonly: resolved.mount_readonly,
        super_readonly: resolved.super_readonly,
    })
}

struct ResolvedMount {
    root: PathBuf,
    mount: PathBuf,
    current: PathBuf,
    mount_readonly: bool,
    super_readonly: bool,
}

fn resolve_mount(current: &Path, mounts: &str) -> io::Result<ResolvedMount> {
    for line in mounts.lines() {
        let Some((fields, filesystem)) = line.split_once(" - ") else {
            continue;
        };
        if !filesystem.starts_with("cgroup2 ") {
            continue;
        }
        let fields: Vec<_> = fields.split_whitespace().collect();
        let (Some(root), Some(mount)) = (fields.get(3), fields.get(4)) else {
            continue;
        };
        let root = unescape_mount_path(root)?;
        let mount = unescape_mount_path(mount)?;
        let Ok(relative) = current.strip_prefix(&root) else {
            continue;
        };
        if relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "The cgroup namespace hides the Runner scope",
            ));
        }
        let current = mount.join(relative);
        let mount_readonly = fields
            .get(5)
            .is_some_and(|options| options.split(',').any(|option| option == "ro"));
        let super_readonly = filesystem
            .split_whitespace()
            .nth(2)
            .is_some_and(|options| options.split(',').any(|option| option == "ro"));
        return Ok(ResolvedMount {
            root,
            mount,
            current,
            mount_readonly,
            super_readonly,
        });
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "No cgroup2 mount exposes the Runner scope",
    ))
}

#[cfg(test)]
mod tests;
