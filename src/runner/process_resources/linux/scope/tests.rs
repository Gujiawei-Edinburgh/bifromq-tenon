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

use super::*;

#[test]
fn mount_discovery_handles_namespace_roots_bind_mounts_and_hidden_ancestors() -> io::Result<()> {
    let mounts = "21 1 0:1 /delegated /sys/fs/cgroup rw - cgroup2 cgroup rw\n";
    let resolved = resolve_mount(Path::new("/delegated"), mounts)?;
    assert_eq!(resolved.root, Path::new("/delegated"));
    assert_eq!(resolved.mount, Path::new("/sys/fs/cgroup"));
    assert_eq!(resolved.current, Path::new("/sys/fs/cgroup/"));
    let private = "21 1 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n";
    assert_eq!(
        resolve_mount(Path::new("/"), private)?.current,
        Path::new("/sys/fs/cgroup/")
    );
    assert_eq!(
        resolve_mount(Path::new("/tenon.runner"), private)?.current,
        Path::new("/sys/fs/cgroup/tenon.runner")
    );
    let escaped = "21 1 0:1 / /run/cgroup\\040mount rw - cgroup2 cgroup rw\n";
    assert_eq!(
        resolve_mount(Path::new("/service"), escaped)?.current,
        Path::new("/run/cgroup mount/service")
    );
    assert!(resolve_mount(Path::new("/outside/tenon.runner"), mounts).is_err());
    assert!(resolve_mount(Path::new("/delegated/../tenon.runner"), mounts).is_err());
    Ok(())
}

#[test]
fn membership_never_migrates_a_neighbor_or_invisible_process() -> io::Result<()> {
    assert_eq!(movable_members("42\n", 42, false)?, [42]);
    assert_eq!(movable_members("1\n42\n", 42, true)?, [1, 42]);
    assert_eq!(movable_members("1\n", 1, false)?, [1]);
    assert!(movable_members("1\n42\n", 42, false).is_err());
    assert!(movable_members("0\n42\n", 42, true).is_err());
    assert!(movable_members("43\n42\n", 42, true).is_err());
    Ok(())
}
