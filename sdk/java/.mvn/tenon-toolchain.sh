#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

_tenon_property() {
  awk -F= -v requested_key="$2" '$1 == requested_key { sub(/^[^=]*=/, ""); print; exit }' "$1"
}

_tenon_platform_from() {
  case "$1:$2" in
  Linux:x86_64 | Linux:amd64) printf '%s\n' linux-amd64 ;;
  Linux:aarch64 | Linux:arm64) printf '%s\n' linux-arm64 ;;
  Darwin:x86_64 | Darwin:amd64) printf '%s\n' macos-amd64 ;;
  Darwin:aarch64 | Darwin:arm64) printf '%s\n' macos-arm64 ;;
  *) return 1 ;;
  esac
}

_tenon_java_setting() {
  printf '%s\n' "$1" | awk -F' = ' -v requested_key="$2" '$1 ~ "^[[:space:]]*" requested_key "$" { print $2; exit }'
}

_tenon_jdk_matches() {
  _tenon_jdk_home=$1
  _tenon_expected_version=$2
  _tenon_expected_vendor=$3
  [ -x "$_tenon_jdk_home/bin/java" ] || return 1
  [ -x "$_tenon_jdk_home/bin/javac" ] || return 1
  [ -x "$_tenon_jdk_home/bin/jmod" ] || return 1
  [ -f "$_tenon_jdk_home/jmods/java.base.jmod" ] || return 1
  [ -f "$_tenon_jdk_home/NOTICE" ] || return 1
  _tenon_java_settings=$("$_tenon_jdk_home/bin/java" -XshowSettings:properties -version 2>&1) || return 1
  _tenon_actual_vendor=$(_tenon_java_setting "$_tenon_java_settings" java.vendor)
  _tenon_actual_vendor_version=$(_tenon_java_setting "$_tenon_java_settings" java.vendor.version)
  _tenon_actual_version=$(_tenon_java_setting "$_tenon_java_settings" java.runtime.version)
  [ "$_tenon_actual_vendor" = "$_tenon_expected_vendor" ] || return 1
  [ "$_tenon_actual_vendor_version" = "Temurin-$_tenon_expected_version" ] || return 1
  case "$_tenon_actual_version" in
  "$_tenon_expected_version" | "$_tenon_expected_version"-*) ;;
  *) return 1 ;;
  esac
  _tenon_expected_module_version=${_tenon_expected_version%+*}
  _tenon_module_description=$("$_tenon_jdk_home/bin/jmod" describe \
    "$_tenon_jdk_home/jmods/java.base.jmod" 2>/dev/null) || return 1
  _tenon_first_module_line=$(printf '%s\n' "$_tenon_module_description" | sed -n '1p')
  [ "$_tenon_first_module_line" = "java.base@$_tenon_expected_module_version" ]
}

_tenon_cached_jdk_matches() {
  _tenon_jdk_matches "$1" "$2" "$3" || return 1
  [ -f "$1/.tenon-jdk-archive-sha256" ] || return 1
  [ -f "$1/.tenon-jmods-archive-sha256" ] || return 1
  [ "$(cat "$1/.tenon-jdk-archive-sha256")" = "$4" ] || return 1
  [ "$(cat "$1/.tenon-jmods-archive-sha256")" = "$5" ]
}

_tenon_use_jdk() {
  JAVA_HOME=$1
  PATH="$JAVA_HOME/bin${PATH:+:$PATH}"
  export JAVA_HOME PATH
}

_tenon_path_java_home() {
  _tenon_path_java=$(command -v java 2>/dev/null) || return 1
  _tenon_path_settings=$("$_tenon_path_java" -XshowSettings:properties -version 2>&1) || return 1
  _tenon_java_setting "$_tenon_path_settings" java.home
}

_tenon_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{ print $1 }'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{ print $1 }'
  else
    return 1
  fi
}

_tenon_download() {
  if command -v curl >/dev/null 2>&1; then
    curl --fail --location --silent --show-error --retry 3 --output "$2" "$1"
  elif command -v wget >/dev/null 2>&1; then
    wget --quiet --output-document="$2" "$1"
  else
    return 1
  fi
}

_tenon_cleanup_download() {
  if [ -n "${_tenon_download_directory-}" ] && [ -d "$_tenon_download_directory" ]; then
    rm -rf "$_tenon_download_directory"
  fi
}

tenon_bootstrap_java() {
  _tenon_manifest=$1
  if [ ! -f "$_tenon_manifest" ]; then
    printf 'Tenon Java toolchain manifest is missing: %s\n' "$_tenon_manifest" >&2
    return 1
  fi

  _tenon_version=$(_tenon_property "$_tenon_manifest" version)
  _tenon_vendor=$(_tenon_property "$_tenon_manifest" vendor)
  _tenon_platform=$(_tenon_platform_from "$(uname -s)" "$(uname -m)") || {
    printf 'Tenon Java Plugin bundles are unsupported on %s/%s\n' "$(uname -s)" "$(uname -m)" >&2
    return 1
  }
  _tenon_jdk_url=$(_tenon_property "$_tenon_manifest" "$_tenon_platform.jdk.url")
  _tenon_jdk_sha256=$(_tenon_property "$_tenon_manifest" "$_tenon_platform.jdk.sha256")
  _tenon_jmods_url=$(_tenon_property "$_tenon_manifest" "$_tenon_platform.jmods.url")
  _tenon_jmods_sha256=$(_tenon_property "$_tenon_manifest" "$_tenon_platform.jmods.sha256")
  if [ -z "$_tenon_version" ] || [ -z "$_tenon_vendor" ] || [ -z "$_tenon_jdk_url" ] ||
    [ -z "$_tenon_jmods_url" ] ||
    ! printf '%s\n' "$_tenon_jdk_sha256" | grep -Eq '^[0-9a-f]{64}$' ||
    ! printf '%s\n' "$_tenon_jmods_sha256" | grep -Eq '^[0-9a-f]{64}$'; then
    printf 'Tenon Java toolchain manifest is invalid: %s\n' "$_tenon_manifest" >&2
    return 1
  fi

  if [ -n "${JAVA_HOME-}" ] && _tenon_jdk_matches "$JAVA_HOME" "$_tenon_version" "$_tenon_vendor"; then
    _tenon_use_jdk "$JAVA_HOME"
    return 0
  fi
  _tenon_discovered_java_home=$(_tenon_path_java_home 2>/dev/null) || _tenon_discovered_java_home=
  if [ -n "$_tenon_discovered_java_home" ] &&
    _tenon_jdk_matches "$_tenon_discovered_java_home" "$_tenon_version" "$_tenon_vendor"; then
    _tenon_use_jdk "$_tenon_discovered_java_home"
    return 0
  fi

  if [ -n "${MAVEN_USER_HOME-}" ]; then
    _tenon_maven_user_home=$MAVEN_USER_HOME
  elif [ -n "${HOME-}" ]; then
    _tenon_maven_user_home=$HOME/.m2
  else
    printf 'HOME or MAVEN_USER_HOME is required to cache the Tenon Java toolchain\n' >&2
    return 1
  fi
  _tenon_cache_root="$_tenon_maven_user_home/tenon/toolchains/temurin-$_tenon_version"
  _tenon_cached_home="$_tenon_cache_root/$_tenon_platform"
  if _tenon_cached_jdk_matches \
    "$_tenon_cached_home" \
    "$_tenon_version" \
    "$_tenon_vendor" \
    "$_tenon_jdk_sha256" \
    "$_tenon_jmods_sha256"; then
    _tenon_use_jdk "$_tenon_cached_home"
    return 0
  fi
  if [ -e "$_tenon_cached_home" ] || [ -L "$_tenon_cached_home" ]; then
    rm -rf "$_tenon_cached_home"
  fi

  mkdir -p "$_tenon_cache_root"
  _tenon_download_directory=$(mktemp -d "$_tenon_cache_root/.download.XXXXXX") || return 1
  trap '_tenon_cleanup_download' 0
  trap '_tenon_cleanup_download; exit 1' 1 2 15
  _tenon_jdk_archive="$_tenon_download_directory/temurin-jdk.tar.gz"
  _tenon_jmods_archive="$_tenon_download_directory/temurin-jmods.tar.gz"
  _tenon_jdk_extracted="$_tenon_download_directory/jdk"
  _tenon_jmods_extracted="$_tenon_download_directory/jmods"
  mkdir -p "$_tenon_jdk_extracted" "$_tenon_jmods_extracted"
  printf 'Tenon is provisioning Eclipse Temurin %s for %s\n' "$_tenon_version" "$_tenon_platform" >&2
  if ! _tenon_download "$_tenon_jdk_url" "$_tenon_jdk_archive"; then
    printf 'Failed to download the fixed Tenon JDK from %s\n' "$_tenon_jdk_url" >&2
    return 1
  fi
  if ! _tenon_download "$_tenon_jmods_url" "$_tenon_jmods_archive"; then
    printf 'Failed to download the fixed Tenon JMODs from %s\n' "$_tenon_jmods_url" >&2
    return 1
  fi
  _tenon_actual_jdk_sha256=$(_tenon_sha256 "$_tenon_jdk_archive") || {
    printf 'Neither sha256sum nor shasum is available to verify the Tenon Java toolchain\n' >&2
    return 1
  }
  _tenon_actual_jmods_sha256=$(_tenon_sha256 "$_tenon_jmods_archive") || return 1
  if [ "$_tenon_actual_jdk_sha256" != "$_tenon_jdk_sha256" ]; then
    printf 'Tenon JDK checksum mismatch: expected %s, found %s\n' \
      "$_tenon_jdk_sha256" "$_tenon_actual_jdk_sha256" >&2
    return 1
  fi
  if [ "$_tenon_actual_jmods_sha256" != "$_tenon_jmods_sha256" ]; then
    printf 'Tenon JMODs checksum mismatch: expected %s, found %s\n' \
      "$_tenon_jmods_sha256" "$_tenon_actual_jmods_sha256" >&2
    return 1
  fi
  if ! tar -xzf "$_tenon_jdk_archive" -C "$_tenon_jdk_extracted"; then
    printf 'Failed to extract the fixed Tenon JDK\n' >&2
    return 1
  fi
  if ! tar -xzf "$_tenon_jmods_archive" -C "$_tenon_jmods_extracted"; then
    printf 'Failed to extract the fixed Tenon JMODs\n' >&2
    return 1
  fi
  _tenon_java_binary=$(find "$_tenon_jdk_extracted" -type f -path '*/bin/java' -print | head -n 1)
  if [ -z "$_tenon_java_binary" ]; then
    printf 'Downloaded Tenon JDK does not contain bin/java\n' >&2
    return 1
  fi
  _tenon_downloaded_home=${_tenon_java_binary%/bin/java}
  _tenon_java_base_jmod=$(find "$_tenon_jmods_extracted" -type f -name java.base.jmod -print | head -n 1)
  if [ -z "$_tenon_java_base_jmod" ]; then
    printf 'Downloaded Tenon JMODs do not contain java.base.jmod\n' >&2
    return 1
  fi
  _tenon_downloaded_jmods=${_tenon_java_base_jmod%/java.base.jmod}
  if [ -e "$_tenon_downloaded_home/jmods" ]; then
    printf 'Downloaded Tenon JDK unexpectedly already contains jmods\n' >&2
    return 1
  fi
  mv "$_tenon_downloaded_jmods" "$_tenon_downloaded_home/jmods"
  if ! _tenon_jdk_matches "$_tenon_downloaded_home" "$_tenon_version" "$_tenon_vendor"; then
    printf 'Downloaded Tenon Java toolchain does not match %s %s\n' \
      "$_tenon_vendor" "$_tenon_version" >&2
    return 1
  fi

  _tenon_install_home=$(mktemp -d "$_tenon_cache_root/.${_tenon_platform}.install.XXXXXX") || return 1
  rmdir "$_tenon_install_home"
  mv "$_tenon_downloaded_home" "$_tenon_install_home"
  printf '%s\n' "$_tenon_jdk_sha256" >"$_tenon_install_home/.tenon-jdk-archive-sha256"
  printf '%s\n' "$_tenon_jmods_sha256" >"$_tenon_install_home/.tenon-jmods-archive-sha256"
  if ln -sn "${_tenon_install_home##*/}" "$_tenon_cached_home" 2>/dev/null; then
    :
  elif _tenon_cached_jdk_matches \
    "$_tenon_cached_home" \
    "$_tenon_version" \
    "$_tenon_vendor" \
    "$_tenon_jdk_sha256" \
    "$_tenon_jmods_sha256"; then
    rm -rf "$_tenon_install_home"
  else
    printf 'Failed to publish the cached Tenon Java toolchain at %s\n' "$_tenon_cached_home" >&2
    return 1
  fi

  trap - 0 1 2 15
  _tenon_cleanup_download
  _tenon_download_directory=
  _tenon_use_jdk "$_tenon_cached_home"
}
