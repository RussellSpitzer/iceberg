#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

[[ -n "${_EXEC_LOADED:-}" ]] && return 0 2>/dev/null || true
_EXEC_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=release/libs/_constants.sh
source "$LIBS_DIR/_constants.sh"
# shellcheck source=release/libs/_log.sh
source "$LIBS_DIR/_log.sh"

# Replaces secret values with *** in a command string before logging it.
# The redaction list mirrors every secret env var used by the release scripts
# and workflows; add new ones here when introducing additional credentials.
function _redact_secrets {
  local cmd_str="$*"
  local secret_var
  for secret_var in NEXUS_PASSWORD NEXUS_USERNAME SVN_PASSWORD SVN_USERNAME GITHUB_TOKEN ICEBERG_NEXUS_PASSWORD ICEBERG_NEXUS_USER ICEBERG_SVN_DEV_PASSWORD ICEBERG_SVN_DEV_USERNAME ORG_GRADLE_PROJECT_mavenUser ORG_GRADLE_PROJECT_mavenPassword; do
    local secret_val="${!secret_var:-}"
    if [[ -n "${secret_val}" ]]; then
      cmd_str="${cmd_str//${secret_val}/***}"
    fi
  done
  echo "${cmd_str}"
}

# Runs a command in real mode, prints what it would have done in dry-run mode.
# Always logs a redacted form of the command. Exit status is preserved so
# callers can react to failures.
function exec_process {
  local redacted
  redacted=$(_redact_secrets "$@")
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    print_command "Executing '${redacted}'"
    "$@"
  else
    print_command "Dry-run, WOULD execute '${redacted}'"
  fi
}

# Retries a command up to max_attempts times. Between failed attempts the
# given cleanup_path (typically a partially-written checkout dir) is removed
# so the next attempt starts from a clean state. Apache SVN occasionally
# returns transient connection failures during checkout/commit, which is
# why this helper exists.
function exec_process_with_retries {
  if [[ $# -lt 4 ]]; then
    echo "ERROR: exec_process_with_retries requires: max_attempts sleep_duration cleanup_path command [args...]"
    exit 1
  fi

  local max_attempts="${1}"
  local sleep_duration="${2}"
  local cleanup_path="${3}"
  shift 3

  local attempt=1
  local redacted
  redacted=$(_redact_secrets "$@")
  while true; do
    if exec_process "$@"; then
      break
    fi
    if [[ $attempt -ge $max_attempts ]]; then
      echo "ERROR: Command failed after ${max_attempts} attempts: ${redacted}"
      exit 1
    fi
    echo "WARNING: Command failed (attempt ${attempt}/${max_attempts}), retrying in ${sleep_duration} seconds..."
    if [[ -n "${cleanup_path}" && -e "${cleanup_path}" ]]; then
      rm -rf "${cleanup_path}"
    fi
    sleep "${sleep_duration}"
    ((attempt++))
  done
}

# Transient directories used by the release scripts (SVN working copies,
# tarball sidecars, credential-adjacent config). Initialized so an EXIT
# trap that fires before the first allocation is safe under `set -u`.
RELEASE_TEMPDIRS=()

function _cleanup_release_tempdirs {
  # Command substitution inherits EXIT traps. Skip cleanup in subshells so
  # a `path=$(mktemp ...)` inside this helper cannot wipe parent temp dirs.
  [[ ${BASH_SUBSHELL} -eq 0 ]] || return 0
  local d
  for d in "${RELEASE_TEMPDIRS[@]+"${RELEASE_TEMPDIRS[@]}"}"; do
    rm -rf "${d}"
  done
}

# Creates a directory outside the repo working tree and stores its path in
# the named variable. Call as `make_release_tempdir varname` (not inside
# command substitution, which would lose the RELEASE_TEMPDIRS registration).
# On GitHub Actions the parent is $RUNNER_TEMP (wiped per job); locally
# $TMPDIR or /tmp. Removed on EXIT of the top-level shell.
#
# The destination variable must not be `local` in the caller: this uses
# eval rather than namerefs so the helper runs on Bash 3.2.
function make_release_tempdir {
  if [[ $# -lt 1 ]]; then
    echo "ERROR: make_release_tempdir requires a destination variable name" >&2
    return 1
  fi
  if [[ ! "$1" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
    echo "ERROR: make_release_tempdir: invalid variable name: $1" >&2
    return 1
  fi
  if [[ ${BASH_SUBSHELL} -eq 0 && -z "${_RELEASE_TEMPDIR_TRAP_INSTALLED:-}" ]]; then
    trap _cleanup_release_tempdirs EXIT
    _RELEASE_TEMPDIR_TRAP_INSTALLED=1
  fi
  local parent="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
  local _release_td_path
  _release_td_path="$(mktemp -d "${parent}/iceberg-release.XXXXXX")"
  RELEASE_TEMPDIRS+=("${_release_td_path}")
  # shellcheck disable=SC2086 # $1 is validated as an identifier above
  eval "$1=\"\${_release_td_path}\""
}

# Writes "<sha512>  <basename>" to <source_file>.sha512. Done in a subshell
# so the relative basename (rather than the full path) is recorded, matching
# the format consumed by `shasum -c`.
function calculate_sha512 {
  local source_file="$1"
  local source_dir source_base
  source_dir="$(dirname "${source_file}")"
  source_base="$(basename "${source_file}")"
  local target_file="${source_file}.sha512"
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    (cd "${source_dir}" && shasum -a 512 "${source_base}") > "${target_file}"
  else
    print_command "Dry-run, WOULD run: cd ${source_dir} && shasum -a 512 ${source_base} > ${target_file}"
  fi
}
