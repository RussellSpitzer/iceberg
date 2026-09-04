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

[[ -n "${_GRADLE_LOADED:-}" ]] && return 0 2>/dev/null || true
_GRADLE_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=release/libs/_constants.sh
source "${LIBS_DIR}/_constants.sh"
# shellcheck source=release/libs/_log.sh
source "${LIBS_DIR}/_log.sh"
# shellcheck source=release/libs/_exec.sh
source "${LIBS_DIR}/_exec.sh"

# Verifies the JDK on PATH is exactly Java 17. Iceberg's deploy.gradle
# enforces this for `-Prelease` builds, so we fail early with a clearer
# message before invoking gradlew.
function verify_jdk_17 {
  if ! command -v java &>/dev/null; then
    print_error "java not found on PATH; release builds require JDK 17"
    return 1
  fi

  local java_version
  java_version=$(java -version 2>&1 | head -1 | sed -E 's/.*version "([^"]+)".*/\1/')
  local major
  if [[ "${java_version}" =~ ^1\.([0-9]+) ]]; then
    major="${BASH_REMATCH[1]}"
  elif [[ "${java_version}" =~ ^([0-9]+) ]]; then
    major="${BASH_REMATCH[1]}"
  else
    print_error "Could not parse Java version: ${java_version}"
    return 1
  fi

  if [[ "${major}" != "17" ]]; then
    print_error "Iceberg releases must be built with Java 17 (deploy.gradle enforces this); detected ${java_version}"
    return 1
  fi
  return 0
}

# Builds the source release tarball.
#
# Tarball contents match dev/source-release.sh (version.txt and
# iceberg-build.properties added via `git archive --add-file`). Sidecar
# files and the tarball itself are written outside the repo tree so that
# a later `-Prelease` Gradle run does not feed them to RAT / spotless.
#
# Args:
#   $1: project root (absolute path)
#   $2: tag prefix without -rcN (e.g. "apache-iceberg-1.10.0")
#   $3: release commit SHA
#   $4: version (e.g. "1.10.0")
#   $5: optional GPG key id to use; defaults to gpg's default key
#
# After success, the tarball, .asc, and .sha512 are present at
# ${source_tarball_dir}/${tag}.tar.gz. Callers should read that global.
function build_source_tarball {
  local projectdir="$1"
  local tag="$2"
  local release_hash="$3"
  local version="$4"
  local keyid="${5:-}"

  local tarball="${tag}.tar.gz"

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD build source tarball ${tarball} from ${release_hash}"
    return 0
  fi

  # Sidecars and the tarball itself stay out of the repo tree. Gradle's
  # release profile runs license / spotless checks against $(pwd); files
  # written next to build.gradle get flagged for missing headers.
  # Populated for callers (prepare-rc.sh copies these into the SVN tree).
  # shellcheck disable=SC2034
  source_tarball_dir=""
  _tarball_sidecar_dir=""
  make_release_tempdir source_tarball_dir
  make_release_tempdir _tarball_sidecar_dir

  print_info "Generating version.txt and iceberg-build.properties..."
  echo "${version}" > "${_tarball_sidecar_dir}/version.txt"
  (cd "${projectdir}" && ./gradlew generateGitProperties)
  cp "${projectdir}/build/iceberg-build.properties" "${_tarball_sidecar_dir}/iceberg-build.properties"

  print_info "Creating tarball ${tarball} using commit ${release_hash}"
  (cd "${projectdir}" && \
    git archive "${release_hash}" \
      --worktree-attributes \
      --prefix "${tag}/" \
      --add-file "${_tarball_sidecar_dir}/version.txt" \
      --add-file "${_tarball_sidecar_dir}/iceberg-build.properties" \
      -o "${source_tarball_dir}/${tarball}")

  print_info "Signing the tarball..."
  local gpg_args=(--armor --output "${source_tarball_dir}/${tarball}.asc" --detach-sig "${source_tarball_dir}/${tarball}")
  if [[ -n "${keyid}" ]]; then
    gpg -u "${keyid}" "${gpg_args[@]}"
  else
    gpg "${gpg_args[@]}"
  fi

  print_info "Generating SHA-512 checksum..."
  (cd "${source_tarball_dir}" && shasum -a 512 "${tarball}") > "${source_tarball_dir}/${tarball}.sha512"
  print_info "Source tarball written to ${source_tarball_dir}/${tarball}"
}

# Stages convenience binaries to Nexus. Two Gradle passes:
#
#   1. Primary-Scala main pass: full Flink/Kafka matrix + the Spark
#      versions that still support the primary Scala (today: Spark 3.x).
#      Spark 4.x is intentionally excluded because it dropped Scala 2.12.
#   2. Secondary-Scala (Spark-only) sweep: one invocation per Spark
#      version in the known matrix, publishing the spark / spark-extensions
#      / spark-runtime modules against the secondary Scala. This covers
#      every Spark version: 3.x (to add the 2.13 variant alongside 2.12)
#      and 4.x (which is published only here).
#
# All matrix values are sourced from gradle.properties via _constants.sh.
#
# Credentials are exported as ORG_GRADLE_PROJECT_mavenUser / mavenPassword
# rather than passed as -PmavenUser=/-PmavenPassword=. Gradle promotes any
# ORG_GRADLE_PROJECT_<name> env var to the project property <name> with
# identical semantics, but the values never enter argv (and so are not
# visible via /proc/<pid>/cmdline).
function stage_convenience_binaries {
  if [[ -z "${NEXUS_USERNAME:-}" || -z "${NEXUS_PASSWORD:-}" ]]; then
    print_warning "NEXUS_USERNAME or NEXUS_PASSWORD not set; Gradle publish will likely fail"
  fi

  export ORG_GRADLE_PROJECT_mavenUser="${NEXUS_USERNAME:-}"
  export ORG_GRADLE_PROJECT_mavenPassword="${NEXUS_PASSWORD:-}"

  local common_flags=(-Prelease --no-parallel --no-configuration-cache)

  if [[ -n "${SPARK_VERSIONS_PRIMARY_SCALA}" ]]; then
    print_info "Publishing main matrix (Scala ${SCALA_PRIMARY}, Flink ${KNOWN_FLINK_VERSIONS}, Spark ${SPARK_VERSIONS_PRIMARY_SCALA}, Kafka ${KNOWN_KAFKA_VERSIONS})..."
    exec_process ./gradlew "${common_flags[@]}" \
      "-DscalaVersion=${SCALA_PRIMARY}" \
      "-DflinkVersions=${KNOWN_FLINK_VERSIONS}" \
      "-DsparkVersions=${SPARK_VERSIONS_PRIMARY_SCALA}" \
      "-DkafkaVersions=${KNOWN_KAFKA_VERSIONS}" \
      publishApachePublicationToMavenRepository
  else
    print_info "No Spark versions support the primary Scala (${SCALA_PRIMARY}); publishing Flink/Kafka matrix only..."
    exec_process ./gradlew "${common_flags[@]}" \
      "-DscalaVersion=${SCALA_PRIMARY}" \
      "-DflinkVersions=${KNOWN_FLINK_VERSIONS}" \
      "-DsparkVersions=" \
      "-DkafkaVersions=${KNOWN_KAFKA_VERSIONS}" \
      publishApachePublicationToMavenRepository
  fi

  if [[ -z "${SPARK_VERSIONS_SECONDARY_SCALA}" ]]; then
    print_info "Known Spark matrix is empty; skipping secondary-Scala (${SCALA_SECONDARY}) sweep."
    return 0
  fi

  local secondary_spark
  IFS=',' read -ra secondary_spark <<< "${SPARK_VERSIONS_SECONDARY_SCALA}"
  local sv
  for sv in "${secondary_spark[@]}"; do
    print_info "Publishing Scala ${SCALA_SECONDARY} artifacts for Spark ${sv}..."
    exec_process ./gradlew "${common_flags[@]}" \
      "-DscalaVersion=${SCALA_SECONDARY}" \
      "-DsparkVersions=${sv}" \
      ":iceberg-spark:iceberg-spark-${sv}_${SCALA_SECONDARY}:publishApachePublicationToMavenRepository" \
      ":iceberg-spark:iceberg-spark-extensions-${sv}_${SCALA_SECONDARY}:publishApachePublicationToMavenRepository" \
      ":iceberg-spark:iceberg-spark-runtime-${sv}_${SCALA_SECONDARY}:publishApachePublicationToMavenRepository"
  done
}
