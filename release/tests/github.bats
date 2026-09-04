#!/usr/bin/env bats
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

setup() {
  load test_helper/common
  source "${LIBS_DIR}/_github.sh"
}

@test "check_github_checks_passed: fails when GITHUB_TOKEN not set" {
  unset GITHUB_TOKEN
  DRY_RUN=0
  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"GITHUB_TOKEN is required"* ]]
}

@test "check_github_checks_passed: skips in dry-run even without GITHUB_TOKEN" {
  unset GITHUB_TOKEN
  DRY_RUN=1
  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"DRY_RUN"* ]]
}

@test "check_github_checks_passed: skips in dry-run mode" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=1
  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"DRY_RUN"* ]]
}

@test "github_code_ci_check_runs: accepts gh --paginate --slurp array of pages" {
  local payload filtered
  payload='[
    {"check_runs":[{"name":"Release - Prepare RC","status":"in_progress","conclusion":null}]},
    {"check_runs":[{"name":"Java CI","status":"completed","conclusion":"success"}]}
  ]'
  filtered=$(printf '%s\n' "${payload}" | github_code_ci_check_runs)
  [ "$(printf '%s\n' "${filtered}" | jq 'length')" -eq 1 ]
  [ "$(printf '%s\n' "${filtered}" | jq -r '.[0].name')" = "Java CI" ]
}

@test "github_code_ci_check_runs: drops in-progress release workflow but keeps CI failures" {
  local payload filtered
  payload='{"check_runs":[
    {"name":"Prepare Release Candidate","status":"in_progress","conclusion":null},
    {"name":"Java CI","status":"completed","conclusion":"failure"}
  ]}'
  filtered=$(printf '%s\n' "${payload}" | github_code_ci_check_runs)
  [[ "${filtered}" != *"Prepare Release Candidate"* ]]
  [[ "${filtered}" == *"Java CI"* ]]
  [ "$(printf '%s\n' "${filtered}" | jq '[.[] | select(.status != "completed")] | length')" -eq 0 ]
  [ "$(printf '%s\n' "${filtered}" | jq '[.[] | select(.conclusion != "success" and .conclusion != "skipped")] | length')" -eq 1 ]
}

@test "github_code_ci_check_runs: drops historical failed release attempts and workflow-named checks" {
  local payload filtered
  payload='{"check_runs":[
    {"name":"Release - Prepare RC","status":"completed","conclusion":"failure"},
    {"name":"Prepare Release Candidate","status":"completed","conclusion":"failure"},
    {"name":"Publish Release","status":"completed","conclusion":"cancelled"},
    {"name":"Cancel Release Candidate","status":"completed","conclusion":"failure"},
    {"name":"License Check","status":"completed","conclusion":"success"}
  ]}'
  filtered=$(printf '%s\n' "${payload}" | github_code_ci_check_runs)
  [ "$(printf '%s\n' "${filtered}" | jq 'length')" -eq 1 ]
  [ "$(printf '%s\n' "${filtered}" | jq -r '.[0].name')" = "License Check" ]
}

@test "check_github_checks_passed: succeeds when all checks completed and passed" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    echo '{"check_runs":[{"name":"Java CI","status":"completed","conclusion":"success"}]}'
    return 0
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"All GitHub checks passed"* ]]
}

@test "check_github_checks_passed: ignores live self-referential release check" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    echo '{"check_runs":[
      {"name":"Prepare Release Candidate","status":"in_progress","conclusion":null},
      {"name":"Java CI","status":"completed","conclusion":"success"}
    ]}'
    return 0
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"All GitHub checks passed"* ]]
}

@test "check_github_checks_passed: still fails on non-release CI failure beside an in-progress release job" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    echo '{"check_runs":[
      {"name":"Prepare Release Candidate","status":"in_progress","conclusion":null},
      {"name":"Java CI","status":"completed","conclusion":"failure"}
    ]}'
    return 0
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"failed GitHub checks"* ]]
  [[ "$output" == *"Java CI"* ]]
  [[ "$output" != *"Prepare Release Candidate"* ]]
}

@test "check_github_checks_passed: fails when checks are still running" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    echo '{"check_runs":[{"name":"Java CI","status":"in_progress","conclusion":null}]}'
    return 0
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"still-running"* ]]
}

@test "check_github_checks_passed: fails when checks have failed conclusions" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    echo '{"check_runs":[{"name":"Java CI","status":"completed","conclusion":"failure"}]}'
    return 0
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"failed GitHub checks"* ]]
}

@test "check_github_checks_passed: fails when gh api errors" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() { return 1; }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Failed to fetch"* ]]
}
