#!/usr/bin/env bash
# Runs every PRD measurement in a fixed order and keeps the full output in
# bench/results/suite-<label>-<time>.log. Results are also published to the
# dashboard (/monitor) and saved as JSON/Markdown in bench/results/.
#
#   1. verify       exact match: every widget (ClickHouse and in-memory views) vs direct SQL, every store
#   2. ladder       one query at a time on the largest store: no index, B-tree, covering index, ClickHouse
#   3. freshness    commit -> queryable in ClickHouse -> visible through the API
#   4. oltp         store latency with change capture paused vs running (DB safety, B-3; CPU-pinned)
#   5. stress       read stress: dashboards hammered, direct-to-DB vs aggregation layer (CPU-pinned)
#   6. write-storm  every store writing at a high rate while dashboards watch (CPU-pinned)
#   7. db safety    write storm at a sustainable rate: store latency with vs without 100 dashboard viewers
#
# Usage: bench/suite.sh 10M     (label is free text, e.g. the data size)
set -uo pipefail
cd "$(dirname "$0")/.."
label="${1:-run}"
log="bench/results/suite-${label}-$(date -u +%Y%m%d-%H%M%S).log"
tp() { docker compose --profile tools run --rm tpctl "$@"; }
step() { printf '\n==== %s  (%s)\n' "$1" "$(date -u +%H:%M:%S)"; }
{
  step "fleet";       tp tenant list
  step "verify";      tp verify
  step "ladder";      bench/run-pinned.sh ladder --slug tenant-001
  step "freshness";   tp freshness --slug tenant-001 --samples 30
  step "oltp: capture paused";  tp capture pause-all; bench/run-pinned.sh oltp --duration 30s --label capture-off
  step "oltp: capture running"; tp capture resume-all; bench/run-pinned.sh oltp --duration 30s --label capture-on
  step "read stress"; bench/run-pinned.sh stress --vus 20 --duration 30s
  step "write storm (heavy)"; bench/run-pinned.sh write-storm --rate "${RATE:-1000}" --viewers "${VIEWERS:-100}" --duration 30s
  # PRD database safety: store latency with vs without dashboard load, at a rate the store can sustain
  step "database safety"; bench/run-pinned.sh write-storm --rate "${SAFE_RATE:-300}" --viewers "${VIEWERS:-100}" --duration 30s --direct-url=
  step "done"
} 2>&1 | tee "$log"
echo "log: $log"
