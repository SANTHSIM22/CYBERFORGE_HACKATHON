#!/usr/bin/env bash
# Final measurement: build, fresh 1M fleet + full suite, then (only if 1M is
# exact and the ingester is healthy) fresh 10M fleet + full suite.
# Output: bench/results/final-run.log plus the per-suite logs.
set -uo pipefail
cd "$(dirname "$0")/.."
mark() { echo "#### $1 ($(date -u +%T))"; }
{
  mark "build"
  docker compose build migrate web 2>&1 | tail -2
  bench/scale.sh 10 1000000 1M
  s1=$(ls -t bench/results/suite-1M-*.log | head -1)
  if ! grep -q "10 tenants, [0-9]* checks, 0 tenants with mismatches" "$s1"; then
    mark "STOP: 1M exactness failed, see $s1"; exit 1
  fi
  if docker compose logs ingester 2>&1 | grep -qi "rollup:"; then
    mark "STOP: ingester rollup errors at 1M"; docker compose logs ingester 2>&1 | grep -i "rollup:" | tail -3; exit 1
  fi
  mark "1M passed; starting 10M"
  bench/scale.sh 100 10000000 10M
  mark "all done"
} 2>&1 | tee bench/results/final-run.log
