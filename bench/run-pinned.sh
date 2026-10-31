#!/usr/bin/env bash
# Runs a tpctl benchmark with each tier on its own CPU cores, standing in for
# separate servers on one 8-core machine:
#   store Postgres      cores 0-2
#   ClickHouse          cores 3-5
#   API, ingester, demo, control plane and the load generator   cores 6-7
# Without this, everything shares every core and the load generator and
# ClickHouse steal CPU from the store database, which inflates "store
# slowdown" numbers in a way separate servers would not.
#
# Usage:  bench/run-pinned.sh stress --vus 20 --duration 30s
#         bench/run-pinned.sh write-storm --rate 1000 --viewers 100
#         bench/run-pinned.sh ladder --slug tenant-001
set -euo pipefail
cd "$(dirname "$0")/.."
p=tenantpulse
pin() { docker update --cpuset-cpus "$1" "$p-$2-1" >/dev/null 2>&1 || true; }
unpin() {
  for c in source-postgres clickhouse api api-direct ingester demo control-postgres web; do pin 0-7 "$c"; done
}
trap unpin EXIT

pin 0-2 source-postgres
pin 3-5 clickhouse
for c in api api-direct ingester demo control-postgres web; do pin 6-7 "$c"; done

extra=()
case "${1:-}" in stress | write-storm) extra=(--cpu-pinned) ;; esac
name="tp-bench-$$"
docker compose --profile tools run -d --name "$name" tpctl "$@" "${extra[@]}" >/dev/null
docker update --cpuset-cpus 6-7 "$name" >/dev/null
docker logs -f "$name"
code=$(docker wait "$name")
docker rm "$name" >/dev/null
exit "$code"
