#!/usr/bin/env bash
# Fresh fleet at a given size, then the full measurement suite (bench/suite.sh).
# DESTRUCTIVE: deletes all local data (Docker volumes). Benchmark history in
# ClickHouse is exported first and restored afterwards.
#
#   bench/scale.sh 10 1000000 1M          10 stores, 1M rows
#   bench/scale.sh 100 10000000 10M       100 stores, 10M rows
#
# Each step prints a "#### " marker.
set -uo pipefail
cd "$(dirname "$0")/.."
tenants="$1" rows="$2" label="$3"
hist="bench/results/benchmark_runs-before-${label}.jsonl"
mark() { echo "#### $1 ($(date -u +%T))"; }

mark "export benchmark history"
docker compose exec -T clickhouse clickhouse-client -q "SELECT * FROM tp.benchmark_runs FORMAT JSONEachRow" > "$hist" 2>/dev/null || true
mark "reset"
docker compose --profile tools --profile obs --profile demo down -v 2>&1 | tail -1
mark "start + seed $tenants stores / $rows rows"
SEED_TENANTS="$tenants" SEED_ROWS="$rows" docker compose up -d 2>&1 | tail -1
until [ "$(docker inspect -f '{{.State.Status}}' tenantpulse-bootstrap-1 2>/dev/null)" = "exited" ]; do sleep 5; done
docker logs tenantpulse-bootstrap-1 2>&1 | tail -6
mark "seeded; waiting for every store's first copy"
docker compose --profile tools run --rm tpctl wait-active --timeout 120m 2>&1 | tail -2
mark "all stores active"
[ -s "$hist" ] && docker compose exec -T clickhouse clickhouse-client -q "INSERT INTO tp.benchmark_runs FORMAT JSONEachRow" < "$hist"
docker compose exec -T clickhouse clickhouse-client -q "
  SELECT 'stores', uniqExact(tenant_id) FROM tp.orders
  UNION ALL SELECT 'orders', count() FROM tp.orders FINAL
  UNION ALL SELECT 'order_items', count() FROM tp.order_items FINAL
  UNION ALL SELECT 'products', count() FROM tp.products FINAL"
mark "suite"
bench/suite.sh "$label"
mark "finished"
