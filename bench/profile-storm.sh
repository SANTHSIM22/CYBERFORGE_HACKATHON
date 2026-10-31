#!/usr/bin/env bash
# Samples where CPU and time go while a write storm runs, to separate design
# costs from machine effects. Writes bench/results/profile-<time>.log.
#   - CPU per container (docker stats)
#   - store Postgres CPU split: change-capture walsenders vs client backends
#   - ingester flush timings (tp.ingester_stats)
# and afterwards ClickHouse's own timings for every statement kind.
# Usage: bench/profile-storm.sh 240   (seconds to sample)
set -uo pipefail
cd "$(dirname "$0")/.."
secs="${1:-240}"
out="bench/results/profile-$(date -u +%Y%m%d-%H%M%S).log"
start=$(date -u +"%Y-%m-%d %H:%M:%S")
ch() { docker compose exec -T clickhouse clickhouse-client -q "$1"; }
pgcpu() { # cumulative CPU ticks of walsenders vs other backends inside the store Postgres
  docker compose exec -T source-postgres sh -c '
    w=0; o=0
    for p in /proc/[0-9]*; do
      c=$(tr -d "\0" < $p/cmdline 2>/dev/null)
      case "$c" in postgres:*) ;; *) continue ;; esac
      t=$(awk "{print \$14+\$15}" $p/stat 2>/dev/null) || continue
      case "$c" in *walsender*) w=$((w+t)) ;; *) o=$((o+t)) ;; esac
    done
    echo "$w $o"'
}
{
  echo "profile from $start UTC for ${secs}s"
  read w0 o0 < <(pgcpu)
  t0=$(date +%s)
  end=$((t0 + secs))
  while [ "$(date +%s)" -lt "$end" ]; do
    echo "--- $(date -u +%T)"
    docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" | grep tenantpulse | sed 's/tenantpulse-//; s/-1 / /' | tr '\n' ' '
    echo
    ch "SELECT worker, events_per_sec, flush_p50_ms, flush_p95_ms, flush_p99_ms FROM tp.ingester_stats FINAL FORMAT TSV" 2>/dev/null
    sleep 8
  done
  read w1 o1 < <(pgcpu)
  el=$(( $(date +%s) - t0 ))
  hz=100
  echo "=== store Postgres CPU over ${el}s: walsenders $(( (w1-w0) * 100 / hz / el ))% of one core, all other backends $(( (o1-o0) * 100 / hz / el ))%"
  echo "=== ClickHouse statements during the window (server-side)"
  ch "SYSTEM FLUSH LOGS"
  ch "
    SELECT multiIf(query ILIKE 'INSERT INTO tp.order_day_rollup%', 'rollup: order day',
                   query ILIKE 'INSERT INTO tp.product_day_rollup%', 'rollup: product day',
                   query ILIKE 'INSERT INTO tp.order_hour_rollup%', 'rollup: order hour',
                   query ILIKE 'INSERT INTO tp.orders%', 'raw: orders',
                   query ILIKE 'INSERT INTO tp.order_items%', 'raw: order_items',
                   query ILIKE 'INSERT INTO tp.tenant_status%', 'status',
                   user = 'tp_api' AND query ILIKE '%_version >%', 'api: view sync',
                   user = 'tp_api' AND query ILIKE '%LEFT JOIN%', 'api: top windows',
                   user = 'tp_api', 'api: other query',
                   user = 'tp_monitor', 'monitor/poll', 'other') AS kind,
           count() AS n, round(quantile(0.5)(query_duration_ms)) AS p50_ms, round(quantile(0.95)(query_duration_ms)) AS p95_ms,
           round(max(query_duration_ms)) AS max_ms, round(sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6, 1) AS cpu_s,
           round(avg(read_rows)) AS avg_rows
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_time >= toDateTime('$start') AND event_time < toDateTime('$start') + INTERVAL $secs SECOND
    GROUP BY kind ORDER BY cpu_s DESC FORMAT PrettyCompactMonoBlock"
} 2>&1 | tee "$out"
