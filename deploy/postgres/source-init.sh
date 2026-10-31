#!/bin/sh
# Runs once when the source cluster is first initialised (docker-entrypoint-initdb.d).
# Least-privilege roles; tenant databases are created later by the provisioner.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v prov_pw="$TP_PROVISIONER_PASSWORD" -v repl_pw="$TP_REPLICATOR_PASSWORD" \
  -v demo_pw="$TP_DEMO_PASSWORD" -v app_pw="$TP_APP_PASSWORD" <<'EOSQL'
-- Creates tenant databases, owns their tables, runs source migrations and publications.
CREATE ROLE tp_provisioner LOGIN CREATEDB PASSWORD :'prov_pw';
-- The ingester: logical replication + snapshot reads of captured tables. Not a superuser.
CREATE ROLE tp_replicator LOGIN REPLICATION PASSWORD :'repl_pw';
-- Demo service (DEMO_MODE only) and the OLTP benchmark workload.
CREATE ROLE tp_demo LOGIN PASSWORD :'demo_pw';
CREATE ROLE tp_app LOGIN PASSWORD :'app_pw';
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
GRANT pg_read_all_stats TO tp_provisioner;
GRANT pg_monitor TO tp_replicator;
EOSQL
