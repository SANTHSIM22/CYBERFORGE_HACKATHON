#!/bin/sh
# Runs once when the control-plane cluster is first initialised.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v owner_pw="$TP_OWNER_PASSWORD" -v ingest_pw="$TP_INGEST_PASSWORD" -v api_pw="$TP_API_PASSWORD" <<'EOSQL'
CREATE ROLE tp_owner LOGIN PASSWORD :'owner_pw';    -- migrations, provisioning
CREATE ROLE tp_ingest LOGIN PASSWORD :'ingest_pw';  -- reads registry, updates capture state
CREATE ROLE tp_api LOGIN PASSWORD :'api_pw';        -- reads registry
CREATE DATABASE tenantpulse_control OWNER tp_owner;
EOSQL
