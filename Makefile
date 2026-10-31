# Thin wrappers around docker compose + tpctl (which runs inside the network).
# Windows without make: use ./tp.ps1 <target> with the same target names.
COMPOSE ?= docker compose
TPCTL    = $(COMPOSE) --profile tools run --rm tpctl

.PHONY: dev-up dev-down dev-reset status logs migrate tenant-add tenant-add-batch ensure-tenants tenant-list \
        tenant-status migrate-source-all seed seed-additive verify rebuild test bench-setup bench-ladder \
        bench-freshness bench-oltp bench-pgbench bench-api bench-stress bench-writes bench

dev-up:            ## start everything; first start seeds 3 tenants
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build
	$(TPCTL) wait-active --timeout 15m

dev-down:          ## stop; named volumes (all tenant data) are preserved
	$(COMPOSE) --profile tools --profile obs --profile demo down

dev-reset:         ## DESTRUCTIVE: deletes all local data. Requires CONFIRM=yes
	@if [ "$(CONFIRM)" != "yes" ]; then echo "refusing: run 'make dev-reset CONFIRM=yes'"; exit 1; fi
	$(COMPOSE) --profile tools --profile obs --profile demo down -v

status:
	$(COMPOSE) ps
	$(TPCTL) tenant list

logs:
	$(COMPOSE) logs -f --tail=100 ingester api demo

migrate:
	$(TPCTL) init

tenant-add:        ## make tenant-add SLUG=tenant-004
	$(TPCTL) tenant add --slug $(SLUG)

tenant-add-batch:  ## make tenant-add-batch COUNT=5
	$(TPCTL) tenant add-batch --count $(COUNT)

ensure-tenants:    ## make ensure-tenants COUNT=10 (monotonic: never removes)
	$(TPCTL) tenant ensure --count $(COUNT)

tenant-list:
	$(TPCTL) tenant list

tenant-status:
	$(TPCTL) tenant status --slug $(SLUG)

migrate-source-all:
	$(TPCTL) migrate-source-all

seed:              ## make seed TENANTS=3 TOTAL_ROWS=60000
	$(TPCTL) seed --tenants $(or $(TENANTS),3) --rows $(or $(TOTAL_ROWS),60000)

seed-additive:     ## make seed-additive SLUG=tenant-002 ROWS=20000
	$(TPCTL) seed-additive --slug $(SLUG) --rows $(or $(ROWS),20000)

verify:            ## exact reconciliation vs direct source SQL
	$(TPCTL) verify $(if $(SLUG),--slug $(SLUG),)

rebuild:           ## make rebuild SLUG=tenant-002 (B-2 shadow generation + cutover)
	$(TPCTL) rebuild --slug $(SLUG)

test:
	cd server && go test ./...

bench-setup:       ## make bench-setup TENANTS=100 TOTAL_ROWS=10000000
	$(TPCTL) seed --tenants $(or $(TENANTS),100) --rows $(or $(TOTAL_ROWS),10000000) --workers 4
	$(TPCTL) wait-active --timeout 120m

bench-ladder:      ## no index vs B-tree vs covering index vs ClickHouse
	$(TPCTL) ladder --slug $(or $(SLUG),tenant-001)

bench-freshness:
	$(TPCTL) freshness --slug $(or $(SLUG),tenant-001) --samples 30

bench-oltp:        ## store latency with capture paused vs running
	$(TPCTL) capture pause-all
	$(TPCTL) oltp --duration 60s --label capture-off
	$(TPCTL) capture resume-all
	$(TPCTL) oltp --duration 60s --label capture-on

bench-pgbench:     ## pgbench on the largest tenant (judge-recognisable TPS/latency)
	$(COMPOSE) --profile tools run --rm pgbench -d tenant_001 -n -f /bench/oltp.sql -c 8 -j 4 -T 60 -P 10 -r

bench-stress:      ## direct-primary vs aggregation instances under identical load
	API_TENANT_CONCURRENCY=64 API_DB_CONCURRENCY=160 $(COMPOSE) --profile compare up -d --force-recreate api api-direct
	$(TPCTL) stress --vus 20 --duration 30s

bench-writes:      ## write storm: every store writing hard while dashboards watch (RATE=1000 VIEWERS=100)
	$(TPCTL) write-storm --rate $(or $(RATE),1000) --viewers $(or $(VIEWERS),100) --duration 30s

bench-api:
	$(COMPOSE) --profile tools run --rm k6 run /scripts/dashboard.js

bench: bench-ladder bench-freshness bench-oltp bench-api
