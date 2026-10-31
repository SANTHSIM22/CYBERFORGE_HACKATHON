# Windows equivalent of the Makefile (no make required).
#   .\tp.ps1 dev-up
#   .\tp.ps1 tenant-add -Slug tenant-004
#   .\tp.ps1 bench-setup -Tenants 100 -TotalRows 10000000
param(
    [Parameter(Position = 0)][string]$Target = "help",
    [string]$Slug = "",
    [int]$Count = 0,
    [int]$Tenants = 0,
    [long]$TotalRows = 0,
    [long]$Rows = 30000,
    [switch]$Confirm
)
$ErrorActionPreference = "Stop"
function Tp { docker compose --profile tools run --rm tpctl @args; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } }
function Need($v, $name) { if (-not $v) { Write-Error "$name is required for $Target"; exit 2 } }

switch ($Target) {
    "dev-up" {
        if (-not (Test-Path .env)) { Copy-Item .env.example .env }
        docker compose up -d --build; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        Tp wait-active --timeout 15m
        Write-Host "Dashboard: http://localhost:8088   Monitor: http://localhost:8088/monitor"
    }
    "dev-down" { docker compose --profile tools --profile obs --profile demo down }
    "dev-reset" {
        if (-not $Confirm) { Write-Error "refusing: run '.\tp.ps1 dev-reset -Confirm' (deletes all local data)"; exit 1 }
        docker compose --profile tools --profile obs --profile demo down -v
    }
    "status" { docker compose ps; Tp tenant list }
    "logs" { docker compose logs -f --tail=100 ingester api demo }
    "migrate" { Tp init }
    "tenant-add" { Need $Slug "-Slug"; Tp tenant add --slug $Slug }
    "tenant-add-batch" { Need $Count "-Count"; Tp tenant add-batch --count $Count }
    "ensure-tenants" { Need $Count "-Count"; Tp tenant ensure --count $Count }
    "tenant-list" { Tp tenant list }
    "tenant-status" { Need $Slug "-Slug"; Tp tenant status --slug $Slug }
    "migrate-source-all" { Tp migrate-source-all }
    "seed" {
        $t = if ($Tenants) { $Tenants } else { 3 }; $r = if ($TotalRows) { $TotalRows } else { 60000 }
        Tp seed --tenants $t --rows $r
    }
    "seed-additive" { Need $Slug "-Slug"; Tp seed-additive --slug $Slug --rows $Rows }
    "verify" { if ($Slug) { Tp verify --slug $Slug } else { Tp verify } }
    "rebuild" { Need $Slug "-Slug"; Tp rebuild --slug $Slug }
    "test" { Push-Location server; go test ./...; Pop-Location }
    "bench-setup" {
        $t = if ($Tenants) { $Tenants } else { 100 }; $r = if ($TotalRows) { $TotalRows } else { 10000000 }
        Tp seed --tenants $t --rows $r --workers 4
        Tp wait-active --timeout 120m
    }
    "bench-stress" { Tp stress --vus 20 --duration 30s }
    "bench-writes" { Tp write-storm --rate 1000 --viewers 100 --duration 30s }
    "bench-api" { docker compose --profile tools run --rm k6 run /scripts/dashboard.js }
    "bench-ladder" { $s = if ($Slug) { $Slug } else { "tenant-001" }; Tp ladder --slug $s }
    "bench-oltp" {
        Tp capture pause-all; Tp oltp --duration 60s --label capture-off
        Tp capture resume-all; Tp oltp --duration 60s --label capture-on
    }
    "bench-pgbench" { docker compose --profile tools run --rm pgbench -d tenant_001 -n -f /bench/oltp.sql -c 8 -j 4 -T 60 -P 10 -r }
    "bench-freshness" { $s = if ($Slug) { $Slug } else { "tenant-001" }; Tp freshness --slug $s --samples 30 }
    default {
        Write-Host "targets: dev-up dev-down dev-reset status logs migrate tenant-add tenant-add-batch ensure-tenants"
        Write-Host "         tenant-list tenant-status migrate-source-all seed seed-additive verify rebuild"
        Write-Host "         test bench-setup bench-ladder bench-freshness bench-oltp bench-pgbench bench-api bench-stress bench-writes"
    }
}
