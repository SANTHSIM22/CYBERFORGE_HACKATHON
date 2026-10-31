// Package migrations embeds the versioned SQL for the three stores.
package migrations

import "embed"

//go:embed control/*.sql
var Control embed.FS

//go:embed source/*.sql
var Source embed.FS

// ClickHouseSchema is applied statement by statement (idempotent DDL).
//
//go:embed clickhouse/001_schema.sql
var ClickHouseSchema string

// SourceSchemaVersion is the newest source migration version. Provisioning
// fails if a tenant database is not at this version after migrating.
const SourceSchemaVersion = 6
