package qsruntime

import "strings"

// MetadataChangeKind classifies Consul/catalog metadata invalidation causes.
type MetadataChangeKind string

const (
	// MetadataChangeCreate means a catalog object was created.
	MetadataChangeCreate MetadataChangeKind = "create"
	// MetadataChangeModify means a catalog object was modified.
	MetadataChangeModify MetadataChangeKind = "modify"
	// MetadataChangeDrop means a catalog object was dropped.
	MetadataChangeDrop MetadataChangeKind = "drop"
)

// MetadataChangeEvent is the qsruntime-neutral form of a catalog metadata change.
type MetadataChangeEvent struct {
	Table string
	Kind  MetadataChangeKind
}

// RuntimeMetadataInvalidator centralizes process-local catalog metadata cache invalidation.
//
// It is intentionally adapter-neutral. Consul schema watches and future admin
// APIs should enter through this boundary rather than clearing catalog caches
// independently. KVStore-backed StringEnum dictionaries are not catalog
// metadata and must use RuntimeDictionaryInvalidator instead.
type RuntimeMetadataInvalidator struct {
	Catalog       CatalogInvalidationTarget
	Tables        TableInvalidationTarget
	DefaultSchema string
}

// CatalogInvalidationTarget is the narrow cache hook required for catalog metadata.
type CatalogInvalidationTarget interface {
	InvalidateTable(schema string, name string)
}

// TableInvalidationTarget is the runtime hook for table-scoped cached sessions and table metadata.
type TableInvalidationTarget interface {
	InvalidateTable(name string)
}

// ApplyChange invalidates metadata affected by a catalog table change.
func (i RuntimeMetadataInvalidator) ApplyChange(event MetadataChangeEvent) {
	table := strings.TrimSpace(event.Table)
	if table == "" {
		return
	}
	i.InvalidateTable(i.DefaultSchema, table)
	i.InvalidateRuntimeTable(table)
}

// InvalidateTable evicts cached catalog metadata for a table.
func (i RuntimeMetadataInvalidator) InvalidateTable(schema string, table string) {
	table = strings.TrimSpace(table)
	if table == "" {
		return
	}
	schema = strings.TrimSpace(schema)
	if schema == "" {
		schema = strings.TrimSpace(i.DefaultSchema)
	}
	if i.Catalog == nil {
		return
	}
	i.Catalog.InvalidateTable(schema, table)
	if schema != "" {
		i.Catalog.InvalidateTable("", table)
	}
}

// InvalidateRuntimeTable evicts table-scoped runtime state such as pooled sessions.
func (i RuntimeMetadataInvalidator) InvalidateRuntimeTable(table string) {
	table = strings.TrimSpace(table)
	if table == "" || i.Tables == nil {
		return
	}
	i.Tables.InvalidateTable(table)
}
