package qsinabox

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
)

// ObserveStandardBSIPrimaryKeyAuthorityManifest reports whether the local
// standard-mode PK authority manifest can be trusted for the mounted catalog.
// A trusted manifest can enable existing catalog-designated PK BSIs as the
// authority path; separate physical authority artifacts remain future work.
func ObserveStandardBSIPrimaryKeyAuthorityManifest(config StandardConfig) core.BSIPrimaryKeyAuthorityManifestObservation {
	config = config.WithDefaults()
	manifest, err := core.LoadBSIPrimaryKeyAuthorityManifest(config.DataDir)
	if err != nil {
		return core.BSIPrimaryKeyAuthorityManifestObservation{
			Status: core.BSIPrimaryKeyAuthorityManifestStatusInvalid,
			Detail: fmt.Sprintf("load manifest: %v", err),
		}
	}
	if manifest.Version == 0 && len(manifest.Entries) == 0 {
		return manifest.ObserveAgainstCatalog(nil)
	}

	tables, err := loadStandardBSIPrimaryKeyAuthorityCatalog(config)
	if err != nil {
		return core.BSIPrimaryKeyAuthorityManifestObservation{
			Status:  core.BSIPrimaryKeyAuthorityManifestStatusInvalid,
			Detail:  fmt.Sprintf("load active catalog: %v", err),
			Entries: len(manifest.Entries),
		}
	}
	return manifest.ObserveAgainstCatalog(tables)
}

// BuildStandardBSIPrimaryKeyAuthorityManifest creates a logical manifest from
// the mounted standard-mode catalog snapshot. It does not make physical
// authority artifacts durable; callers decide when to persist the manifest.
func BuildStandardBSIPrimaryKeyAuthorityManifest(config StandardConfig, source string) (core.BSIPrimaryKeyAuthorityManifest, error) {
	config = config.WithDefaults()
	tables, err := loadStandardBSIPrimaryKeyAuthorityCatalogTables(config)
	if err != nil {
		return core.BSIPrimaryKeyAuthorityManifest{}, err
	}
	manifest := core.BSIPrimaryKeyAuthorityManifest{
		Version:     core.BSIPrimaryKeyAuthorityManifestVersion,
		GeneratedAt: time.Now().UTC(),
		Source:      strings.TrimSpace(source),
		Entries:     make([]core.BSIPrimaryKeyAuthorityManifestEntry, 0, len(tables)),
	}
	for _, catalogTable := range tables {
		table := catalogTable.Table
		if table == nil || table.BasicTable == nil || strings.TrimSpace(table.PrimaryKey) == "" {
			continue
		}
		entry, err := core.NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
		if err != nil {
			return core.BSIPrimaryKeyAuthorityManifest{}, fmt.Errorf("build BSI primary-key authority manifest entry for %s: %w", table.Name, err)
		}
		manifest.Entries = append(manifest.Entries, entry)
	}
	return manifest, nil
}

func loadStandardBSIPrimaryKeyAuthorityCatalog(config StandardConfig) (map[string]*core.Table, error) {
	tables, err := loadStandardBSIPrimaryKeyAuthorityCatalogTables(config)
	if err != nil {
		return nil, err
	}
	catalog := make(map[string]*core.Table, len(tables))
	for _, catalogTable := range tables {
		table := catalogTable.Table
		if table == nil {
			continue
		}
		if catalogTable.ActiveName != "" {
			catalog[catalogTable.ActiveName] = table
		}
		if table.Name != "" {
			catalog[table.Name] = table
		}
	}
	return catalog, nil
}

type standardBSIPrimaryKeyAuthorityCatalogTableEntry struct {
	ActiveName string
	Table      *core.Table
}

func loadStandardBSIPrimaryKeyAuthorityCatalogTables(config StandardConfig) ([]standardBSIPrimaryKeyAuthorityCatalogTableEntry, error) {
	config = config.WithDefaults()
	configDir := filepath.Join(config.DataDir, "config")
	tableNames, err := shared.ActiveOrDiscoveredSchemaTables(configDir, config.Database)
	if err != nil {
		return nil, err
	}
	tables := make([]standardBSIPrimaryKeyAuthorityCatalogTableEntry, 0, len(tableNames))
	for _, tableName := range tableNames {
		basic, err := shared.LoadSchema(configDir, tableName, nil)
		if err != nil {
			return nil, err
		}
		table := standardBSIPrimaryKeyAuthorityTable(basic)
		tables = append(tables, standardBSIPrimaryKeyAuthorityCatalogTableEntry{
			ActiveName: tableName,
			Table:      table,
		})
	}
	return tables, nil
}

func standardBSIPrimaryKeyAuthorityTable(basic *shared.BasicTable) *core.Table {
	if basic == nil {
		return nil
	}
	table := &core.Table{
		BasicTable:       basic,
		Attributes:       make([]core.Attribute, len(basic.Attributes)),
		AttributeNameMap: make(map[string]*core.Attribute, len(basic.Attributes)),
	}
	for i := range basic.Attributes {
		table.Attributes[i] = core.Attribute{
			BasicAttribute: &basic.Attributes[i],
			Parent:         table,
		}
		table.AttributeNameMap[table.Attributes[i].FieldName] = &table.Attributes[i]
	}
	return table
}
