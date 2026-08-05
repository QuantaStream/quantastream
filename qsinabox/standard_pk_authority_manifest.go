package qsinabox

import (
	"fmt"
	"path/filepath"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
)

// ObserveStandardBSIPrimaryKeyAuthorityManifest reports whether the local
// standard-mode PK authority manifest can be trusted for the mounted catalog.
// It is diagnostic-only; callers must not use this observation as storage
// authority until physical authority artifact loading is implemented.
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

func loadStandardBSIPrimaryKeyAuthorityCatalog(config StandardConfig) (map[string]*core.Table, error) {
	config = config.WithDefaults()
	configDir := filepath.Join(config.DataDir, "config")
	tableNames, err := shared.ActiveOrDiscoveredSchemaTables(configDir, config.Database)
	if err != nil {
		return nil, err
	}
	tables := make(map[string]*core.Table, len(tableNames))
	for _, tableName := range tableNames {
		basic, err := shared.LoadSchema(configDir, tableName, nil)
		if err != nil {
			return nil, err
		}
		table := standardBSIPrimaryKeyAuthorityTable(basic)
		tables[tableName] = table
		if table != nil && table.Name != "" {
			tables[table.Name] = table
		}
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
