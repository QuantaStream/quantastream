package core

import (
	"fmt"
	"strings"
	"time"
)

const (
	// BSIPrimaryKeyAuthorityManifestFileName is the intended persisted startup
	// index for BSI-backed primary-key authority artifacts.
	BSIPrimaryKeyAuthorityManifestFileName = "BSI_PRIMARY_KEY_AUTHORITY_MANIFEST"

	// BSIPrimaryKeyAuthorityManifestVersion is the manifest schema version.
	BSIPrimaryKeyAuthorityManifestVersion = 1

	// BSIPrimaryKeyAuthorityManifestStatusOK means the manifest matches the
	// current catalog key shape and identity encoding.
	BSIPrimaryKeyAuthorityManifestStatusOK = "ok"

	// BSIPrimaryKeyAuthorityManifestStatusMissing means no usable manifest was
	// supplied. Callers may rebuild from authoritative table data.
	BSIPrimaryKeyAuthorityManifestStatusMissing = "missing"

	// BSIPrimaryKeyAuthorityManifestStatusInvalid means the manifest must not be
	// trusted for primary-key authority.
	BSIPrimaryKeyAuthorityManifestStatusInvalid = "invalid"
)

// BSIPrimaryKeyAuthorityManifest records the logical identity contract for
// BSI-backed primary-key authority artifacts.
//
// The manifest is catalog-adjacent validation metadata. It does not make the
// persisted bitmap files authoritative by itself; callers still need to load
// the actual BSI data and verify normal storage lifecycle rules.
type BSIPrimaryKeyAuthorityManifest struct {
	Version     int                                   `yaml:"version" json:"version"`
	GeneratedAt time.Time                             `yaml:"generated_at,omitempty" json:"generated_at,omitempty"`
	Source      string                                `yaml:"source,omitempty" json:"source,omitempty"`
	Entries     []BSIPrimaryKeyAuthorityManifestEntry `yaml:"entries" json:"entries"`
}

// BSIPrimaryKeyAuthorityManifestEntry describes one table's primary-key
// authority shape as written by a future persisted BSI authority artifact.
type BSIPrimaryKeyAuthorityManifestEntry struct {
	SchemaName      string                                `yaml:"schema_name,omitempty" json:"schema_name,omitempty"`
	TableName       string                                `yaml:"table" json:"table"`
	PrimaryKey      string                                `yaml:"primary_key" json:"primary_key"`
	EncodingVersion int                                   `yaml:"encoding_version" json:"encoding_version"`
	Fields          []BSIPrimaryKeyAuthorityManifestField `yaml:"fields" json:"fields"`
	LogicalShard    string                                `yaml:"logical_shard,omitempty" json:"logical_shard,omitempty"`
	ArtifactPath    string                                `yaml:"artifact_path,omitempty" json:"artifact_path,omitempty"`
	Fingerprint     string                                `yaml:"fingerprint,omitempty" json:"fingerprint,omitempty"`
	Clean           bool                                  `yaml:"clean" json:"clean"`
	CreatedAt       time.Time                             `yaml:"created_at,omitempty" json:"created_at,omitempty"`
	ModifiedAt      time.Time                             `yaml:"modified_at,omitempty" json:"modified_at,omitempty"`
}

// BSIPrimaryKeyAuthorityManifestField captures the effective primary-key field
// sequence. For time-sharded tables this includes the time quantum field because
// GetPrimaryKeyInfo includes it in the authority identity shape.
type BSIPrimaryKeyAuthorityManifestField struct {
	Name            string `yaml:"name" json:"name"`
	Type            string `yaml:"type,omitempty" json:"type,omitempty"`
	MappingStrategy string `yaml:"mapping_strategy,omitempty" json:"mapping_strategy,omitempty"`
	ColumnID        bool   `yaml:"column_id,omitempty" json:"column_id,omitempty"`
}

// BSIPrimaryKeyAuthorityManifestObservation reports whether a persisted
// authority manifest matches the current catalog.
type BSIPrimaryKeyAuthorityManifestObservation struct {
	Status        string
	Detail        string
	ManifestEntry string
	Entries       int
}

// NewBSIPrimaryKeyAuthorityManifestEntry builds the logical manifest contract
// for a table from the catalog's effective primary-key shape.
func NewBSIPrimaryKeyAuthorityManifestEntry(table *Table, logicalShard string) (BSIPrimaryKeyAuthorityManifestEntry, error) {
	if table == nil || table.BasicTable == nil {
		return BSIPrimaryKeyAuthorityManifestEntry{}, fmt.Errorf("primary-key authority manifest entry requires table")
	}
	if strings.TrimSpace(table.Name) == "" {
		return BSIPrimaryKeyAuthorityManifestEntry{}, fmt.Errorf("primary-key authority manifest entry requires table name")
	}
	if strings.TrimSpace(table.PrimaryKey) == "" {
		return BSIPrimaryKeyAuthorityManifestEntry{}, fmt.Errorf("primary-key authority manifest entry requires primary key")
	}
	attrs, err := table.GetPrimaryKeyInfo()
	if err != nil {
		return BSIPrimaryKeyAuthorityManifestEntry{}, err
	}
	if len(attrs) == 0 {
		return BSIPrimaryKeyAuthorityManifestEntry{}, fmt.Errorf("table %s has no primary-key authority fields", table.Name)
	}

	fields := make([]BSIPrimaryKeyAuthorityManifestField, 0, len(attrs))
	for _, attr := range attrs {
		if attr == nil || attr.BasicAttribute == nil {
			return BSIPrimaryKeyAuthorityManifestEntry{}, fmt.Errorf("table %s has incomplete primary-key authority field", table.Name)
		}
		fields = append(fields, BSIPrimaryKeyAuthorityManifestField{
			Name:            attr.FieldName,
			Type:            attr.Type,
			MappingStrategy: attr.MappingStrategy,
			ColumnID:        attr.ColumnID,
		})
	}

	return BSIPrimaryKeyAuthorityManifestEntry{
		TableName:       table.Name,
		PrimaryKey:      strings.TrimSpace(table.PrimaryKey),
		EncodingVersion: PrimaryKeyIdentityEncodingVersion,
		Fields:          fields,
		LogicalShard:    logicalShard,
		Clean:           true,
	}, nil
}

// ObserveAgainstCatalog validates the manifest against the current table cache.
func (m BSIPrimaryKeyAuthorityManifest) ObserveAgainstCatalog(tables map[string]*Table) BSIPrimaryKeyAuthorityManifestObservation {
	observation := BSIPrimaryKeyAuthorityManifestObservation{
		Status:  BSIPrimaryKeyAuthorityManifestStatusOK,
		Entries: len(m.Entries),
	}
	if m.Version == 0 && len(m.Entries) == 0 {
		observation.Status = BSIPrimaryKeyAuthorityManifestStatusMissing
		observation.Detail = "manifest is empty"
		return observation
	}
	if m.Version != BSIPrimaryKeyAuthorityManifestVersion {
		observation.Status = BSIPrimaryKeyAuthorityManifestStatusInvalid
		observation.Detail = fmt.Sprintf("manifest version=%d expected=%d", m.Version, BSIPrimaryKeyAuthorityManifestVersion)
		return observation
	}
	if len(m.Entries) == 0 {
		observation.Status = BSIPrimaryKeyAuthorityManifestStatusMissing
		observation.Detail = "manifest contains no entries"
		return observation
	}
	if len(tables) == 0 {
		observation.Status = BSIPrimaryKeyAuthorityManifestStatusInvalid
		observation.Detail = "catalog contains no tables"
		return observation
	}

	seen := make(map[string]struct{}, len(m.Entries))
	for _, entry := range m.Entries {
		entryKey := strings.TrimSpace(entry.TableName)
		observation.ManifestEntry = entryKey
		if entryKey == "" {
			observation.Status = BSIPrimaryKeyAuthorityManifestStatusInvalid
			observation.Detail = "manifest entry is missing table"
			return observation
		}
		if _, ok := seen[entryKey]; ok {
			observation.Status = BSIPrimaryKeyAuthorityManifestStatusInvalid
			observation.Detail = fmt.Sprintf("duplicate manifest entry for table %s", entryKey)
			return observation
		}
		seen[entryKey] = struct{}{}

		table, ok := tables[entryKey]
		if !ok {
			observation.Status = BSIPrimaryKeyAuthorityManifestStatusInvalid
			observation.Detail = fmt.Sprintf("table not found: %s", entryKey)
			return observation
		}
		if detail := observeBSIPrimaryKeyAuthorityManifestEntry(entry, table); detail != "" {
			observation.Status = BSIPrimaryKeyAuthorityManifestStatusInvalid
			observation.Detail = detail
			return observation
		}
	}
	return observation
}

func observeBSIPrimaryKeyAuthorityManifestEntry(entry BSIPrimaryKeyAuthorityManifestEntry, table *Table) string {
	expected, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, entry.LogicalShard)
	if err != nil {
		return err.Error()
	}
	if entry.EncodingVersion != PrimaryKeyIdentityEncodingVersion {
		return fmt.Sprintf("table %s primary-key identity encoding version=%d expected=%d",
			entry.TableName, entry.EncodingVersion, PrimaryKeyIdentityEncodingVersion)
	}
	if strings.TrimSpace(entry.PrimaryKey) != expected.PrimaryKey {
		return fmt.Sprintf("table %s primary key=%q expected=%q", entry.TableName, entry.PrimaryKey, expected.PrimaryKey)
	}
	if len(entry.Fields) != len(expected.Fields) {
		return fmt.Sprintf("table %s primary-key field count=%d expected=%d",
			entry.TableName, len(entry.Fields), len(expected.Fields))
	}
	for i := range expected.Fields {
		if detail := observeBSIPrimaryKeyAuthorityManifestField(entry.TableName, i, entry.Fields[i], expected.Fields[i]); detail != "" {
			return detail
		}
	}
	if !entry.Clean {
		return fmt.Sprintf("table %s primary-key authority artifact is not clean", entry.TableName)
	}
	return ""
}

func observeBSIPrimaryKeyAuthorityManifestField(table string, index int, actual, expected BSIPrimaryKeyAuthorityManifestField) string {
	if actual.Name != expected.Name {
		return fmt.Sprintf("table %s primary-key field[%d]=%q expected=%q", table, index, actual.Name, expected.Name)
	}
	if actual.Type != "" && actual.Type != expected.Type {
		return fmt.Sprintf("table %s primary-key field[%d] type=%q expected=%q", table, index, actual.Type, expected.Type)
	}
	if actual.MappingStrategy != "" && actual.MappingStrategy != expected.MappingStrategy {
		return fmt.Sprintf("table %s primary-key field[%d] mapping=%q expected=%q", table, index, actual.MappingStrategy, expected.MappingStrategy)
	}
	if actual.ColumnID != expected.ColumnID {
		return fmt.Sprintf("table %s primary-key field[%d] column_id=%t expected=%t", table, index, actual.ColumnID, expected.ColumnID)
	}
	return ""
}
