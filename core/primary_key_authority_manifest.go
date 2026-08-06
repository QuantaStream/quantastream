package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"gopkg.in/yaml.v2"
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

	// BSIPrimaryKeyAuthorityManifestStatusStale means the manifest is
	// well-formed but no longer matches the active catalog or identity shape.
	BSIPrimaryKeyAuthorityManifestStatusStale = "stale"

	// BSIPrimaryKeyAuthorityManifestStatusInvalid means the manifest must not be
	// trusted for primary-key authority.
	BSIPrimaryKeyAuthorityManifestStatusInvalid = "invalid"

	// BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI identifies the physical
	// BSI authority artifact used to map primary-key identity values to rownums.
	BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI = "primary_key_bsi"

	// BSIPrimaryKeyAuthorityManifestValidationNone means no manifest validation
	// was possible, usually because no manifest exists.
	BSIPrimaryKeyAuthorityManifestValidationNone = "none"

	// BSIPrimaryKeyAuthorityManifestValidationManifestOnly means startup has
	// validated manifest metadata against the catalog but has not validated
	// physical authority artifacts.
	BSIPrimaryKeyAuthorityManifestValidationManifestOnly = "manifest_only"

	// BSIPrimaryKeyAuthorityArtifactTrustNone means no artifact trust decision
	// exists.
	BSIPrimaryKeyAuthorityArtifactTrustNone = "none"

	// BSIPrimaryKeyAuthorityArtifactTrustMetadataOnly means artifact descriptors
	// are metadata only and are not yet trusted as durable authority.
	BSIPrimaryKeyAuthorityArtifactTrustMetadataOnly = "metadata_only"

	// BSIPrimaryKeyAuthorityArtifactPresenceNone means the manifest describes
	// no physical authority artifacts.
	BSIPrimaryKeyAuthorityArtifactPresenceNone = "none"

	// BSIPrimaryKeyAuthorityArtifactPresenceUnchecked means physical artifact
	// presence has not been checked.
	BSIPrimaryKeyAuthorityArtifactPresenceUnchecked = "unchecked"

	// BSIPrimaryKeyAuthorityArtifactPresencePresent means every described
	// artifact path exists.
	BSIPrimaryKeyAuthorityArtifactPresencePresent = "present"

	// BSIPrimaryKeyAuthorityArtifactPresencePartial means some described
	// artifact paths exist and some are missing.
	BSIPrimaryKeyAuthorityArtifactPresencePartial = "partial"

	// BSIPrimaryKeyAuthorityArtifactPresenceMissing means every described
	// artifact path is missing.
	BSIPrimaryKeyAuthorityArtifactPresenceMissing = "missing"
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
	SchemaName         string                                   `yaml:"schema_name,omitempty" json:"schema_name,omitempty"`
	TableName          string                                   `yaml:"table" json:"table"`
	PrimaryKey         string                                   `yaml:"primary_key" json:"primary_key"`
	AuthorityMode      string                                   `yaml:"authority_mode,omitempty" json:"authority_mode,omitempty"`
	AuthorityField     string                                   `yaml:"authority_field,omitempty" json:"authority_field,omitempty"`
	EncodingVersion    int                                      `yaml:"encoding_version" json:"encoding_version"`
	Fields             []BSIPrimaryKeyAuthorityManifestField    `yaml:"fields" json:"fields"`
	LogicalShard       string                                   `yaml:"logical_shard,omitempty" json:"logical_shard,omitempty"`
	ArtifactPath       string                                   `yaml:"artifact_path,omitempty" json:"artifact_path,omitempty"`
	Artifacts          []BSIPrimaryKeyAuthorityManifestArtifact `yaml:"artifacts,omitempty" json:"artifacts,omitempty"`
	Fingerprint        string                                   `yaml:"fingerprint,omitempty" json:"fingerprint,omitempty"`
	CatalogFingerprint string                                   `yaml:"catalog_fingerprint,omitempty" json:"catalog_fingerprint,omitempty"`
	KeyCount           uint64                                   `yaml:"key_count,omitempty" json:"key_count,omitempty"`
	MinColumnID        uint64                                   `yaml:"min_column_id,omitempty" json:"min_column_id,omitempty"`
	MaxColumnID        uint64                                   `yaml:"max_column_id,omitempty" json:"max_column_id,omitempty"`
	Clean              bool                                     `yaml:"clean" json:"clean"`
	CreatedAt          time.Time                                `yaml:"created_at,omitempty" json:"created_at,omitempty"`
	ModifiedAt         time.Time                                `yaml:"modified_at,omitempty" json:"modified_at,omitempty"`
}

// BSIPrimaryKeyAuthorityManifestArtifact describes a physical authority
// artifact that belongs to a logical manifest entry. The legacy ArtifactPath
// field remains for compatibility with the first manifest shape; new writers
// can use Artifacts when an authority entry spans more than one file.
type BSIPrimaryKeyAuthorityManifestArtifact struct {
	Kind        string `yaml:"kind,omitempty" json:"kind,omitempty"`
	Path        string `yaml:"path" json:"path"`
	Fingerprint string `yaml:"fingerprint,omitempty" json:"fingerprint,omitempty"`
	FileCount   uint64 `yaml:"file_count,omitempty" json:"file_count,omitempty"`
	KeyCount    uint64 `yaml:"key_count,omitempty" json:"key_count,omitempty"`
	MinColumnID uint64 `yaml:"min_column_id,omitempty" json:"min_column_id,omitempty"`
	MaxColumnID uint64 `yaml:"max_column_id,omitempty" json:"max_column_id,omitempty"`
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
	Status              string
	Detail              string
	ManifestEntry       string
	ValidationLevel     string
	ArtifactTrust       string
	ArtifactPresence    string
	ArtifactDetail      string
	Entries             int
	ArtifactDescriptors int
	ArtifactPresent     int
	ArtifactMissing     int
	ArtifactFileCount   uint64
	EntryKeyCount       uint64
	ArtifactKeyCount    uint64
	KeyCountMismatches  int
	CleanEntries        int
	DirtyEntries        int
}

// BSIPrimaryKeyAuthorityManifestPath returns the conventional persisted
// manifest path under a storage data directory.
func BSIPrimaryKeyAuthorityManifestPath(dataDir string) string {
	return filepath.Join(dataDir, BSIPrimaryKeyAuthorityManifestFileName)
}

// SaveBSIPrimaryKeyAuthorityManifest writes a manifest atomically using the
// current YAML metadata format.
func SaveBSIPrimaryKeyAuthorityManifest(dataDir string, manifest BSIPrimaryKeyAuthorityManifest) error {
	if strings.TrimSpace(dataDir) == "" {
		return fmt.Errorf("primary-key authority manifest requires data directory")
	}
	if manifest.Version == 0 {
		manifest.Version = BSIPrimaryKeyAuthorityManifestVersion
	}
	if manifest.GeneratedAt.IsZero() {
		manifest.GeneratedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("create primary-key authority manifest directory: %w", err)
	}
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal primary-key authority manifest: %w", err)
	}
	path := BSIPrimaryKeyAuthorityManifestPath(dataDir)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write primary-key authority manifest: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace primary-key authority manifest: %w", err)
	}
	return nil
}

// LoadBSIPrimaryKeyAuthorityManifest reads the current YAML metadata format. A
// missing manifest is returned as a zero-value manifest so callers can decide
// whether to rebuild, warn, or fail closed.
func LoadBSIPrimaryKeyAuthorityManifest(dataDir string) (BSIPrimaryKeyAuthorityManifest, error) {
	var manifest BSIPrimaryKeyAuthorityManifest
	if strings.TrimSpace(dataDir) == "" {
		return manifest, fmt.Errorf("primary-key authority manifest requires data directory")
	}
	data, err := os.ReadFile(BSIPrimaryKeyAuthorityManifestPath(dataDir))
	if os.IsNotExist(err) {
		return manifest, nil
	}
	if err != nil {
		return manifest, fmt.Errorf("read primary-key authority manifest: %w", err)
	}
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return manifest, fmt.Errorf("parse primary-key authority manifest: %w", err)
	}
	return manifest, nil
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

	eligibility := ObserveBSIPrimaryKeyAuthorityEligibility(table)
	return BSIPrimaryKeyAuthorityManifestEntry{
		TableName:       table.Name,
		PrimaryKey:      strings.TrimSpace(table.PrimaryKey),
		AuthorityMode:   eligibility.Mode,
		AuthorityField:  bsiPrimaryKeyAuthorityManifestAuthorityField(eligibility),
		EncodingVersion: PrimaryKeyIdentityEncodingVersion,
		Fields:          fields,
		LogicalShard:    logicalShard,
		Clean:           true,
	}, nil
}

// ObserveAgainstCatalog validates the manifest against the current table cache.
func (m BSIPrimaryKeyAuthorityManifest) ObserveAgainstCatalog(tables map[string]*Table) BSIPrimaryKeyAuthorityManifestObservation {
	artifactDescriptors, entryKeyCount, artifactKeyCount, keyCountMismatches, cleanEntries, dirtyEntries := summarizeBSIPrimaryKeyAuthorityManifestEntries(m.Entries)
	observation := BSIPrimaryKeyAuthorityManifestObservation{
		Status:              BSIPrimaryKeyAuthorityManifestStatusOK,
		ValidationLevel:     BSIPrimaryKeyAuthorityManifestValidationManifestOnly,
		ArtifactTrust:       BSIPrimaryKeyAuthorityArtifactTrustMetadataOnly,
		ArtifactPresence:    BSIPrimaryKeyAuthorityArtifactPresenceNone,
		Entries:             len(m.Entries),
		ArtifactDescriptors: artifactDescriptors,
		EntryKeyCount:       entryKeyCount,
		ArtifactKeyCount:    artifactKeyCount,
		KeyCountMismatches:  keyCountMismatches,
		CleanEntries:        cleanEntries,
		DirtyEntries:        dirtyEntries,
	}
	if artifactDescriptors > 0 {
		observation.ArtifactPresence = BSIPrimaryKeyAuthorityArtifactPresenceUnchecked
	}
	if m.Version == 0 && len(m.Entries) == 0 {
		observation.Status = BSIPrimaryKeyAuthorityManifestStatusMissing
		observation.ValidationLevel = BSIPrimaryKeyAuthorityManifestValidationNone
		observation.ArtifactTrust = BSIPrimaryKeyAuthorityArtifactTrustNone
		observation.ArtifactPresence = BSIPrimaryKeyAuthorityArtifactPresenceNone
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
			observation.Status = BSIPrimaryKeyAuthorityManifestStatusStale
			observation.Detail = fmt.Sprintf("table not found: %s", entryKey)
			return observation
		}
		if status, detail := observeBSIPrimaryKeyAuthorityManifestEntry(entry, table); detail != "" {
			observation.Status = status
			observation.Detail = detail
			return observation
		}
	}
	return observation
}

func summarizeBSIPrimaryKeyAuthorityManifestEntries(entries []BSIPrimaryKeyAuthorityManifestEntry) (int, uint64, uint64, int, int, int) {
	artifactDescriptors := 0
	var entryKeyCount uint64
	var artifactKeyCount uint64
	keyCountMismatches := 0
	cleanEntries := 0
	dirtyEntries := 0
	for _, entry := range entries {
		artifactDescriptors += len(entry.Artifacts)
		entryKeyCount += entry.KeyCount
		var entryArtifactKeyCount uint64
		entryHasArtifactKeyCount := false
		for _, artifact := range entry.Artifacts {
			if artifact.KeyCount != 0 {
				entryHasArtifactKeyCount = true
				entryArtifactKeyCount += artifact.KeyCount
			}
		}
		artifactKeyCount += entryArtifactKeyCount
		if entry.KeyCount != 0 && entryHasArtifactKeyCount && entry.KeyCount != entryArtifactKeyCount {
			keyCountMismatches++
		}
		if entry.Clean {
			cleanEntries++
		} else {
			dirtyEntries++
		}
	}
	return artifactDescriptors, entryKeyCount, artifactKeyCount, keyCountMismatches, cleanEntries, dirtyEntries
}

func observeBSIPrimaryKeyAuthorityManifestEntry(entry BSIPrimaryKeyAuthorityManifestEntry, table *Table) (string, string) {
	expected, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, entry.LogicalShard)
	if err != nil {
		return BSIPrimaryKeyAuthorityManifestStatusInvalid, err.Error()
	}
	if entry.EncodingVersion != PrimaryKeyIdentityEncodingVersion {
		return BSIPrimaryKeyAuthorityManifestStatusStale, fmt.Sprintf("table %s primary-key identity encoding version=%d expected=%d",
			entry.TableName, entry.EncodingVersion, PrimaryKeyIdentityEncodingVersion)
	}
	if strings.TrimSpace(entry.PrimaryKey) != expected.PrimaryKey {
		return BSIPrimaryKeyAuthorityManifestStatusStale, fmt.Sprintf("table %s primary key=%q expected=%q", entry.TableName, entry.PrimaryKey, expected.PrimaryKey)
	}
	if strings.TrimSpace(entry.AuthorityMode) != "" && entry.AuthorityMode != expected.AuthorityMode {
		return BSIPrimaryKeyAuthorityManifestStatusStale, fmt.Sprintf("table %s primary-key authority mode=%q expected=%q",
			entry.TableName, entry.AuthorityMode, expected.AuthorityMode)
	}
	if strings.TrimSpace(entry.AuthorityField) != "" && entry.AuthorityField != expected.AuthorityField {
		return BSIPrimaryKeyAuthorityManifestStatusStale, fmt.Sprintf("table %s primary-key authority field=%q expected=%q",
			entry.TableName, entry.AuthorityField, expected.AuthorityField)
	}
	if len(entry.Fields) != len(expected.Fields) {
		return BSIPrimaryKeyAuthorityManifestStatusStale, fmt.Sprintf("table %s primary-key field count=%d expected=%d",
			entry.TableName, len(entry.Fields), len(expected.Fields))
	}
	for i := range expected.Fields {
		if detail := observeBSIPrimaryKeyAuthorityManifestField(entry.TableName, i, entry.Fields[i], expected.Fields[i]); detail != "" {
			return BSIPrimaryKeyAuthorityManifestStatusStale, detail
		}
	}
	if detail := observeBSIPrimaryKeyAuthorityManifestArtifactMetadata(entry); detail != "" {
		return BSIPrimaryKeyAuthorityManifestStatusInvalid, detail
	}
	if !entry.Clean {
		return BSIPrimaryKeyAuthorityManifestStatusInvalid, fmt.Sprintf("table %s primary-key authority artifact is not clean", entry.TableName)
	}
	return "", ""
}

func bsiPrimaryKeyAuthorityManifestAuthorityField(eligibility BSIPrimaryKeyAuthorityEligibility) string {
	switch eligibility.Mode {
	case BSIPrimaryKeyAuthorityModeDirectColumnID, BSIPrimaryKeyAuthorityModeSingleColumnBSI:
		return eligibility.FieldName
	case BSIPrimaryKeyAuthorityModeCompoundEncodedBSI:
		return shared.CompoundPrimaryKeyAuthorityFieldName
	default:
		return ""
	}
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

func observeBSIPrimaryKeyAuthorityManifestArtifactMetadata(entry BSIPrimaryKeyAuthorityManifestEntry) string {
	if entry.MinColumnID != 0 && entry.MaxColumnID != 0 && entry.MinColumnID > entry.MaxColumnID {
		return fmt.Sprintf("table %s primary-key authority column bounds min=%d max=%d", entry.TableName, entry.MinColumnID, entry.MaxColumnID)
	}
	for i, artifact := range entry.Artifacts {
		if strings.TrimSpace(artifact.Path) == "" {
			return fmt.Sprintf("table %s primary-key authority artifact[%d] is missing path", entry.TableName, i)
		}
		if !isSupportedBSIPrimaryKeyAuthorityArtifactKind(artifact.Kind) {
			return fmt.Sprintf("table %s primary-key authority artifact[%d] has unsupported kind %q",
				entry.TableName, i, artifact.Kind)
		}
		if artifact.MinColumnID != 0 && artifact.MaxColumnID != 0 && artifact.MinColumnID > artifact.MaxColumnID {
			return fmt.Sprintf("table %s primary-key authority artifact[%d] column bounds min=%d max=%d",
				entry.TableName, i, artifact.MinColumnID, artifact.MaxColumnID)
		}
	}
	return ""
}

func isSupportedBSIPrimaryKeyAuthorityArtifactKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "", BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI, "bsi":
		return true
	default:
		return false
	}
}
