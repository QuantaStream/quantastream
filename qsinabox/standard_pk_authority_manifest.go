package qsinabox

import (
	"fmt"
	"os"
	"path"
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
	return observeStandardBSIPrimaryKeyAuthorityArtifacts(config, manifest, manifest.ObserveAgainstCatalog(tables))
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
		standardPopulateBSIPrimaryKeyAuthorityArtifacts(&entry)
		manifest.Entries = append(manifest.Entries, entry)
	}
	return manifest, nil
}

func standardPopulateBSIPrimaryKeyAuthorityArtifacts(entry *core.BSIPrimaryKeyAuthorityManifestEntry) {
	if entry == nil || strings.TrimSpace(entry.AuthorityField) == "" {
		return
	}
	switch entry.AuthorityMode {
	case core.BSIPrimaryKeyAuthorityModeSingleColumnBSI, core.BSIPrimaryKeyAuthorityModeCompoundEncodedBSI:
	default:
		return
	}
	entry.Artifacts = []core.BSIPrimaryKeyAuthorityManifestArtifact{
		{
			Kind: core.BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI,
			Path: standardBSIPrimaryKeyAuthorityArtifactPath(entry.TableName, entry.AuthorityField, entry.LogicalShard),
		},
	}
}

func standardBSIPrimaryKeyAuthorityArtifactPath(tableName, authorityField, logicalShard string) string {
	shard := strings.TrimSpace(logicalShard)
	if shard == "" {
		shard = "default"
	}
	return path.Join("bitmap", strings.TrimSpace(tableName), "_bsi_pack", shard, "bundle")
}

func observeStandardBSIPrimaryKeyAuthorityArtifacts(config StandardConfig, manifest core.BSIPrimaryKeyAuthorityManifest, observation core.BSIPrimaryKeyAuthorityManifestObservation) core.BSIPrimaryKeyAuthorityManifestObservation {
	if observation.Status != core.BSIPrimaryKeyAuthorityManifestStatusOK || observation.ArtifactDescriptors == 0 {
		return observation
	}
	present := 0
	missing := 0
	var fileCount uint64
	var firstMissing string
	var firstError string
	for _, entry := range manifest.Entries {
		for _, artifact := range entry.Artifacts {
			count, exists, err := standardBSIPrimaryKeyAuthorityArtifactFileCount(config, artifact.Path)
			if err != nil {
				missing++
				if firstError == "" {
					firstError = fmt.Sprintf("%s: %v", artifact.Path, err)
				}
				continue
			}
			if !exists {
				missing++
				if firstMissing == "" {
					firstMissing = artifact.Path
				}
				continue
			}
			present++
			fileCount += count
		}
	}
	observation.ArtifactPresent = present
	observation.ArtifactMissing = missing
	observation.ArtifactFileCount = fileCount
	switch {
	case present > 0 && missing == 0:
		observation.ArtifactPresence = core.BSIPrimaryKeyAuthorityArtifactPresencePresent
	case present > 0 && missing > 0:
		observation.ArtifactPresence = core.BSIPrimaryKeyAuthorityArtifactPresencePartial
	case missing > 0:
		observation.ArtifactPresence = core.BSIPrimaryKeyAuthorityArtifactPresenceMissing
	default:
		observation.ArtifactPresence = core.BSIPrimaryKeyAuthorityArtifactPresenceNone
	}
	if firstError != "" {
		observation.ArtifactDetail = "artifact path check failed: " + firstError
	} else if firstMissing != "" {
		observation.ArtifactDetail = "artifact path missing: " + firstMissing
	}
	return observation
}

func standardPopulateBSIPrimaryKeyAuthorityArtifactFileCounts(config StandardConfig, manifest *core.BSIPrimaryKeyAuthorityManifest) error {
	if manifest == nil {
		return nil
	}
	for entryIndex := range manifest.Entries {
		for artifactIndex := range manifest.Entries[entryIndex].Artifacts {
			count, exists, err := standardBSIPrimaryKeyAuthorityArtifactFileCount(config, manifest.Entries[entryIndex].Artifacts[artifactIndex].Path)
			if err != nil {
				return err
			}
			if !exists {
				count = 0
			}
			manifest.Entries[entryIndex].Artifacts[artifactIndex].FileCount = count
		}
	}
	return nil
}

// RefreshStandardBSIPrimaryKeyAuthorityManifestArtifacts ensures the
// standard-mode authority manifest exists, then recomputes physical artifact
// metadata after the local bitmap storage layer has persisted dirty BSI shards.
func RefreshStandardBSIPrimaryKeyAuthorityManifestArtifacts(config StandardConfig, source string) (bool, error) {
	config = config.WithDefaults()
	manifest, err := core.LoadBSIPrimaryKeyAuthorityManifest(config.DataDir)
	if err != nil {
		return false, err
	}
	if manifest.Version == 0 && len(manifest.Entries) == 0 {
		manifest, err = BuildStandardBSIPrimaryKeyAuthorityManifest(config, source)
		if err != nil {
			return false, err
		}
		if len(manifest.Entries) == 0 {
			return false, nil
		}
	}
	if err := standardPopulateBSIPrimaryKeyAuthorityArtifactFileCounts(config, &manifest); err != nil {
		return false, err
	}
	if trimmed := strings.TrimSpace(source); trimmed != "" {
		manifest.Source = trimmed
	}
	if err := SaveStandardBSIPrimaryKeyAuthorityManifest(config, manifest); err != nil {
		return false, err
	}
	return true, nil
}

func standardBSIPrimaryKeyAuthorityArtifactFileCount(config StandardConfig, artifactPath string) (uint64, bool, error) {
	resolved, err := standardBSIPrimaryKeyAuthorityArtifactPhysicalPath(config, artifactPath)
	if err != nil {
		return 0, false, err
	}
	info, err := os.Stat(resolved)
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !info.IsDir() {
		return 1, true, nil
	}
	var count uint64
	err = filepath.WalkDir(resolved, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			count++
		}
		return nil
	})
	return count, true, err
}

func standardBSIPrimaryKeyAuthorityArtifactPhysicalPath(config StandardConfig, artifactPath string) (string, error) {
	config = config.WithDefaults()
	raw := strings.TrimSpace(artifactPath)
	if raw == "" {
		return "", fmt.Errorf("artifact path is empty")
	}
	if filepath.IsAbs(raw) {
		return "", fmt.Errorf("artifact path must be relative: %s", raw)
	}
	base := filepath.Clean(config.DataDir)
	resolved := filepath.Join(base, filepath.Clean(filepath.FromSlash(raw)))
	rel, err := filepath.Rel(base, resolved)
	if err != nil {
		return "", err
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("artifact path escapes data directory: %s", raw)
	}
	return resolved, nil
}

// SaveStandardBSIPrimaryKeyAuthorityManifest writes a logical authority
// manifest to the standard-mode data directory.
func SaveStandardBSIPrimaryKeyAuthorityManifest(config StandardConfig, manifest core.BSIPrimaryKeyAuthorityManifest) error {
	config = config.WithDefaults()
	return core.SaveBSIPrimaryKeyAuthorityManifest(config.DataDir, manifest)
}

// StandardBSIPrimaryKeyAuthorityManifestPath returns the standard-mode manifest
// path for diagnostics and command output.
func StandardBSIPrimaryKeyAuthorityManifestPath(config StandardConfig) string {
	config = config.WithDefaults()
	return core.BSIPrimaryKeyAuthorityManifestPath(config.DataDir)
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
