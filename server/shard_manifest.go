package server

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	// BitmapShardManifestFileName is the persisted bitmap startup index.
	BitmapShardManifestFileName = "BITMAP_SHARD_MANIFEST"
	bitmapShardManifestVersion  = 1
	bitmapShardKindStandard     = "standard"
	bitmapShardKindBSI          = "bsi"
	bitmapShardFileRoleBundle   = "bundle"
)

// BitmapShardManifest records logical bitmap artifacts discovered on disk.
//
// The manifest is an acceleration structure. Catalog metadata and persisted
// bitmap/KV files remain authoritative; a missing or stale manifest must be
// rebuilt from disk rather than trusted blindly.
type BitmapShardManifest struct {
	Version     int                        `yaml:"version"`
	GeneratedAt time.Time                  `yaml:"generated_at"`
	Source      string                     `yaml:"source"`
	Stats       BitmapShardManifestStats   `yaml:"stats"`
	Entries     []BitmapShardManifestEntry `yaml:"entries"`
}

// BitmapShardManifestStats summarizes the manifest for startup diagnostics.
type BitmapShardManifestStats struct {
	TotalEntries    int `yaml:"total_entries"`
	StandardEntries int `yaml:"standard_entries"`
	BSIEntries      int `yaml:"bsi_entries"`
	TotalFiles      int `yaml:"total_files"`
	StandardFiles   int `yaml:"standard_files"`
	BSIFiles        int `yaml:"bsi_files"`
}

// BitmapShardManifestEntry describes one logical standard bitmap or BSI shard.
type BitmapShardManifestEntry struct {
	Table            string                    `yaml:"table"`
	Field            string                    `yaml:"field"`
	Kind             string                    `yaml:"kind"`
	RowIDOrBits      int64                     `yaml:"row_id_or_bits"`
	Shard            string                    `yaml:"shard"`
	ShardTime        time.Time                 `yaml:"shard_time"`
	BaseRelativePath string                    `yaml:"base_relative_path,omitempty"`
	FileCount        int                       `yaml:"file_count,omitempty"`
	MaxBitSlice      int                       `yaml:"max_bit_slice,omitempty"`
	HasExistence     bool                      `yaml:"has_existence,omitempty"`
	ModTime          time.Time                 `yaml:"mod_time,omitempty"`
	Files            []BitmapShardManifestFile `yaml:"files,omitempty"`
}

// BitmapShardManifestFile describes one physical file backing a logical shard.
type BitmapShardManifestFile struct {
	RelativePath string    `yaml:"relative_path"`
	Role         string    `yaml:"role"`
	BitSlice     int       `yaml:"bit_slice,omitempty"`
	SizeBytes    int64     `yaml:"size_bytes"`
	ModTime      time.Time `yaml:"mod_time"`
}

// BitmapShardManifestObservation reports whether the persisted manifest looks usable.
type BitmapShardManifestObservation struct {
	Status           string
	Detail           string
	ManifestEntries  int
	ManifestFiles    int
	ScanEntries      int
	ScanFiles        int
	MissingFileCount int
	Elapsed          time.Duration
}

func useBitmapShardManifestEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QUANTASTREAM_USE_SHARD_MANIFEST"))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

type bitmapShardManifestBuilder struct {
	dataDir string
	entries map[string]*BitmapShardManifestEntry
}

func newBitmapShardManifestBuilder(dataDir string) *bitmapShardManifestBuilder {
	return &bitmapShardManifestBuilder{
		dataDir: dataDir,
		entries: make(map[string]*BitmapShardManifestEntry),
	}
}

func (b *bitmapShardManifestBuilder) addStandardFile(path string, info os.FileInfo, table, field string, rowID int64, shardTime time.Time) {
	entry := b.entry(table, field, bitmapShardKindStandard, rowID, shardTime)
	entry.Files = append(entry.Files, BitmapShardManifestFile{
		RelativePath: b.relativePath(path),
		Role:         "bitmap",
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime().UTC(),
	})
}

func (b *bitmapShardManifestBuilder) addStandardBundleFile(path string, info os.FileInfo, table, field string, shardTime time.Time) {
	entry := b.entry(table, field, bitmapShardKindStandard, -1, shardTime)
	entry.Files = []BitmapShardManifestFile{{
		RelativePath: b.relativePath(path),
		Role:         bitmapShardFileRoleBundle,
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime().UTC(),
	}}
	entry.FileCount = 1
	entry.ModTime = info.ModTime().UTC()
	entry.BaseRelativePath = ""
}

func (b *bitmapShardManifestBuilder) addBSIFile(path string, info os.FileInfo, table, field string, shardTime time.Time, bitSliceIndex int) {
	entry := b.entry(table, field, bitmapShardKindBSI, -1, shardTime)
	entry.BaseRelativePath = b.relativePath(filepath.Dir(path))
	entry.FileCount++
	if bitSliceIndex > entry.MaxBitSlice {
		entry.MaxBitSlice = bitSliceIndex
	}
	if bitSliceIndex == 0 {
		entry.HasExistence = true
	}
	if entry.ModTime.IsZero() || info.ModTime().After(entry.ModTime) {
		entry.ModTime = info.ModTime().UTC()
	}
}

func (b *bitmapShardManifestBuilder) addBSIBundleFile(path string, info os.FileInfo, table, field string, shardTime time.Time) {
	entry := b.entry(table, field, bitmapShardKindBSI, -1, shardTime)
	entry.Files = []BitmapShardManifestFile{{
		RelativePath: b.relativePath(path),
		Role:         bitmapShardFileRoleBundle,
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime().UTC(),
	}}
	entry.FileCount = 1
	entry.MaxBitSlice = 0
	entry.HasExistence = true
	entry.ModTime = info.ModTime().UTC()
	entry.BaseRelativePath = ""
}

func (b *bitmapShardManifestBuilder) addStandardCacheEntry(path string, table, field string, rowID int64, shardTime time.Time, modTime time.Time) {
	entry := b.entry(table, field, bitmapShardKindStandard, rowID, shardTime)
	entry.Files = []BitmapShardManifestFile{{
		RelativePath: b.relativePath(path),
		Role:         "bitmap",
		ModTime:      modTime.UTC(),
	}}
	if info, err := os.Stat(path); err == nil {
		entry.Files[0].SizeBytes = info.Size()
		entry.Files[0].ModTime = info.ModTime().UTC()
	}
}

func (b *bitmapShardManifestBuilder) addStandardBundleCacheEntry(path string, table, field string, shardTime time.Time, modTime time.Time) {
	entry := b.entry(table, field, bitmapShardKindStandard, -1, shardTime)
	entry.Files = []BitmapShardManifestFile{{
		RelativePath: b.relativePath(path),
		Role:         bitmapShardFileRoleBundle,
		ModTime:      modTime.UTC(),
	}}
	if info, err := os.Stat(path); err == nil {
		entry.Files[0].SizeBytes = info.Size()
		entry.Files[0].ModTime = info.ModTime().UTC()
	}
	entry.FileCount = 1
	entry.ModTime = modTime.UTC()
	entry.BaseRelativePath = ""
}

func (b *bitmapShardManifestBuilder) addBSICacheEntry(basePath string, table, field string, shardTime time.Time, _ int, modTime time.Time) {
	entry := b.entry(table, field, bitmapShardKindBSI, -1, shardTime)
	entry.Files = []BitmapShardManifestFile{{
		RelativePath: b.relativePath(filepath.Join(basePath, bsiBundleFileName)),
		Role:         bitmapShardFileRoleBundle,
		ModTime:      modTime.UTC(),
	}}
	if info, err := os.Stat(filepath.Join(basePath, bsiBundleFileName)); err == nil {
		entry.Files[0].SizeBytes = info.Size()
		entry.Files[0].ModTime = info.ModTime().UTC()
	}
	entry.FileCount = 1
	entry.MaxBitSlice = 0
	entry.HasExistence = true
	entry.ModTime = modTime.UTC()
	entry.BaseRelativePath = ""
}

func (b *bitmapShardManifestBuilder) manifest(generatedAt time.Time, source string) BitmapShardManifest {
	if generatedAt.IsZero() {
		generatedAt = time.Now()
	}
	manifest := BitmapShardManifest{
		Version:     bitmapShardManifestVersion,
		GeneratedAt: generatedAt.UTC(),
		Source:      source,
		Entries:     make([]BitmapShardManifestEntry, 0, len(b.entries)),
	}
	for _, entry := range b.entries {
		if entry.Kind != bitmapShardKindBSI {
			sort.SliceStable(entry.Files, func(i, j int) bool {
				if entry.Files[i].BitSlice != entry.Files[j].BitSlice {
					return entry.Files[i].BitSlice < entry.Files[j].BitSlice
				}
				return entry.Files[i].RelativePath < entry.Files[j].RelativePath
			})
		}
		manifest.Entries = append(manifest.Entries, *entry)
	}
	sort.SliceStable(manifest.Entries, func(i, j int) bool {
		left := manifest.Entries[i]
		right := manifest.Entries[j]
		if left.Table != right.Table {
			return left.Table < right.Table
		}
		if left.Field != right.Field {
			return left.Field < right.Field
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.RowIDOrBits != right.RowIDOrBits {
			return left.RowIDOrBits < right.RowIDOrBits
		}
		return left.Shard < right.Shard
	})
	for _, entry := range manifest.Entries {
		manifest.Stats.TotalEntries++
		entryFileCount := entry.manifestFileCount()
		manifest.Stats.TotalFiles += entryFileCount
		switch entry.Kind {
		case bitmapShardKindBSI:
			manifest.Stats.BSIEntries++
			manifest.Stats.BSIFiles += entryFileCount
		default:
			manifest.Stats.StandardEntries++
			manifest.Stats.StandardFiles += entryFileCount
		}
	}
	return manifest
}

func (e BitmapShardManifestEntry) manifestFileCount() int {
	if e.Kind == bitmapShardKindBSI && e.FileCount > 0 {
		return e.FileCount
	}
	return len(e.Files)
}

func (b *bitmapShardManifestBuilder) entry(table, field, kind string, rowIDOrBits int64, shardTime time.Time) *BitmapShardManifestEntry {
	shard := formatShardTime(shardTime)
	key := fmt.Sprintf("%s/%s/%s/%d/%s", table, field, kind, rowIDOrBits, shard)
	if entry, ok := b.entries[key]; ok {
		return entry
	}
	entry := &BitmapShardManifestEntry{
		Table:       table,
		Field:       field,
		Kind:        kind,
		RowIDOrBits: rowIDOrBits,
		Shard:       shard,
		ShardTime:   shardTime.UTC(),
	}
	b.entries[key] = entry
	return entry
}

func (b *bitmapShardManifestBuilder) relativePath(path string) string {
	rel, err := filepath.Rel(b.dataDir, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}

func (m *BitmapIndex) bitmapShardManifestPath() string {
	return filepath.Join(m.dataDir, BitmapShardManifestFileName)
}

func (m *BitmapIndex) saveBitmapShardManifest(manifest BitmapShardManifest) error {
	if manifest.Version == 0 {
		manifest.Version = bitmapShardManifestVersion
	}
	if manifest.GeneratedAt.IsZero() {
		manifest.GeneratedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(m.dataDir, 0755); err != nil {
		return fmt.Errorf("create bitmap manifest directory: %w", err)
	}
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal bitmap shard manifest: %w", err)
	}
	path := m.bitmapShardManifestPath()
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write bitmap shard manifest: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace bitmap shard manifest: %w", err)
	}
	return nil
}

func (m *BitmapIndex) saveBitmapShardManifestFromCache(source string) error {
	if m.persistenceDisabled() {
		return nil
	}
	start := time.Now()
	builder := newBitmapShardManifestBuilder(m.dataDir)
	standardEntries := 0
	bsiEntries := 0

	m.bitmapCacheLock.RLock()
	type standardManifestGroup struct {
		indexName string
		fieldName string
		shardNano int64
		tqType    string
		modTime   time.Time
	}
	standardGroups := make(map[string]standardManifestGroup)
	for indexName, index := range m.bitmapCache {
		for fieldName, field := range index {
			for rowID, ts := range field {
				_ = rowID
				for shardNano, bitmap := range ts {
					if bitmap == nil {
						continue
					}
					bitmap.Lock.RLock()
					modTime := bitmap.PersistTime
					if modTime.IsZero() {
						modTime = bitmap.ModTime
					}
					tqType := bitmap.TQType
					bitmap.Lock.RUnlock()
					groupShardNano := shardNano
					if tqType == "" {
						groupShardNano = 0
					}
					key := fmt.Sprintf("%s/%s/%d", indexName, fieldName, groupShardNano)
					group := standardGroups[key]
					if group.indexName == "" {
						group = standardManifestGroup{
							indexName: indexName,
							fieldName: fieldName,
							shardNano: groupShardNano,
							tqType:    tqType,
							modTime:   modTime,
						}
					}
					if modTime.After(group.modTime) {
						group.modTime = modTime
					}
					standardGroups[key] = group
				}
			}
		}
	}
	for _, group := range standardGroups {
		shardTime := time.Unix(0, group.shardNano)
		_, path := m.standardBitmapBundleFilePath(group.indexName, group.fieldName, shardTime, group.tqType)
		builder.addStandardBundleCacheEntry(path, group.indexName, group.fieldName, shardTime, group.modTime)
		standardEntries++
	}
	m.bitmapCacheLock.RUnlock()

	m.bsiCacheLock.RLock()
	for indexName, index := range m.bsiCache {
		for fieldName, field := range index {
			for shardNano, bsi := range field {
				if bsi == nil || bsi.BSI == nil {
					continue
				}
				bsi.Lock.RLock()
				shardTime := time.Unix(0, shardNano)
				modTime := bsi.PersistTime
				if modTime.IsZero() {
					modTime = bsi.ModTime
				}
				maxBitSlice := int(bsi.BitCount())
				tqType := bsi.TQType
				bsi.Lock.RUnlock()
				partition := &Partition{Index: indexName, Field: fieldName, RowIDOrBits: -1, Time: shardTime, TQType: tqType}
				builder.addBSICacheEntry(m.generateBitmapFilePath(partition, false), indexName, fieldName, shardTime, maxBitSlice, modTime)
				bsiEntries++
			}
		}
	}
	m.bsiCacheLock.RUnlock()

	manifest := builder.manifest(time.Now().UTC(), source)
	if len(manifest.Entries) == 0 {
		if err := m.invalidateBitmapShardManifest("empty cache manifest refresh"); err != nil {
			return err
		}
		return nil
	}
	if err := m.saveBitmapShardManifest(manifest); err != nil {
		return fmt.Errorf("save bitmap shard manifest from cache: %w", err)
	}
	fmt.Printf("BitmapIndex refreshed shard manifest source=%s entries=%d files=%d standard_entries=%d bsi_entries=%d elapsed=%v\n",
		source, manifest.Stats.TotalEntries, manifest.Stats.TotalFiles, standardEntries, bsiEntries, time.Since(start))
	return nil
}

func (m *BitmapIndex) invalidateBitmapShardManifest(reason string) error {
	path := m.bitmapShardManifestPath()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove bitmap shard manifest after %s: %w", reason, err)
	}
	tmpPath := path + ".tmp"
	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove temporary bitmap shard manifest after %s: %w", reason, err)
	}
	return nil
}

func (m *BitmapIndex) loadBitmapShardManifest() (BitmapShardManifest, error) {
	var manifest BitmapShardManifest
	data, err := os.ReadFile(m.bitmapShardManifestPath())
	if os.IsNotExist(err) {
		return manifest, nil
	}
	if err != nil {
		return manifest, fmt.Errorf("read bitmap shard manifest: %w", err)
	}
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return manifest, fmt.Errorf("parse bitmap shard manifest: %w", err)
	}
	return manifest, nil
}

func (m *BitmapIndex) observeBitmapShardManifest(scan BitmapShardManifest) BitmapShardManifestObservation {
	_, observation := m.loadAndObserveBitmapShardManifest(&scan)
	return observation
}

func (m *BitmapIndex) loadAndObserveBitmapShardManifest(scan *BitmapShardManifest) (BitmapShardManifest, BitmapShardManifestObservation) {
	start := time.Now()
	observation := BitmapShardManifestObservation{
		Status: "missing",
	}
	if scan != nil {
		observation.ScanEntries = scan.Stats.TotalEntries
		observation.ScanFiles = scan.Stats.TotalFiles
	}
	manifest, err := m.loadBitmapShardManifest()
	if err != nil {
		observation.Status = "invalid"
		observation.Detail = err.Error()
		observation.Elapsed = time.Since(start)
		return manifest, observation
	}
	if manifest.Version == 0 && len(manifest.Entries) == 0 {
		observation.Elapsed = time.Since(start)
		return manifest, observation
	}
	observation.ManifestEntries = manifest.Stats.TotalEntries
	observation.ManifestFiles = manifest.Stats.TotalFiles

	if manifest.Version != bitmapShardManifestVersion {
		observation.Status = "invalid"
		observation.Detail = fmt.Sprintf("version=%d expected=%d", manifest.Version, bitmapShardManifestVersion)
		observation.Elapsed = time.Since(start)
		return manifest, observation
	}
	if len(manifest.Entries) == 0 {
		observation.Status = "invalid"
		observation.Detail = "empty_entries"
		observation.Elapsed = time.Since(start)
		return manifest, observation
	}
	if manifest.Stats.TotalEntries != len(manifest.Entries) {
		observation.Status = "invalid"
		observation.Detail = fmt.Sprintf("entry_stats=%d actual_entries=%d", manifest.Stats.TotalEntries, len(manifest.Entries))
		observation.Elapsed = time.Since(start)
		return manifest, observation
	}
	actualFiles := 0
	for _, entry := range manifest.Entries {
		if entry.Table == "" || entry.Field == "" || entry.Kind == "" || entry.Shard == "" {
			observation.Status = "invalid"
			observation.Detail = "entry_missing_required_field"
			observation.Elapsed = time.Since(start)
			return manifest, observation
		}
		if entry.Kind != bitmapShardKindStandard && entry.Kind != bitmapShardKindBSI {
			observation.Status = "invalid"
			observation.Detail = fmt.Sprintf("unknown_kind=%s", entry.Kind)
			observation.Elapsed = time.Since(start)
			return manifest, observation
		}
		if entry.Kind == bitmapShardKindBSI && entry.BaseRelativePath != "" {
			actualFiles += entry.manifestFileCount()
			if entry.FileCount <= 0 || entry.MaxBitSlice < 0 || !entry.HasExistence {
				observation.Status = "invalid"
				observation.Detail = "bsi_compact_entry_missing_required_field"
				observation.Elapsed = time.Since(start)
				return manifest, observation
			}
			if _, err := os.Stat(filepath.Join(m.dataDir, filepath.FromSlash(entry.BaseRelativePath))); err != nil {
				observation.MissingFileCount++
			}
			if _, err := os.Stat(filepath.Join(m.dataDir, filepath.FromSlash(entry.BaseRelativePath), "EBM")); err != nil {
				observation.MissingFileCount++
			}
			continue
		}
		for _, file := range entry.Files {
			actualFiles++
			if file.RelativePath == "" {
				observation.Status = "invalid"
				observation.Detail = "file_missing_relative_path"
				observation.Elapsed = time.Since(start)
				return manifest, observation
			}
			if _, err := os.Stat(filepath.Join(m.dataDir, filepath.FromSlash(file.RelativePath))); err != nil {
				observation.MissingFileCount++
			}
		}
	}
	if manifest.Stats.TotalFiles != actualFiles {
		observation.Status = "invalid"
		observation.Detail = fmt.Sprintf("file_stats=%d actual_files=%d", manifest.Stats.TotalFiles, actualFiles)
		observation.Elapsed = time.Since(start)
		return manifest, observation
	}
	if observation.MissingFileCount > 0 {
		observation.Status = "stale"
		observation.Detail = fmt.Sprintf("missing_files=%d", observation.MissingFileCount)
		observation.Elapsed = time.Since(start)
		return manifest, observation
	}
	if scan != nil && !manifest.Stats.equal(scan.Stats) {
		observation.Status = "mismatch"
		observation.Detail = "stats_differ_from_slow_scan"
		observation.Elapsed = time.Since(start)
		return manifest, observation
	}
	observation.Status = "ok"
	observation.Elapsed = time.Since(start)
	return manifest, observation
}

func (s BitmapShardManifestStats) equal(other BitmapShardManifestStats) bool {
	return s.TotalEntries == other.TotalEntries &&
		s.StandardEntries == other.StandardEntries &&
		s.BSIEntries == other.BSIEntries &&
		s.TotalFiles == other.TotalFiles &&
		s.StandardFiles == other.StandardFiles &&
		s.BSIFiles == other.BSIFiles
}
