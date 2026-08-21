package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	LocalStorageBackupManifestFileName = "manifest.json"
	LocalStorageBackupSnapshotDir      = "snapshot"
	LocalStorageBackupFormat           = "quantastream-local-storage-backup"
	LocalStorageBackupVersion          = 1
	LocalStorageBackupModeOffline      = "offline-local-snapshot"
	LocalStorageBackupModeQuiescent    = "quiescent-local-snapshot"
	LocalStorageQuiescenceFormat       = "quantastream-local-storage-quiescence"
	LocalStorageQuiescenceVersion      = 1
	LocalStorageQuiescenceDir          = ".quantastream"
	LocalStorageQuiescenceFileName     = "backup-quiescence.json"
)

// LocalStorageBackupManifest describes a provider-neutral backup generation.
// The manifest is written last so a backup without this file is incomplete.
type LocalStorageBackupManifest struct {
	Version        int                          `json:"version"`
	Format         string                       `json:"format"`
	Mode           string                       `json:"mode"`
	CreatedAt      time.Time                    `json:"created_at"`
	CompletedAt    time.Time                    `json:"completed_at"`
	SourceDataDir  string                       `json:"source_data_dir"`
	SnapshotDir    string                       `json:"snapshot_dir"`
	FileCount      int                          `json:"file_count"`
	DirectoryCount int                          `json:"directory_count"`
	ByteCount      int64                        `json:"byte_count"`
	Checkpoint     LocalStorageBackupCheckpoint `json:"checkpoint"`
	Entries        []LocalStorageBackupEntry    `json:"entries"`
}

// LocalStorageBackupCheckpoint is intentionally narrow until WAL/checkpoint
// support lands. It reserves the manifest shape without inventing live semantics.
type LocalStorageBackupCheckpoint struct {
	Kind              string `json:"kind"`
	WALIncluded       bool   `json:"wal_included"`
	LSN               uint64 `json:"lsn"`
	WALPath           string `json:"wal_path,omitempty"`
	WALCheckpointPath string `json:"wal_checkpoint_path,omitempty"`
	WALLastLSN        uint64 `json:"wal_last_lsn,omitempty"`
	WALReplayRecords  int    `json:"wal_replay_records,omitempty"`
	WALPendingRecords int    `json:"wal_pending_records,omitempty"`
}

// LocalStorageBackupEntry records one directory or file copied into the backup.
type LocalStorageBackupEntry struct {
	Path    string    `json:"path"`
	Type    string    `json:"type"`
	Mode    uint32    `json:"mode"`
	Size    int64     `json:"size,omitempty"`
	ModTime time.Time `json:"mod_time"`
	SHA256  string    `json:"sha256,omitempty"`
}

type LocalStorageQuiescenceLease struct {
	Version           int       `json:"version"`
	Format            string    `json:"format"`
	ID                string    `json:"id"`
	Owner             string    `json:"owner,omitempty"`
	Reason            string    `json:"reason,omitempty"`
	DataDir           string    `json:"data_dir"`
	WALPath           string    `json:"wal_path,omitempty"`
	WALCheckpointPath string    `json:"wal_checkpoint_path,omitempty"`
	CheckpointLSN     uint64    `json:"checkpoint_lsn,omitempty"`
	LastLSN           uint64    `json:"last_lsn,omitempty"`
	ReplayRecords     int       `json:"replay_records,omitempty"`
	PendingRecords    int       `json:"pending_records,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

type CreateLocalStorageBackupRequest struct {
	DataDir string
	Target  string
	Now     time.Time
	Quiesce bool
	WALPath string
	Owner   string
	Reason  string
}

type RestoreLocalStorageBackupRequest struct {
	Source  string
	DataDir string
}

// ResolveLocalFileTarget accepts file:// URLs and plain local paths. Other
// schemes belong to future backup adapters.
func ResolveLocalFileTarget(location string) (string, error) {
	location = strings.TrimSpace(location)
	if location == "" {
		return "", fmt.Errorf("local backup target is required")
	}
	if strings.Contains(location, "://") {
		parsed, err := url.Parse(location)
		if err != nil {
			return "", fmt.Errorf("parse backup target %q: %w", location, err)
		}
		if parsed.Scheme != "file" {
			return "", fmt.Errorf("unsupported backup target scheme %q; only file:// is supported", parsed.Scheme)
		}
		if parsed.Host != "" {
			return "", fmt.Errorf("file backup target must not include a host: %q", location)
		}
		location = parsed.Path
	}
	if strings.TrimSpace(location) == "" {
		return "", fmt.Errorf("local backup target path is required")
	}
	abs, err := filepath.Abs(location)
	if err != nil {
		return "", fmt.Errorf("resolve backup target %q: %w", location, err)
	}
	return filepath.Clean(abs), nil
}

func CreateLocalStorageBackup(ctx context.Context, req CreateLocalStorageBackupRequest) (LocalStorageBackupManifest, error) {
	var manifest LocalStorageBackupManifest
	sourceDir, err := resolveExistingDirectory(req.DataDir, "backup data directory")
	if err != nil {
		return manifest, err
	}
	var lease LocalStorageQuiescenceLease
	if req.Quiesce {
		lease, err = BeginLocalStorageQuiescence(ctx, BeginLocalStorageQuiescenceRequest{
			DataDir: sourceDir,
			WALPath: req.WALPath,
			Owner:   req.Owner,
			Reason:  req.Reason,
			Now:     req.Now,
		})
		if err != nil {
			return manifest, err
		}
		defer func() { _ = EndLocalStorageQuiescence(sourceDir, lease.ID) }()
	}
	targetDir, err := ResolveLocalFileTarget(req.Target)
	if err != nil {
		return manifest, err
	}
	if err := rejectNestedBackupPaths(sourceDir, targetDir); err != nil {
		return manifest, err
	}
	if err := ensureDirectoryEmptyOrMissing(targetDir, "backup target"); err != nil {
		return manifest, err
	}
	snapshotDir := filepath.Join(targetDir, LocalStorageBackupSnapshotDir)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return manifest, fmt.Errorf("create backup snapshot directory: %w", err)
	}

	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	manifest = LocalStorageBackupManifest{
		Version:       LocalStorageBackupVersion,
		Format:        LocalStorageBackupFormat,
		Mode:          LocalStorageBackupModeOffline,
		CreatedAt:     now.UTC(),
		SourceDataDir: sourceDir,
		SnapshotDir:   LocalStorageBackupSnapshotDir,
		Checkpoint: LocalStorageBackupCheckpoint{
			Kind:        "offline",
			WALIncluded: false,
		},
	}
	if req.Quiesce {
		manifest.Mode = LocalStorageBackupModeQuiescent
		manifest.Checkpoint = LocalStorageBackupCheckpoint{
			Kind:              "quiescent",
			WALIncluded:       localStoragePathInside(sourceDir, lease.WALPath),
			LSN:               lease.CheckpointLSN,
			WALPath:           lease.WALPath,
			WALCheckpointPath: lease.WALCheckpointPath,
			WALLastLSN:        lease.LastLSN,
			WALReplayRecords:  lease.ReplayRecords,
			WALPendingRecords: lease.PendingRecords,
		}
		if req.WALPath == "" {
			manifest.Checkpoint.Kind = "quiescent-no-wal"
		}
	}

	err = filepath.WalkDir(sourceDir, func(srcPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if srcPath == sourceDir {
			return nil
		}
		if localStorageBackupSkipQuiescenceFile(sourceDir, srcPath) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat backup source %s: %w", srcPath, err)
		}
		relPath, err := manifestRelativePath(sourceDir, srcPath)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("backup source contains unsupported symlink: %s", relPath)
		}
		destPath, err := safeJoin(snapshotDir, relPath)
		if err != nil {
			return err
		}
		backupEntry := LocalStorageBackupEntry{
			Path:    relPath,
			Mode:    uint32(info.Mode().Perm()),
			ModTime: info.ModTime().UTC(),
		}
		if entry.IsDir() {
			backupEntry.Type = "dir"
			if err := os.MkdirAll(destPath, info.Mode().Perm()); err != nil {
				return fmt.Errorf("create backup directory %s: %w", relPath, err)
			}
			manifest.DirectoryCount++
			manifest.Entries = append(manifest.Entries, backupEntry)
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("backup source contains unsupported file type: %s", relPath)
		}
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("create backup parent for %s: %w", relPath, err)
		}
		size, checksum, err := copyFileWithSHA256(srcPath, destPath, info.Mode().Perm())
		if err != nil {
			return fmt.Errorf("copy backup file %s: %w", relPath, err)
		}
		if err := os.Chtimes(destPath, info.ModTime(), info.ModTime()); err != nil {
			return fmt.Errorf("preserve backup file timestamp %s: %w", relPath, err)
		}
		backupEntry.Type = "file"
		backupEntry.Size = size
		backupEntry.SHA256 = checksum
		manifest.FileCount++
		manifest.ByteCount += size
		manifest.Entries = append(manifest.Entries, backupEntry)
		return nil
	})
	if err != nil {
		return manifest, fmt.Errorf("create local storage backup: %w", err)
	}

	sort.Slice(manifest.Entries, func(i, j int) bool {
		if manifest.Entries[i].Path == manifest.Entries[j].Path {
			return manifest.Entries[i].Type < manifest.Entries[j].Type
		}
		return manifest.Entries[i].Path < manifest.Entries[j].Path
	})
	manifest.CompletedAt = time.Now().UTC()
	if req.Now.IsZero() {
		manifest.CompletedAt = manifest.CompletedAt.UTC()
	} else {
		manifest.CompletedAt = req.Now.UTC()
	}
	if err := writeLocalStorageBackupManifest(targetDir, manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

type BeginLocalStorageQuiescenceRequest struct {
	DataDir string
	WALPath string
	Owner   string
	Reason  string
	Now     time.Time
}

func BeginLocalStorageQuiescence(ctx context.Context, req BeginLocalStorageQuiescenceRequest) (LocalStorageQuiescenceLease, error) {
	dataDir, err := resolveExistingDirectory(req.DataDir, "quiescence data directory")
	if err != nil {
		return LocalStorageQuiescenceLease{}, err
	}
	if existing, found, err := ObserveLocalStorageQuiescence(dataDir); err != nil {
		return LocalStorageQuiescenceLease{}, err
	} else if found {
		return LocalStorageQuiescenceLease{}, fmt.Errorf("storage is already quiesced for backup id=%s owner=%s reason=%s", existing.ID, existing.Owner, existing.Reason)
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	lease := LocalStorageQuiescenceLease{
		Version:   LocalStorageQuiescenceVersion,
		Format:    LocalStorageQuiescenceFormat,
		ID:        fmt.Sprintf("%d-%d", now.UnixNano(), os.Getpid()),
		Owner:     strings.TrimSpace(req.Owner),
		Reason:    strings.TrimSpace(req.Reason),
		DataDir:   dataDir,
		CreatedAt: now.UTC(),
	}
	if strings.TrimSpace(lease.Owner) == "" {
		lease.Owner = "quanta-admin backup"
	}
	if strings.TrimSpace(lease.Reason) == "" {
		lease.Reason = "local storage backup"
	}
	if strings.TrimSpace(req.WALPath) != "" {
		plan, err := PlanLocalWALRecovery(req.WALPath)
		if err != nil {
			return LocalStorageQuiescenceLease{}, fmt.Errorf("plan WAL recovery before quiescence: %w", err)
		}
		lease.WALPath = plan.WALPath
		lease.WALCheckpointPath = plan.CheckpointPath
		lease.CheckpointLSN = plan.CheckpointLSN
		lease.LastLSN = plan.LastLSN
		lease.ReplayRecords = len(plan.ReplayRecords)
		lease.PendingRecords = len(plan.PendingRecords)
		if !localStoragePathInside(dataDir, lease.WALPath) {
			return LocalStorageQuiescenceLease{}, fmt.Errorf("quiescent backup WAL path %q is outside data directory %q; place the WAL under the data directory or omit WAL capture for this local snapshot", lease.WALPath, dataDir)
		}
		if !localStoragePathInside(dataDir, lease.WALCheckpointPath) {
			return LocalStorageQuiescenceLease{}, fmt.Errorf("quiescent backup WAL checkpoint path %q is outside data directory %q; place the WAL checkpoint under the data directory or omit WAL capture for this local snapshot", lease.WALCheckpointPath, dataDir)
		}
	}
	if err := ctx.Err(); err != nil {
		return LocalStorageQuiescenceLease{}, err
	}
	if err := writeLocalStorageQuiescenceLease(dataDir, lease); err != nil {
		return LocalStorageQuiescenceLease{}, err
	}
	return lease, nil
}

func EndLocalStorageQuiescence(dataDir, leaseID string) error {
	path, err := LocalStorageQuiescencePath(dataDir)
	if err != nil {
		return err
	}
	lease, found, err := ObserveLocalStorageQuiescence(dataDir)
	if err != nil || !found {
		return err
	}
	if strings.TrimSpace(leaseID) != "" && lease.ID != leaseID {
		return fmt.Errorf("quiescence lease id mismatch: active=%s requested=%s", lease.ID, leaseID)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove quiescence lease: %w", err)
	}
	return nil
}

func ObserveLocalStorageQuiescence(dataDir string) (LocalStorageQuiescenceLease, bool, error) {
	path, err := LocalStorageQuiescencePath(dataDir)
	if err != nil {
		return LocalStorageQuiescenceLease{}, false, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return LocalStorageQuiescenceLease{}, false, nil
	}
	if err != nil {
		return LocalStorageQuiescenceLease{}, false, fmt.Errorf("read quiescence lease: %w", err)
	}
	var lease LocalStorageQuiescenceLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return LocalStorageQuiescenceLease{}, false, fmt.Errorf("parse quiescence lease: %w", err)
	}
	if lease.Version != LocalStorageQuiescenceVersion || lease.Format != LocalStorageQuiescenceFormat {
		return LocalStorageQuiescenceLease{}, false, fmt.Errorf("unsupported quiescence lease %d/%s", lease.Version, lease.Format)
	}
	return lease, true, nil
}

func RejectLocalStorageMutationIfQuiesced(dataDir, operation string) error {
	lease, found, err := ObserveLocalStorageQuiescence(dataDir)
	if err != nil || !found {
		return err
	}
	if strings.TrimSpace(operation) == "" {
		operation = "storage mutation"
	}
	return fmt.Errorf("%s is blocked while local storage is quiesced for backup id=%s owner=%s reason=%s", operation, lease.ID, lease.Owner, lease.Reason)
}

func LocalStorageQuiescencePath(dataDir string) (string, error) {
	dataDir = strings.TrimSpace(dataDir)
	if dataDir == "" {
		return "", fmt.Errorf("quiescence data directory is required")
	}
	resolved, err := ResolveLocalFileTarget(dataDir)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, LocalStorageQuiescenceDir, LocalStorageQuiescenceFileName), nil
}

func writeLocalStorageQuiescenceLease(dataDir string, lease LocalStorageQuiescenceLease) error {
	path, err := LocalStorageQuiescencePath(dataDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create quiescence lease directory: %w", err)
	}
	data, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal quiescence lease: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("write quiescence lease: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write quiescence lease: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close quiescence lease: %w", err)
	}
	return nil
}

func LoadLocalStorageBackupManifest(source string) (LocalStorageBackupManifest, string, error) {
	var manifest LocalStorageBackupManifest
	backupDir, err := ResolveLocalFileTarget(source)
	if err != nil {
		return manifest, "", err
	}
	data, err := os.ReadFile(filepath.Join(backupDir, LocalStorageBackupManifestFileName))
	if err != nil {
		return manifest, backupDir, fmt.Errorf("read backup manifest: %w", err)
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, backupDir, fmt.Errorf("parse backup manifest: %w", err)
	}
	if err := validateLocalStorageBackupManifestShape(manifest); err != nil {
		return manifest, backupDir, err
	}
	return manifest, backupDir, nil
}

func ValidateLocalStorageBackup(source string) (LocalStorageBackupManifest, error) {
	manifest, backupDir, err := LoadLocalStorageBackupManifest(source)
	if err != nil {
		return manifest, err
	}
	if err := validateLocalStorageBackupFiles(backupDir, manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func RestoreLocalStorageBackup(ctx context.Context, req RestoreLocalStorageBackupRequest) (LocalStorageBackupManifest, error) {
	manifest, backupDir, err := LoadLocalStorageBackupManifest(req.Source)
	if err != nil {
		return manifest, err
	}
	if err := validateLocalStorageBackupFiles(backupDir, manifest); err != nil {
		return manifest, err
	}
	targetDir, err := ResolveLocalFileTarget(req.DataDir)
	if err != nil {
		return manifest, err
	}
	if err := ensureDirectoryEmptyOrMissing(targetDir, "restore data directory"); err != nil {
		return manifest, err
	}
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return manifest, fmt.Errorf("create restore data directory: %w", err)
	}
	snapshotDir := filepath.Join(backupDir, manifest.SnapshotDir)
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return manifest, err
		}
		if entry.Type != "dir" {
			continue
		}
		targetPath, err := safeJoin(targetDir, entry.Path)
		if err != nil {
			return manifest, err
		}
		if err := os.MkdirAll(targetPath, os.FileMode(entry.Mode)); err != nil {
			return manifest, fmt.Errorf("restore directory %s: %w", entry.Path, err)
		}
	}
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return manifest, err
		}
		if entry.Type != "file" {
			continue
		}
		srcPath, err := safeJoin(snapshotDir, entry.Path)
		if err != nil {
			return manifest, err
		}
		targetPath, err := safeJoin(targetDir, entry.Path)
		if err != nil {
			return manifest, err
		}
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return manifest, fmt.Errorf("create restore parent for %s: %w", entry.Path, err)
		}
		if _, _, err := copyFileWithSHA256(srcPath, targetPath, os.FileMode(entry.Mode)); err != nil {
			return manifest, fmt.Errorf("restore file %s: %w", entry.Path, err)
		}
		if err := os.Chtimes(targetPath, entry.ModTime, entry.ModTime); err != nil {
			return manifest, fmt.Errorf("preserve restored file timestamp %s: %w", entry.Path, err)
		}
	}
	return manifest, nil
}

func writeLocalStorageBackupManifest(backupDir string, manifest LocalStorageBackupManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal backup manifest: %w", err)
	}
	path := filepath.Join(backupDir, LocalStorageBackupManifestFileName)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write backup manifest: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("commit backup manifest: %w", err)
	}
	return nil
}

func validateLocalStorageBackupManifestShape(manifest LocalStorageBackupManifest) error {
	if manifest.Version != LocalStorageBackupVersion {
		return fmt.Errorf("unsupported backup manifest version %d", manifest.Version)
	}
	if manifest.Format != LocalStorageBackupFormat {
		return fmt.Errorf("unsupported backup manifest format %q", manifest.Format)
	}
	switch manifest.Mode {
	case LocalStorageBackupModeOffline, LocalStorageBackupModeQuiescent:
	default:
		return fmt.Errorf("unsupported backup manifest mode %q", manifest.Mode)
	}
	if manifest.SnapshotDir != LocalStorageBackupSnapshotDir {
		return fmt.Errorf("unsupported backup snapshot directory %q", manifest.SnapshotDir)
	}
	for _, entry := range manifest.Entries {
		if _, err := cleanManifestPath(entry.Path); err != nil {
			return err
		}
		switch entry.Type {
		case "dir", "file":
		default:
			return fmt.Errorf("unsupported backup entry type %q for %s", entry.Type, entry.Path)
		}
	}
	return nil
}

func localStorageBackupSkipQuiescenceFile(sourceDir, srcPath string) bool {
	leasePath, err := LocalStorageQuiescencePath(sourceDir)
	if err != nil {
		return false
	}
	relLease, err := filepath.Rel(filepath.Clean(sourceDir), filepath.Clean(leasePath))
	if err != nil || strings.HasPrefix(relLease, ".."+string(filepath.Separator)) || relLease == ".." {
		return false
	}
	return filepath.Clean(srcPath) == filepath.Clean(leasePath)
}

func localStoragePathInside(root, candidate string) bool {
	root = strings.TrimSpace(root)
	candidate = strings.TrimSpace(candidate)
	if root == "" || candidate == "" {
		return false
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return false
	}
	return sameOrChildPath(filepath.Clean(rootAbs), filepath.Clean(candidateAbs))
}

func validateLocalStorageBackupFiles(backupDir string, manifest LocalStorageBackupManifest) error {
	snapshotDir := filepath.Join(backupDir, manifest.SnapshotDir)
	var fileCount int
	var dirCount int
	var byteCount int64
	for _, entry := range manifest.Entries {
		entryPath, err := safeJoin(snapshotDir, entry.Path)
		if err != nil {
			return err
		}
		info, err := os.Stat(entryPath)
		if err != nil {
			return fmt.Errorf("validate backup entry %s: %w", entry.Path, err)
		}
		switch entry.Type {
		case "dir":
			if !info.IsDir() {
				return fmt.Errorf("backup entry %s is not a directory", entry.Path)
			}
			dirCount++
		case "file":
			if !info.Mode().IsRegular() {
				return fmt.Errorf("backup entry %s is not a regular file", entry.Path)
			}
			if info.Size() != entry.Size {
				return fmt.Errorf("backup entry %s size mismatch: got %d want %d", entry.Path, info.Size(), entry.Size)
			}
			checksum, err := fileSHA256(entryPath)
			if err != nil {
				return fmt.Errorf("checksum backup entry %s: %w", entry.Path, err)
			}
			if checksum != entry.SHA256 {
				return fmt.Errorf("backup entry %s checksum mismatch", entry.Path)
			}
			fileCount++
			byteCount += entry.Size
		}
	}
	if fileCount != manifest.FileCount {
		return fmt.Errorf("backup manifest file count mismatch: got %d want %d", fileCount, manifest.FileCount)
	}
	if dirCount != manifest.DirectoryCount {
		return fmt.Errorf("backup manifest directory count mismatch: got %d want %d", dirCount, manifest.DirectoryCount)
	}
	if byteCount != manifest.ByteCount {
		return fmt.Errorf("backup manifest byte count mismatch: got %d want %d", byteCount, manifest.ByteCount)
	}
	return nil
}

func resolveExistingDirectory(location, name string) (string, error) {
	resolved, err := ResolveLocalFileTarget(location)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("%s %q is not readable: %w", name, resolved, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s %q is not a directory", name, resolved)
	}
	return resolved, nil
}

func rejectNestedBackupPaths(sourceDir, targetDir string) error {
	sourceDir = filepath.Clean(sourceDir)
	targetDir = filepath.Clean(targetDir)
	if sameOrChildPath(sourceDir, targetDir) {
		return fmt.Errorf("backup target %q must not be inside source data directory %q", targetDir, sourceDir)
	}
	if sameOrChildPath(targetDir, sourceDir) {
		return fmt.Errorf("backup source data directory %q must not be inside backup target %q", sourceDir, targetDir)
	}
	return nil
}

func sameOrChildPath(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func ensureDirectoryEmptyOrMissing(dir, name string) error {
	info, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat %s %q: %w", name, dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %q is not a directory", name, dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read %s %q: %w", name, dir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("%s %q must be empty", name, dir)
	}
	return nil
}

func manifestRelativePath(root, item string) (string, error) {
	rel, err := filepath.Rel(root, item)
	if err != nil {
		return "", fmt.Errorf("resolve backup relative path for %s: %w", item, err)
	}
	return cleanManifestPath(filepath.ToSlash(rel))
}

func cleanManifestPath(rel string) (string, error) {
	rel = strings.TrimSpace(filepath.ToSlash(rel))
	if rel == "" || rel == "." {
		return "", fmt.Errorf("backup entry path is empty")
	}
	if strings.HasPrefix(rel, "/") {
		return "", fmt.Errorf("backup entry path must be relative: %s", rel)
	}
	clean := path.Clean(rel)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("backup entry path escapes snapshot: %s", rel)
	}
	return clean, nil
}

func safeJoin(root, rel string) (string, error) {
	clean, err := cleanManifestPath(rel)
	if err != nil {
		return "", err
	}
	joined := filepath.Join(root, filepath.FromSlash(clean))
	rootClean := filepath.Clean(root)
	joinedClean := filepath.Clean(joined)
	if !sameOrChildPath(rootClean, joinedClean) {
		return "", fmt.Errorf("backup entry path escapes target: %s", rel)
	}
	return joinedClean, nil
}

func copyFileWithSHA256(srcPath, destPath string, mode os.FileMode) (int64, string, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return 0, "", err
	}
	defer src.Close()
	dst, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return 0, "", err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(dst, hash), src)
	closeErr := dst.Close()
	if copyErr != nil {
		return 0, "", copyErr
	}
	if closeErr != nil {
		return 0, "", closeErr
	}
	return size, hex.EncodeToString(hash.Sum(nil)), nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
