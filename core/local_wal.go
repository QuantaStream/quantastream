package core

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	LocalWALFormat           = "quantastream-local-wal"
	LocalWALCheckpointFormat = "quantastream-local-wal-checkpoint"
	LocalWALVersion          = 1
)

type LocalWALOptions struct {
	SyncOnAppend   bool
	CheckpointPath string
}

type LocalWAL struct {
	mu             sync.Mutex
	path           string
	checkpointPath string
	file           *os.File
	nextLSN        uint64
	checkpointLSN  uint64
	syncOnAppend   bool
}

type LocalWALRecord struct {
	Version     int             `json:"version"`
	LSN         uint64          `json:"lsn"`
	Timestamp   time.Time       `json:"timestamp"`
	Transaction string          `json:"transaction,omitempty"`
	Statement   string          `json:"statement,omitempty"`
	OperationID string          `json:"operation_id"`
	Kind        string          `json:"kind"`
	Table       string          `json:"table,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

type LocalWALFrame struct {
	Format string         `json:"format"`
	Record LocalWALRecord `json:"record"`
	SHA256 string         `json:"sha256"`
}

type LocalWALSummary struct {
	RecordCount      int
	LastLSN          uint64
	CheckpointLSN    uint64
	CheckpointPath   string
	CheckpointExists bool
	ByteCount        int64
}

type LocalWALCheckpoint struct {
	Version          int       `json:"version"`
	Format           string    `json:"format"`
	WALPath          string    `json:"wal_path"`
	LastCommittedLSN uint64    `json:"last_committed_lsn"`
	OperationID      string    `json:"operation_id,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type LocalWALCheckpointFrame struct {
	Format     string             `json:"format"`
	Checkpoint LocalWALCheckpoint `json:"checkpoint"`
	SHA256     string             `json:"sha256"`
}

func OpenLocalWAL(path string) (*LocalWAL, error) {
	return OpenLocalWALWithOptions(path, LocalWALOptions{SyncOnAppend: true})
}

func OpenLocalWALWithOptions(path string, opts LocalWALOptions) (*LocalWAL, error) {
	resolved, err := ResolveLocalFileTarget(path)
	if err != nil {
		return nil, err
	}
	checkpointPath, err := localWALCheckpointPath(resolved, opts.CheckpointPath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0755); err != nil {
		return nil, fmt.Errorf("create WAL parent directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0755); err != nil {
		return nil, fmt.Errorf("create WAL checkpoint parent directory: %w", err)
	}
	records, err := ReadLocalWAL(resolved)
	if err != nil {
		return nil, err
	}
	var lastLSN uint64
	for _, record := range records {
		if record.LSN <= lastLSN {
			return nil, fmt.Errorf("WAL LSN sequence is not increasing: got %d after %d", record.LSN, lastLSN)
		}
		lastLSN = record.LSN
	}
	checkpoint, checkpointExists, err := LoadLocalWALCheckpoint(checkpointPath)
	if err != nil {
		return nil, err
	}
	if checkpointExists && checkpoint.LastCommittedLSN > lastLSN {
		return nil, fmt.Errorf("WAL checkpoint LSN %d is ahead of WAL tail %d", checkpoint.LastCommittedLSN, lastLSN)
	}
	file, err := os.OpenFile(resolved, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL %q: %w", resolved, err)
	}
	return &LocalWAL{
		path:           resolved,
		checkpointPath: checkpointPath,
		file:           file,
		nextLSN:        lastLSN + 1,
		checkpointLSN:  checkpoint.LastCommittedLSN,
		syncOnAppend:   opts.SyncOnAppend,
	}, nil
}

func (w *LocalWAL) Path() string {
	if w == nil {
		return ""
	}
	return w.path
}

func (w *LocalWAL) CheckpointPath() string {
	if w == nil {
		return ""
	}
	return w.checkpointPath
}

func (w *LocalWAL) Append(ctx context.Context, record LocalWALRecord) (LocalWALRecord, error) {
	if w == nil || w.file == nil {
		return LocalWALRecord{}, fmt.Errorf("WAL is not open")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return LocalWALRecord{}, err
	}
	return w.appendLocked(record)
}

func (w *LocalWAL) CommitBoundary(ctx context.Context, record LocalWALRecord, commit func() error) (LocalWALRecord, LocalWALCheckpoint, error) {
	if w == nil || w.file == nil {
		return LocalWALRecord{}, LocalWALCheckpoint{}, fmt.Errorf("WAL is not open")
	}
	if commit == nil {
		return LocalWALRecord{}, LocalWALCheckpoint{}, fmt.Errorf("WAL commit function is required")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return LocalWALRecord{}, LocalWALCheckpoint{}, err
	}
	committedRecord, err := w.appendLocked(record)
	if err != nil {
		return LocalWALRecord{}, LocalWALCheckpoint{}, err
	}
	if err := commit(); err != nil {
		return committedRecord, LocalWALCheckpoint{}, err
	}
	checkpoint, err := w.advanceCheckpointLocked(committedRecord)
	if err != nil {
		return committedRecord, LocalWALCheckpoint{}, err
	}
	return committedRecord, checkpoint, nil
}

func (w *LocalWAL) appendLocked(record LocalWALRecord) (LocalWALRecord, error) {
	record.Version = LocalWALVersion
	record.LSN = w.nextLSN
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	} else {
		record.Timestamp = record.Timestamp.UTC()
	}
	if len(record.Payload) == 0 {
		record.Payload = json.RawMessage(`{}`)
	}
	if err := validateLocalWALRecord(record); err != nil {
		return LocalWALRecord{}, err
	}
	frame, err := newLocalWALFrame(record)
	if err != nil {
		return LocalWALRecord{}, err
	}
	data, err := json.Marshal(frame)
	if err != nil {
		return LocalWALRecord{}, fmt.Errorf("marshal WAL frame: %w", err)
	}
	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return LocalWALRecord{}, fmt.Errorf("append WAL frame: %w", err)
	}
	if w.syncOnAppend {
		if err := w.file.Sync(); err != nil {
			return LocalWALRecord{}, fmt.Errorf("sync WAL frame: %w", err)
		}
	}
	w.nextLSN++
	return record, nil
}

func (w *LocalWAL) advanceCheckpointLocked(record LocalWALRecord) (LocalWALCheckpoint, error) {
	checkpoint := LocalWALCheckpoint{
		Version:          LocalWALVersion,
		Format:           LocalWALCheckpointFormat,
		WALPath:          w.path,
		LastCommittedLSN: record.LSN,
		OperationID:      record.OperationID,
		UpdatedAt:        time.Now().UTC(),
	}
	if checkpoint.LastCommittedLSN <= w.checkpointLSN {
		return checkpoint, nil
	}
	if err := WriteLocalWALCheckpoint(w.checkpointPath, checkpoint); err != nil {
		return LocalWALCheckpoint{}, err
	}
	w.checkpointLSN = checkpoint.LastCommittedLSN
	return checkpoint, nil
}

func (w *LocalWAL) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.file.Close()
	w.file = nil
	return err
}

func ReadLocalWAL(path string) ([]LocalWALRecord, error) {
	resolved, err := ResolveLocalFileTarget(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(resolved)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open WAL %q: %w", resolved, err)
	}
	defer file.Close()
	return readLocalWAL(file)
}

func ValidateLocalWAL(path string) (LocalWALSummary, error) {
	resolved, err := ResolveLocalFileTarget(path)
	if err != nil {
		return LocalWALSummary{}, err
	}
	checkpointPath, err := localWALCheckpointPath(resolved, "")
	if err != nil {
		return LocalWALSummary{}, err
	}
	records, err := ReadLocalWAL(resolved)
	if err != nil {
		return LocalWALSummary{}, err
	}
	info, err := os.Stat(resolved)
	if errors.Is(err, os.ErrNotExist) {
		return LocalWALSummary{}, nil
	}
	if err != nil {
		return LocalWALSummary{}, fmt.Errorf("stat WAL %q: %w", resolved, err)
	}
	var lastLSN uint64
	for _, record := range records {
		if record.LSN <= lastLSN {
			return LocalWALSummary{}, fmt.Errorf("WAL LSN sequence is not increasing: got %d after %d", record.LSN, lastLSN)
		}
		lastLSN = record.LSN
	}
	checkpoint, checkpointExists, err := LoadLocalWALCheckpoint(checkpointPath)
	if err != nil {
		return LocalWALSummary{}, err
	}
	if checkpointExists && checkpoint.LastCommittedLSN > lastLSN {
		return LocalWALSummary{}, fmt.Errorf("WAL checkpoint LSN %d is ahead of WAL tail %d", checkpoint.LastCommittedLSN, lastLSN)
	}
	return LocalWALSummary{
		RecordCount:      len(records),
		LastLSN:          lastLSN,
		CheckpointLSN:    checkpoint.LastCommittedLSN,
		CheckpointPath:   checkpointPath,
		CheckpointExists: checkpointExists,
		ByteCount:        info.Size(),
	}, nil
}

func LoadLocalWALCheckpoint(path string) (LocalWALCheckpoint, bool, error) {
	resolved, err := ResolveLocalFileTarget(path)
	if err != nil {
		return LocalWALCheckpoint{}, false, err
	}
	data, err := os.ReadFile(resolved)
	if errors.Is(err, os.ErrNotExist) {
		return LocalWALCheckpoint{}, false, nil
	}
	if err != nil {
		return LocalWALCheckpoint{}, false, fmt.Errorf("read WAL checkpoint %q: %w", resolved, err)
	}
	var frame LocalWALCheckpointFrame
	if err := json.Unmarshal(data, &frame); err != nil {
		return LocalWALCheckpoint{}, false, fmt.Errorf("parse WAL checkpoint %q: %w", resolved, err)
	}
	if err := validateLocalWALCheckpointFrame(frame); err != nil {
		return LocalWALCheckpoint{}, false, err
	}
	return frame.Checkpoint, true, nil
}

func WriteLocalWALCheckpoint(path string, checkpoint LocalWALCheckpoint) error {
	resolved, err := ResolveLocalFileTarget(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0755); err != nil {
		return fmt.Errorf("create WAL checkpoint parent directory: %w", err)
	}
	checkpoint.Version = LocalWALVersion
	checkpoint.Format = LocalWALCheckpointFormat
	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	} else {
		checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
	}
	frame, err := newLocalWALCheckpointFrame(checkpoint)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(frame, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal WAL checkpoint: %w", err)
	}
	tmpPath := resolved + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("write WAL checkpoint %q: %w", resolved, err)
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write WAL checkpoint %q: %w", resolved, writeErr)
	}
	if syncErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync WAL checkpoint %q: %w", resolved, syncErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close WAL checkpoint %q: %w", resolved, closeErr)
	}
	if err := os.Rename(tmpPath, resolved); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("commit WAL checkpoint %q: %w", resolved, err)
	}
	return nil
}

func ReplayLocalWAL(path string, apply func(LocalWALRecord) error) error {
	if apply == nil {
		return fmt.Errorf("WAL replay function is required")
	}
	records, err := ReadLocalWAL(path)
	if err != nil {
		return err
	}
	var lastLSN uint64
	for _, record := range records {
		if record.LSN <= lastLSN {
			return fmt.Errorf("WAL LSN sequence is not increasing: got %d after %d", record.LSN, lastLSN)
		}
		if err := apply(record); err != nil {
			return fmt.Errorf("replay WAL record lsn=%d operation_id=%s: %w", record.LSN, record.OperationID, err)
		}
		lastLSN = record.LSN
	}
	return nil
}

func readLocalWAL(reader io.Reader) ([]LocalWALRecord, error) {
	buffered := bufio.NewReader(reader)
	var records []LocalWALRecord
	var lastLSN uint64
	lineNumber := 0
	for {
		line, err := buffered.ReadBytes('\n')
		if len(line) > 0 {
			lineNumber++
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var frame LocalWALFrame
			if err := json.Unmarshal(line, &frame); err != nil {
				return nil, fmt.Errorf("parse WAL frame line %d: %w", lineNumber, err)
			}
			if err := validateLocalWALFrame(frame); err != nil {
				return nil, fmt.Errorf("validate WAL frame line %d: %w", lineNumber, err)
			}
			if frame.Record.LSN <= lastLSN {
				return nil, fmt.Errorf("WAL LSN sequence is not increasing at line %d: got %d after %d", lineNumber, frame.Record.LSN, lastLSN)
			}
			lastLSN = frame.Record.LSN
			records = append(records, frame.Record)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read WAL frame line %d: %w", lineNumber+1, err)
		}
	}
	return records, nil
}

func newLocalWALFrame(record LocalWALRecord) (LocalWALFrame, error) {
	checksum, err := localWALRecordChecksum(record)
	if err != nil {
		return LocalWALFrame{}, err
	}
	return LocalWALFrame{
		Format: LocalWALFormat,
		Record: record,
		SHA256: checksum,
	}, nil
}

func validateLocalWALFrame(frame LocalWALFrame) error {
	if frame.Format != LocalWALFormat {
		return fmt.Errorf("unsupported WAL format %q", frame.Format)
	}
	if err := validateLocalWALRecord(frame.Record); err != nil {
		return err
	}
	checksum, err := localWALRecordChecksum(frame.Record)
	if err != nil {
		return err
	}
	if checksum != frame.SHA256 {
		return fmt.Errorf("checksum mismatch for lsn=%d", frame.Record.LSN)
	}
	return nil
}

func newLocalWALCheckpointFrame(checkpoint LocalWALCheckpoint) (LocalWALCheckpointFrame, error) {
	checksum, err := localWALCheckpointChecksum(checkpoint)
	if err != nil {
		return LocalWALCheckpointFrame{}, err
	}
	return LocalWALCheckpointFrame{
		Format:     LocalWALCheckpointFormat,
		Checkpoint: checkpoint,
		SHA256:     checksum,
	}, nil
}

func validateLocalWALCheckpointFrame(frame LocalWALCheckpointFrame) error {
	if frame.Format != LocalWALCheckpointFormat {
		return fmt.Errorf("unsupported WAL checkpoint format %q", frame.Format)
	}
	if frame.Checkpoint.Version != LocalWALVersion {
		return fmt.Errorf("unsupported WAL checkpoint version %d", frame.Checkpoint.Version)
	}
	if frame.Checkpoint.Format != LocalWALCheckpointFormat {
		return fmt.Errorf("unsupported WAL checkpoint inner format %q", frame.Checkpoint.Format)
	}
	checksum, err := localWALCheckpointChecksum(frame.Checkpoint)
	if err != nil {
		return err
	}
	if checksum != frame.SHA256 {
		return fmt.Errorf("WAL checkpoint checksum mismatch")
	}
	return nil
}

func validateLocalWALRecord(record LocalWALRecord) error {
	if record.Version != LocalWALVersion {
		return fmt.Errorf("unsupported WAL record version %d", record.Version)
	}
	if record.LSN == 0 {
		return fmt.Errorf("WAL record LSN is required")
	}
	if strings.TrimSpace(record.Kind) == "" {
		return fmt.Errorf("WAL record kind is required")
	}
	if strings.TrimSpace(record.OperationID) == "" {
		return fmt.Errorf("WAL record operation_id is required")
	}
	if len(record.Payload) == 0 {
		return fmt.Errorf("WAL record payload is required")
	}
	if !json.Valid(record.Payload) {
		return fmt.Errorf("WAL record payload is not valid JSON")
	}
	return nil
}

func localWALRecordChecksum(record LocalWALRecord) (string, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("marshal WAL record for checksum: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func localWALCheckpointChecksum(checkpoint LocalWALCheckpoint) (string, error) {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return "", fmt.Errorf("marshal WAL checkpoint for checksum: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func localWALCheckpointPath(walPath, override string) (string, error) {
	if strings.TrimSpace(override) != "" {
		return ResolveLocalFileTarget(override)
	}
	if strings.TrimSpace(walPath) == "" {
		return "", fmt.Errorf("WAL path is required")
	}
	return filepath.Clean(walPath) + ".checkpoint.json", nil
}
