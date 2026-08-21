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
	LocalWALFormat  = "quantastream-local-wal"
	LocalWALVersion = 1
)

type LocalWALOptions struct {
	SyncOnAppend bool
}

type LocalWAL struct {
	mu           sync.Mutex
	path         string
	file         *os.File
	nextLSN      uint64
	syncOnAppend bool
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
	RecordCount int
	LastLSN     uint64
	ByteCount   int64
}

func OpenLocalWAL(path string) (*LocalWAL, error) {
	return OpenLocalWALWithOptions(path, LocalWALOptions{SyncOnAppend: true})
}

func OpenLocalWALWithOptions(path string, opts LocalWALOptions) (*LocalWAL, error) {
	resolved, err := ResolveLocalFileTarget(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0755); err != nil {
		return nil, fmt.Errorf("create WAL parent directory: %w", err)
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
	file, err := os.OpenFile(resolved, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL %q: %w", resolved, err)
	}
	return &LocalWAL{
		path:         resolved,
		file:         file,
		nextLSN:      lastLSN + 1,
		syncOnAppend: opts.SyncOnAppend,
	}, nil
}

func (w *LocalWAL) Path() string {
	if w == nil {
		return ""
	}
	return w.path
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
	return LocalWALSummary{
		RecordCount: len(records),
		LastLSN:     lastLSN,
		ByteCount:   info.Size(),
	}, nil
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
