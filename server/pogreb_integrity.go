package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/akrylysov/pogreb"
)

const (
	pogrebSegmentExt          = ".psg"
	pogrebHeaderSize          = 512
	pogrebRecordHeaderSize    = 6
	pogrebRecordChecksumSize  = 4
	pogrebRecordValueSizeMask = uint32(1<<31) - 1
	pogrebFormatVersion       = uint32(2)
)

var pogrebFileSignature = []byte{'p', 'o', 'g', 'r', 'e', 'b', '\x0e', '\xfd'}

type pogrebIntegritySummary struct {
	Stores       int
	SegmentFiles int
	Records      int
	Bytes        int64
	SkippedLocks int
}

func (s *pogrebIntegritySummary) add(other pogrebIntegritySummary) {
	s.Stores += other.Stores
	s.SegmentFiles += other.SegmentFiles
	s.Records += other.Records
	s.Bytes += other.Bytes
	s.SkippedLocks += other.SkippedLocks
}

func openVerifiedPogrebStore(path string) (*pogreb.DB, error) {
	db, err := pogreb.Open(path, nil)
	if err != nil {
		return nil, err
	}
	if _, err := validatePogrebStore(path); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("pogreb integrity check failed for %s: %v; close failed: %v", path, err, closeErr)
		}
		return nil, fmt.Errorf("pogreb integrity check failed for %s: %w", path, err)
	}
	return db, nil
}

func validatePogrebStoreTree(root string) (pogrebIntegritySummary, error) {
	var summary pogrebIntegritySummary
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return summary, nil
		}
		return summary, err
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		hasSegments, err := pogrebStoreDirHasSegment(path)
		if err != nil {
			return err
		}
		if !hasSegments {
			return nil
		}
		if pogrebStoreDirHasLock(path) {
			summary.SkippedLocks++
			return filepath.SkipDir
		}
		storeSummary, err := validatePogrebStore(path)
		if err != nil {
			return err
		}
		storeSummary.Stores = 1
		summary.add(storeSummary)
		return filepath.SkipDir
	})
	return summary, err
}

func pogrebStoreDirHasSegment(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), pogrebSegmentExt) {
			return true, nil
		}
	}
	return false, nil
}

func pogrebStoreDirHasLock(path string) bool {
	_, err := os.Stat(filepath.Join(path, "lock"))
	return err == nil
}

func validatePogrebStore(path string) (pogrebIntegritySummary, error) {
	var summary pogrebIntegritySummary
	entries, err := os.ReadDir(path)
	if err != nil {
		return summary, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), pogrebSegmentExt) {
			continue
		}
		segmentSummary, err := validatePogrebSegment(filepath.Join(path, entry.Name()))
		if err != nil {
			return summary, err
		}
		summary.add(segmentSummary)
	}
	return summary, nil
}

func validatePogrebSegment(path string) (pogrebIntegritySummary, error) {
	var summary pogrebIntegritySummary
	f, err := os.Open(path)
	if err != nil {
		return summary, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return summary, err
	}
	summary.SegmentFiles = 1
	summary.Bytes = stat.Size()
	if stat.Size() < pogrebHeaderSize {
		return summary, fmt.Errorf("pogreb segment %s is shorter than header: %d bytes", path, stat.Size())
	}

	header := make([]byte, pogrebHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return summary, fmt.Errorf("read pogreb segment header %s: %w", path, err)
	}
	if !bytes.Equal(header[:len(pogrebFileSignature)], pogrebFileSignature) {
		return summary, fmt.Errorf("pogreb segment %s has invalid header signature", path)
	}
	if version := binary.LittleEndian.Uint32(header[8:12]); version != pogrebFormatVersion {
		return summary, fmt.Errorf("pogreb segment %s has unsupported format version %d", path, version)
	}

	offset := int64(pogrebHeaderSize)
	for offset < stat.Size() {
		recordHeader := make([]byte, pogrebRecordHeaderSize)
		n, err := io.ReadFull(f, recordHeader)
		if err != nil {
			return summary, fmt.Errorf("pogreb segment %s has truncated record header at offset %d: read %d bytes: %w", path, offset, n, err)
		}
		keySize := uint32(binary.LittleEndian.Uint16(recordHeader[:2]))
		valueSize := binary.LittleEndian.Uint32(recordHeader[2:]) & pogrebRecordValueSizeMask
		recordSize := int64(pogrebRecordHeaderSize) + int64(keySize) + int64(valueSize) + int64(pogrebRecordChecksumSize)
		if offset+recordSize > stat.Size() {
			return summary, fmt.Errorf("pogreb segment %s has truncated record at offset %d: record_size=%d file_size=%d", path, offset, recordSize, stat.Size())
		}

		hasher := crc32.NewIEEE()
		if _, err := hasher.Write(recordHeader); err != nil {
			return summary, err
		}
		if _, err := io.CopyN(hasher, f, int64(keySize)+int64(valueSize)); err != nil {
			return summary, fmt.Errorf("read pogreb segment %s record payload at offset %d: %w", path, offset, err)
		}
		checksumBytes := make([]byte, pogrebRecordChecksumSize)
		if _, err := io.ReadFull(f, checksumBytes); err != nil {
			return summary, fmt.Errorf("read pogreb segment %s checksum at offset %d: %w", path, offset, err)
		}
		expected := binary.LittleEndian.Uint32(checksumBytes)
		if actual := hasher.Sum32(); actual != expected {
			return summary, fmt.Errorf("pogreb segment %s checksum mismatch at offset %d", path, offset)
		}
		summary.Records++
		offset += recordSize
	}
	return summary, nil
}
