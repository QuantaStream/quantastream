package server

//
// This file contains the bitmap I/O and low level persistance functions for the bitmap server.
//

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	u "github.com/araddon/gou"
)

const (
	bsiBundleFileName         = "bundle"
	bsiBundleMagic            = "QSBIS001"
	bsiPackLeafDir            = "_bsi_pack"
	bsiPackFileName           = "bundle"
	bsiPackMagic              = "QSBIP001"
	standardBundleLeafDir     = "standard"
	standardBundleFileName    = "bundle"
	standardBitmapBundleMagic = "QSSTB001"
)

type standardBitmapBundleEntry struct {
	RowID uint64
	Data  []byte
}

type bsiPackBundleEntry struct {
	Field string
	Data  [][]byte
}

// Partition - Description of partition
type Partition struct {
	Index       string
	Field       string
	RowIDOrBits int64
	Time        time.Time
	TQType      string
	HasStrings  bool
	IsPK        bool // is primary key
	Shard       interface{}
}

// PartitionOperation - Partition operation
type PartitionOperation struct {
	*Partition
	RemoveOnly bool
	newPath    string
}

// NewPartitionOperation - Archival/Removal operations on an entire partition/shard.
func (m *BitmapIndex) NewPartitionOperation(p *Partition, removeOnly bool) *PartitionOperation {

	m.tableCacheLock.RLock()
	defer m.tableCacheLock.RUnlock()
	table := m.tableCache[p.Index]
	if table == nil {
		u.Errorf("NewPartitionOperation: assertion fail table is nil")
		return nil
	}
	pka, err := table.GetPrimaryKeyInfo()
	if err != nil {
		u.Errorf("NewPartitionOperation: assertion fail GetPrimaryKeyInfo: %v", err)
		return nil
	}
	p.IsPK = p.Field == pka[0].FieldName
	attr, err := table.GetAttribute(p.Field)
	if err != nil {
		u.Errorf("assertion fail: %v", err)
	} else {
		p.HasStrings = attr.MappingStrategy == "StringHashBSI"
	}
	return &PartitionOperation{Partition: p, RemoveOnly: removeOnly}
}

// Persist a standard bitmap field to disk
func (m *BitmapIndex) saveCompleteBitmap(bm *StandardBitmap, indexName, fieldName string, rowID int64,
	ts time.Time) error {

	data, err := bm.Bits.MarshalBinary()
	if err != nil {
		return err
	}

	fd, err := m.openCompleteFile(indexName, fieldName, rowID, ts, bm.TQType)
	if err == nil {
		if _, err := fd[0].Write(data); err != nil {
			return err
		}
		if err := fd[0].Close(); err != nil {
			return err
		}
		return nil
	}
	return err
}

func (m *BitmapIndex) saveCompleteStandardBundle(bitmaps map[uint64]*StandardBitmap, indexName, fieldName string,
	ts time.Time, tqType string) (int, error) {

	if len(bitmaps) == 0 {
		return 0, fmt.Errorf("cannot persist empty standard bitmap bundle")
	}
	rowIDs := make([]uint64, 0, len(bitmaps))
	for rowID := range bitmaps {
		rowIDs = append(rowIDs, rowID)
	}
	sort.Slice(rowIDs, func(i, j int) bool { return rowIDs[i] < rowIDs[j] })

	entries := make([]standardBitmapBundleEntry, 0, len(rowIDs))
	capturedModTimes := make(map[uint64]time.Time, len(rowIDs))
	for _, rowID := range rowIDs {
		bitmap := bitmaps[rowID]
		if bitmap == nil || bitmap.Bits == nil {
			continue
		}
		bitmap.Lock.RLock()
		data, err := bitmap.Bits.MarshalBinary()
		modTime := bitmap.ModTime
		bitmap.Lock.RUnlock()
		if err != nil {
			return 0, err
		}
		entries = append(entries, standardBitmapBundleEntry{RowID: rowID, Data: data})
		capturedModTimes[rowID] = modTime
	}
	if len(entries) == 0 {
		return 0, fmt.Errorf("cannot persist standard bitmap bundle with no bitmap entries")
	}

	bundle, err := encodeStandardBitmapBundle(entries)
	if err != nil {
		return 0, err
	}
	_, bundlePath := m.standardBitmapBundleFilePath(indexName, fieldName, ts, tqType)
	if err := writeAtomicBundleFile(bundlePath, bundle, 0666); err != nil {
		return 0, err
	}
	if err := m.removeLegacyStandardBitmapShardFiles(indexName, fieldName, ts, tqType); err != nil {
		return 0, err
	}

	persistedAt := time.Now()
	for _, entry := range entries {
		bitmap := bitmaps[entry.RowID]
		if bitmap == nil {
			continue
		}
		bitmap.Lock.Lock()
		if !bitmap.ModTime.After(capturedModTimes[entry.RowID]) {
			bitmap.PersistTime = persistedAt
		}
		bitmap.Lock.Unlock()
	}
	return len(entries), nil
}

func (m *BitmapIndex) standardBitmapBundleFilePath(indexName, fieldName string, ts time.Time, tqType string) (string, string) {
	return m.standardBitmapBundleFilePathWithCreate(indexName, fieldName, ts, tqType, true)
}

func (m *BitmapIndex) standardBitmapBundleFilePathWithCreate(indexName, fieldName string, ts time.Time, tqType string, create bool) (string, string) {
	dir := filepath.Join(m.dataDir, "bitmap", indexName, fieldName, standardBundleLeafDir)
	shard := "default"
	switch tqType {
	case "YMD":
		shard = formatShardTime(ts)
	case "YMDH":
		utcTime := ts.UTC()
		dir = filepath.Join(dir, fmt.Sprintf("%d%02d%02d", utcTime.Year(), utcTime.Month(), utcTime.Day()))
		shard = formatShardTime(ts)
	}
	dir = filepath.Join(dir, shard)
	if create {
		_ = os.MkdirAll(dir, 0755)
	}
	return dir, filepath.Join(dir, standardBundleFileName)
}

func encodeStandardBitmapBundle(entries []standardBitmapBundleEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("cannot encode empty standard bitmap bundle")
	}
	var buf bytes.Buffer
	if _, err := buf.WriteString(standardBitmapBundleMagic); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(entries))); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if err := binary.Write(&buf, binary.BigEndian, entry.RowID); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint64(len(entry.Data))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(entry.Data); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeStandardBitmapBundle(data []byte) ([]standardBitmapBundleEntry, error) {
	reader := bytes.NewReader(data)
	magic := make([]byte, len(standardBitmapBundleMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return nil, fmt.Errorf("read standard bitmap bundle magic: %w", err)
	}
	if string(magic) != standardBitmapBundleMagic {
		return nil, fmt.Errorf("invalid standard bitmap bundle magic %q", string(magic))
	}
	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("read standard bitmap bundle entry count: %w", err)
	}
	if count == 0 || count > 1<<24 {
		return nil, fmt.Errorf("invalid standard bitmap bundle entry count %d", count)
	}
	entries := make([]standardBitmapBundleEntry, 0, count)
	seen := make(map[uint64]struct{}, count)
	for i := uint32(0); i < count; i++ {
		var rowID uint64
		if err := binary.Read(reader, binary.BigEndian, &rowID); err != nil {
			return nil, fmt.Errorf("read standard bitmap bundle row ID %d: %w", i, err)
		}
		if _, ok := seen[rowID]; ok {
			return nil, fmt.Errorf("duplicate standard bitmap bundle row ID %d", rowID)
		}
		seen[rowID] = struct{}{}
		var dataLen uint64
		if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
			return nil, fmt.Errorf("read standard bitmap bundle length %d: %w", i, err)
		}
		if dataLen > uint64(reader.Len()) {
			return nil, fmt.Errorf("standard bitmap bundle row %d length %d exceeds remaining %d", rowID, dataLen, reader.Len())
		}
		entryData := make([]byte, dataLen)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return nil, fmt.Errorf("read standard bitmap bundle row %d: %w", rowID, err)
		}
		entries = append(entries, standardBitmapBundleEntry{RowID: rowID, Data: entryData})
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("standard bitmap bundle has %d trailing bytes", reader.Len())
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].RowID < entries[j].RowID })
	return entries, nil
}

func (m *BitmapIndex) removeLegacyStandardBitmapShardFiles(indexName, fieldName string, ts time.Time, tqType string) error {
	fieldDir := filepath.Join(m.dataDir, "bitmap", indexName, fieldName)
	entries, err := os.ReadDir(fieldDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "bsi" || entry.Name() == standardBundleLeafDir {
			continue
		}
		if _, err := strconv.ParseUint(entry.Name(), 10, 64); err != nil {
			continue
		}
		for _, path := range legacyStandardBitmapShardPaths(fieldDir, entry.Name(), ts, tqType) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			removeEmptyParents(path, fieldDir)
		}
	}
	return nil
}

func legacyStandardBitmapShardPaths(fieldDir, rowID string, ts time.Time, tqType string) []string {
	shard := formatShardTime(ts)
	if tqType == "YMDH" {
		utcTime := ts.UTC()
		return []string{filepath.Join(fieldDir, rowID, fmt.Sprintf("%d%02d%02d", utcTime.Year(), utcTime.Month(), utcTime.Day()), shard)}
	}
	paths := []string{filepath.Join(fieldDir, rowID, shard)}
	if tqType == "" {
		paths = append(paths, filepath.Join(fieldDir, rowID, "default"))
	}
	return paths
}

func removeEmptyParents(path, stopDir string) {
	dir := filepath.Dir(path)
	stopDir = filepath.Clean(stopDir)
	for dir != "." && dir != string(filepath.Separator) && filepath.Clean(dir) != stopDir {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// Persist a BSI field to disk
func (m *BitmapIndex) saveCompleteBSI(bsi *BSIBitmap, indexName, fieldName string, _ int,
	ts time.Time) error {
	_, err := m.saveCompleteBSIWithTimings(bsi, indexName, fieldName, ts)
	return err
}

type bsiBundlePersistTimings struct {
	marshalElapsed   time.Duration
	encodeElapsed    time.Duration
	pathElapsed      time.Duration
	fileWriteElapsed time.Duration
	cleanupElapsed   time.Duration
	chunkCount       int
	chunkBytes       uint64
	bundleBytes      uint64
}

func (m *BitmapIndex) saveCompleteBSIWithTimings(bsi *BSIBitmap, indexName, fieldName string,
	ts time.Time) (bsiBundlePersistTimings, error) {
	var timings bsiBundlePersistTimings

	marshalStart := time.Now()
	data, err := bsi.MarshalBinary()
	timings.marshalElapsed = time.Since(marshalStart)
	if err != nil {
		return timings, err
	}
	timings.chunkCount = len(data)
	for _, chunk := range data {
		timings.chunkBytes += uint64(len(chunk))
	}

	encodeStart := time.Now()
	bundle, err := encodeBSIBundle(data)
	timings.encodeElapsed = time.Since(encodeStart)
	if err != nil {
		return timings, err
	}
	timings.bundleBytes = uint64(len(bundle))

	pathStart := time.Now()
	dir, bundlePath := m.bsiBundleFilePath(indexName, fieldName, ts, bsi.TQType)
	timings.pathElapsed = time.Since(pathStart)
	fileWriteStart := time.Now()
	if err := writeAtomicBundleFile(bundlePath, bundle, 0666); err != nil {
		timings.fileWriteElapsed = time.Since(fileWriteStart)
		return timings, err
	}
	timings.fileWriteElapsed = time.Since(fileWriteStart)

	cleanupStart := time.Now()
	err = removeLegacyBSISliceFiles(dir)
	timings.cleanupElapsed = time.Since(cleanupStart)
	return timings, err
}

func (m *BitmapIndex) saveCompleteBSIPackWithTimings(bsis map[string]*BSIBitmap, indexName string,
	ts time.Time, tqType string) (bsiBundlePersistTimings, error) {
	var timings bsiBundlePersistTimings
	if len(bsis) == 0 {
		return timings, fmt.Errorf("cannot persist empty BSI pack")
	}
	fields := make([]string, 0, len(bsis))
	for fieldName := range bsis {
		fields = append(fields, fieldName)
	}
	sort.Strings(fields)

	entries := make([]bsiPackBundleEntry, 0, len(fields))
	capturedModTimes := make(map[string]time.Time, len(fields))
	for _, fieldName := range fields {
		bsi := bsis[fieldName]
		if bsi == nil || bsi.BSI == nil {
			continue
		}
		bsi.Lock.RLock()
		marshalStart := time.Now()
		data, err := bsi.MarshalBinary()
		timings.marshalElapsed += time.Since(marshalStart)
		modTime := bsi.ModTime
		bsi.Lock.RUnlock()
		if err != nil {
			return timings, err
		}
		timings.chunkCount += len(data)
		for _, chunk := range data {
			timings.chunkBytes += uint64(len(chunk))
		}
		entries = append(entries, bsiPackBundleEntry{Field: fieldName, Data: data})
		capturedModTimes[fieldName] = modTime
	}
	if len(entries) == 0 {
		return timings, fmt.Errorf("cannot persist BSI pack with no entries")
	}

	encodeStart := time.Now()
	pack, err := encodeBSIPackBundle(entries)
	timings.encodeElapsed = time.Since(encodeStart)
	if err != nil {
		return timings, err
	}
	timings.bundleBytes = uint64(len(pack))

	pathStart := time.Now()
	_, packPath := m.bsiPackBundleFilePath(indexName, ts, tqType)
	timings.pathElapsed = time.Since(pathStart)

	fileWriteStart := time.Now()
	if err := writeAtomicBundleFile(packPath, pack, 0666); err != nil {
		timings.fileWriteElapsed = time.Since(fileWriteStart)
		return timings, err
	}
	timings.fileWriteElapsed = time.Since(fileWriteStart)

	persistedAt := time.Now()
	for _, fieldName := range fields {
		bsi := bsis[fieldName]
		if bsi == nil {
			continue
		}
		captured, ok := capturedModTimes[fieldName]
		if !ok {
			continue
		}
		bsi.Lock.Lock()
		if !bsi.ModTime.After(captured) {
			bsi.PersistTime = persistedAt
		}
		bsi.Lock.Unlock()
	}
	return timings, nil
}

func (m *BitmapIndex) bsiBundleFilePath(indexName, fieldName string, ts time.Time, tqType string) (string, string) {
	partition := &Partition{Index: indexName, Field: fieldName, Time: ts, TQType: tqType, RowIDOrBits: -1}
	dir := m.generateBitmapFilePath(partition, false)
	return dir, filepath.Join(dir, bsiBundleFileName)
}

func (m *BitmapIndex) bsiPackBundleFilePath(indexName string, ts time.Time, tqType string) (string, string) {
	return m.bsiPackBundleFilePathWithCreate(indexName, ts, tqType, true)
}

func (m *BitmapIndex) bsiPackBundleFilePathWithCreate(indexName string, ts time.Time, tqType string, create bool) (string, string) {
	dir := filepath.Join(m.dataDir, "bitmap", indexName, bsiPackLeafDir)
	shard := "default"
	switch tqType {
	case "YMD":
		shard = formatShardTime(ts)
	case "YMDH":
		utcTime := ts.UTC()
		dir = filepath.Join(dir, fmt.Sprintf("%d%02d%02d", utcTime.Year(), utcTime.Month(), utcTime.Day()))
		shard = formatShardTime(ts)
	}
	dir = filepath.Join(dir, shard)
	if create {
		_ = os.MkdirAll(dir, 0755)
	}
	return dir, filepath.Join(dir, bsiPackFileName)
}

func isBSIPackBundlePath(parts []string) bool {
	return len(parts) >= 4 && parts[1] == bsiPackLeafDir && parts[len(parts)-1] == bsiPackFileName
}

func bsiPackShardTimeFromPathParts(parts []string) (time.Time, error) {
	if !isBSIPackBundlePath(parts) {
		return time.Time{}, fmt.Errorf("not a BSI pack bundle path")
	}
	shard := parts[len(parts)-2]
	if shard == "default" {
		return time.Unix(0, 0), nil
	}
	ts, err := time.Parse(timeFmt, shard)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse BSI pack shard time %s: %w", shard, err)
	}
	return ts, nil
}

func encodeBSIBundle(chunks [][]byte) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, fmt.Errorf("cannot encode empty BSI bundle")
	}
	var buf bytes.Buffer
	if _, err := buf.WriteString(bsiBundleMagic); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(chunks))); err != nil {
		return nil, err
	}
	for _, chunk := range chunks {
		if err := binary.Write(&buf, binary.BigEndian, uint64(len(chunk))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(chunk); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeBSIBundle(data []byte) ([][]byte, error) {
	reader := bytes.NewReader(data)
	magic := make([]byte, len(bsiBundleMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return nil, fmt.Errorf("read BSI bundle magic: %w", err)
	}
	if string(magic) != bsiBundleMagic {
		return nil, fmt.Errorf("invalid BSI bundle magic %q", string(magic))
	}
	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("read BSI bundle chunk count: %w", err)
	}
	if count == 0 || count > 1<<20 {
		return nil, fmt.Errorf("invalid BSI bundle chunk count %d", count)
	}
	chunks := make([][]byte, count)
	for i := uint32(0); i < count; i++ {
		var chunkLen uint64
		if err := binary.Read(reader, binary.BigEndian, &chunkLen); err != nil {
			return nil, fmt.Errorf("read BSI bundle chunk length %d: %w", i, err)
		}
		if chunkLen > uint64(reader.Len()) {
			return nil, fmt.Errorf("BSI bundle chunk %d length %d exceeds remaining %d", i, chunkLen, reader.Len())
		}
		chunks[i] = make([]byte, chunkLen)
		if _, err := io.ReadFull(reader, chunks[i]); err != nil {
			return nil, fmt.Errorf("read BSI bundle chunk %d: %w", i, err)
		}
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("BSI bundle has %d trailing bytes", reader.Len())
	}
	return chunks, nil
}

func encodeBSIPackBundle(entries []bsiPackBundleEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("cannot encode empty BSI pack bundle")
	}
	ordered := append([]bsiPackBundleEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Field < ordered[j].Field })

	var buf bytes.Buffer
	if _, err := buf.WriteString(bsiPackMagic); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(ordered))); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(ordered))
	for _, entry := range ordered {
		if entry.Field == "" {
			return nil, fmt.Errorf("cannot encode BSI pack entry with empty field")
		}
		if _, ok := seen[entry.Field]; ok {
			return nil, fmt.Errorf("duplicate BSI pack field %s", entry.Field)
		}
		seen[entry.Field] = struct{}{}
		field := []byte(entry.Field)
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(field))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(field); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(entry.Data))); err != nil {
			return nil, err
		}
		for _, chunk := range entry.Data {
			if err := binary.Write(&buf, binary.BigEndian, uint64(len(chunk))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(chunk); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func decodeBSIPackBundle(data []byte) ([]bsiPackBundleEntry, error) {
	reader := bytes.NewReader(data)
	magic := make([]byte, len(bsiPackMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return nil, fmt.Errorf("read BSI pack magic: %w", err)
	}
	if string(magic) != bsiPackMagic {
		return nil, fmt.Errorf("invalid BSI pack magic %q", string(magic))
	}
	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("read BSI pack entry count: %w", err)
	}
	if count == 0 || count > 1<<20 {
		return nil, fmt.Errorf("invalid BSI pack entry count %d", count)
	}
	entries := make([]bsiPackBundleEntry, 0, count)
	seen := make(map[string]struct{}, count)
	for i := uint32(0); i < count; i++ {
		var fieldLen uint32
		if err := binary.Read(reader, binary.BigEndian, &fieldLen); err != nil {
			return nil, fmt.Errorf("read BSI pack field length %d: %w", i, err)
		}
		if fieldLen == 0 || fieldLen > uint32(reader.Len()) {
			return nil, fmt.Errorf("invalid BSI pack field length %d", fieldLen)
		}
		fieldBytes := make([]byte, fieldLen)
		if _, err := io.ReadFull(reader, fieldBytes); err != nil {
			return nil, fmt.Errorf("read BSI pack field %d: %w", i, err)
		}
		field := string(fieldBytes)
		if _, ok := seen[field]; ok {
			return nil, fmt.Errorf("duplicate BSI pack field %s", field)
		}
		seen[field] = struct{}{}
		var chunkCount uint32
		if err := binary.Read(reader, binary.BigEndian, &chunkCount); err != nil {
			return nil, fmt.Errorf("read BSI pack chunk count %s: %w", field, err)
		}
		if chunkCount == 0 || chunkCount > 1<<20 {
			return nil, fmt.Errorf("invalid BSI pack chunk count %d for field %s", chunkCount, field)
		}
		chunks := make([][]byte, chunkCount)
		for j := uint32(0); j < chunkCount; j++ {
			var chunkLen uint64
			if err := binary.Read(reader, binary.BigEndian, &chunkLen); err != nil {
				return nil, fmt.Errorf("read BSI pack chunk length %s[%d]: %w", field, j, err)
			}
			if chunkLen > uint64(reader.Len()) {
				return nil, fmt.Errorf("BSI pack field %s chunk %d length %d exceeds remaining %d",
					field, j, chunkLen, reader.Len())
			}
			chunks[j] = make([]byte, chunkLen)
			if _, err := io.ReadFull(reader, chunks[j]); err != nil {
				return nil, fmt.Errorf("read BSI pack chunk %s[%d]: %w", field, j, err)
			}
		}
		entries = append(entries, bsiPackBundleEntry{Field: field, Data: chunks})
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("BSI pack has %d trailing bytes", reader.Len())
	}
	return entries, nil
}

func findBSIPackBundleEntry(entries []bsiPackBundleEntry, field string) (bsiPackBundleEntry, bool) {
	for _, entry := range entries {
		if entry.Field == field {
			return entry, true
		}
	}
	return bsiPackBundleEntry{}, false
}

func writeAtomicBundleFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func removeLegacyBSISliceFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == bsiBundleFileName || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Move data from active use to the archive directory path
func (m *BitmapIndex) executeOperation(aop *PartitionOperation) error {

	oldPath := m.generateBitmapFilePath(aop.Partition, false)
	newPath := m.generateBitmapFilePath(aop.Partition, true)
	aop.newPath = newPath

	if err := filepath.Walk(oldPath, aop.perform); err != nil {
		return err
	}
	if aop.RowIDOrBits >= 0 {
		return nil
	}
	localKV := m.Node.GetNodeService("KVStore").(*KVStore)
	if aop.HasStrings {
		var iname string
		oldPath, iname = m.generateStringsFilePath(aop, false)
		localKV.closeStore(iname)
		aop.newPath, _ = m.generateStringsFilePath(aop, true)
		os.MkdirAll(aop.newPath, 0755)
		if err := filepath.Walk(oldPath, aop.perform); err != nil {
			return err
		}
	} else {
		if aop.IsPK {
			var iname string
			oldPath, iname = m.generateIndexFilePath(aop, false, 0)
			localKV.closeStore(iname)
			aop.newPath, _ = m.generateIndexFilePath(aop, true, 0)
			os.MkdirAll(aop.newPath, 0755)
			if err := filepath.Walk(oldPath, aop.perform); err != nil {
				return err
			}
		}
	}
	return nil
}

func (po *PartitionOperation) perform(path string, info os.FileInfo, err error) error {

	if info == nil {
		err := fmt.Errorf("assert info is nil for path %s", path)
		u.Errorf("%v", err)
		return err
	}

	if info.IsDir() {
		//u.Warn("Partition operation not allowed against directory [%v]", path)
		return nil
	}
	if po.RemoveOnly {
		u.Infof("Partition remove only for [%v]", path)
		return os.Remove(path)
	}
	dest := po.newPath + sep + info.Name()
	err2 := os.Rename(path, dest)
	if err2 == nil {
		u.Infof("Partition rename move [%v] to [%v]", path, dest)
		return nil
	}
	if !strings.HasSuffix(err2.Error(), "invalid cross-device link") {
		return err2
	}
	input, err3 := ioutil.ReadFile(path)
	if err3 != nil {
		return err3
	}
	err4 := ioutil.WriteFile(dest, input, 0644)
	if err4 != nil {
		return err4
	}
	u.Infof("Partition copy move [%v] to [%v]", path, dest)
	return os.Remove(path)
}

func (p *Partition) generatePath(isArchivePath bool, base, leaf string) (string, string) {

	baseDir := base + sep + "index" + sep + p.Index + sep + p.Field + sep + leaf
	baseWithDest := base + sep + "index" + sep
	if isArchivePath {
		baseDir = base + sep + "archive" + sep + p.Index + sep + p.Field + sep + leaf
		baseWithDest = base + sep + "archive" + sep
	}

	fname := "default"
	dayDir := ""
	if p.TQType == "YMD" {
		fname = formatShardTime(p.Time)
	}
	if p.TQType == "YMDH" {
		utcTime := p.Time.UTC()
		dayDir = fmt.Sprintf("%d%02d%02d", utcTime.Year(), utcTime.Month(), utcTime.Day()) + "/"
		fname = formatShardTime(p.Time)
	}
	ret := baseDir + sep + dayDir + fname
	return ret, strings.ReplaceAll(ret, baseWithDest, "")
}

// Figure out the appropriate file path given type BSI/Standard and applicable time quantum
func (m *BitmapIndex) generateBitmapFilePath(aop *Partition, isArchivePath bool) string {
	return m.generateBitmapFilePathWithCreate(aop, isArchivePath, true)
}

func (m *BitmapIndex) generateBitmapFilePathWithCreate(aop *Partition, isArchivePath bool, create bool) string {

	// field is a BSI if rowIDOrBits < 0
	leafDir := "bsi"
	if aop.RowIDOrBits >= 0 {
		leafDir = fmt.Sprintf("%d", aop.RowIDOrBits)
	}
	baseDir := m.dataDir + sep + "bitmap" + sep + aop.Index + sep + aop.Field + sep + leafDir
	if isArchivePath {
		baseDir = m.dataDir + sep + "archive" + sep + aop.Index + sep + aop.Field + sep + leafDir
	}
	fname := "default"
	if aop.TQType == "YMD" {
		fname = formatShardTime(aop.Time)
	}
	if aop.TQType == "YMDH" {
		utcTime := aop.Time.UTC()
		baseDir = baseDir + sep + fmt.Sprintf("%d%02d%02d", utcTime.Year(), utcTime.Month(), utcTime.Day())
		fname = formatShardTime(aop.Time)
	}
	if leafDir == "bsi" {
		baseDir = baseDir + sep + fname
		fname = ""
	}
	if create {
		os.MkdirAll(baseDir, 0755)
	}
	//return baseDir + sep + fname
	return baseDir
}

// Figure out the appropriate file path for backing strings file
func (m *BitmapIndex) generateStringsFilePath(aop *PartitionOperation, isArchivePath bool) (string, string) {

	return aop.generatePath(isArchivePath, m.dataDir, "strings")
}

// Figure out the appropriate file path for an index file (PK/SK)
// Index number 0 is PK, index number 1 is first SK (if any)  and so forth
func (m *BitmapIndex) generateIndexFilePath(aop *PartitionOperation, isArchivePath bool, indexNo int) (string, string) {

	m.tableCacheLock.RLock()
	defer m.tableCacheLock.RUnlock()
	table := m.tableCache[aop.Index]
	if table == nil {
		u.Errorf("generateIndexFilePath: assertion fail table is nil")
		return "", ""
	}
	name := table.PrimaryKey + ".PK"
	if indexNo > 0 {
		s := strings.Split(table.SecondaryKeys, ",")
		if indexNo > len(s) {
			u.Errorf("generateIndexFilePath: indexNo is invalid")
			return "", ""
		}
		name = s[indexNo-1] + ".SK"
	}
	return aop.generatePath(isArchivePath, m.dataDir, name)
}

// Return open file descriptor(s) for writing
func (m *BitmapIndex) openCompleteFile(index, field string, rowIDOrBits int64, ts time.Time,
	tqType string) ([]*os.File, error) {

	// if the bitmap file is a BSI (rowidOrBits < 0) then return an array of open file handles in low
	// to high bit significance order.  For BSI, RowIDOrBits is the number of bits as a negative value.
	// For StandardBitmap just use this value as rowID
	operation := &Partition{Index: index, Field: field, Time: ts, TQType: tqType, RowIDOrBits: rowIDOrBits}
	path := m.generateBitmapFilePath(operation, false)
	var err error
	f := make([]*os.File, 1)
	numFiles := 1
	i := 0
	if rowIDOrBits < 0 {
		// Open numfiles + 1 (extra one for EBM)
		numFiles = int(rowIDOrBits*-1) + 1
		i = 1
		f = make([]*os.File, numFiles)
		// EBM is at fd[0]
		f[0], err = os.OpenFile(path+sep+"EBM", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	} else {
		path = path + sep + formatShardTime(ts)
	}
	for ; i < numFiles; i++ {
		fpath := path
		if numFiles > 1 {
			fpath = path + sep + fmt.Sprintf("%d", i)
		}
		f[i], err = os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Called during server startup.  Iterates filesystem and loads up fragement queue.
func (m *BitmapIndex) readBitmapFiles(fragQueue chan *BitmapFragment) error {

	start := time.Now()
	m.fragFileLock.Lock()
	defer m.fragFileLock.Unlock()

	baseDir := m.dataDir + sep + "bitmap"

	if useBitmapShardManifestEnabled() {
		manifest, observation := m.loadAndObserveBitmapShardManifest(nil)
		if observation.Status == "ok" {
			if err := m.readBitmapFilesFromManifest(manifest, observation, fragQueue, start); err == nil {
				return nil
			} else {
				u.Warnf("BitmapIndex manifest startup load failed; falling back to slow scan: %v", err)
			}
		} else {
			fmt.Printf("BitmapIndex startup manifest opt_in=true load_source=slow_scan manifest_status=%s manifest_detail=%q manifest_entries=%d manifest_files=%d manifest_missing_files=%d manifest_observe_elapsed=%v\n",
				observation.Status, observation.Detail, observation.ManifestEntries, observation.ManifestFiles, observation.MissingFileCount, observation.Elapsed)
		}
	}

	var fragMap = newBitmapStartupFragmentMap()
	fileCount := 0
	standardFileCount := 0
	bsiFileCount := 0
	ignoredFieldCount := 0
	manifestBuilder := newBitmapShardManifestBuilder(m.dataDir)

	walkStart := time.Now()
	err := filepath.Walk(baseDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if strings.HasSuffix(info.Name(), ".tmp") {
				return nil
			}
			fileCount++
			if fileCount%50000 == 0 {
				fmt.Printf("BitmapIndex startup scan progress files=%d standard_files=%d bsi_files=%d ignored_fields=%d elapsed=%v\n",
					fileCount, standardFileCount, bsiFileCount, ignoredFieldCount, time.Since(walkStart))
			}
			bf := &BitmapFragment{ModTime: info.ModTime(), IsInit: true}

			data, err := ioutil.ReadFile(path)
			if err != nil {
				u.Errorf("readBitmapFiles: ioutil.ReadFile - %v", err)
				return err
			}

			trPath := strings.Replace(path, baseDir+sep, "", 1)

			s := strings.Split(trPath, sep)
			if len(s) < 4 {
				err := fmt.Errorf("readBitmapFiles: Could not parse path [%s]", path)
				u.Error(err)
				return err
			}
			bf.IndexName = s[0]
			bf.FieldName = s[1]
			if isBSIPackBundlePath(s) {
				shardTime, err := bsiPackShardTimeFromPathParts(s)
				if err != nil {
					return fmt.Errorf("readBitmapFiles: parse BSI pack %s: %w", path, err)
				}
				entries, err := decodeBSIPackBundle(data)
				if err != nil {
					return fmt.Errorf("readBitmapFiles: decode BSI pack %s: %w", path, err)
				}
				bsiFileCount++
				for _, entry := range entries {
					if _, err := m.getFieldConfig(bf.IndexName, entry.Field); err != nil {
						u.Errorf("Attribute %s.%s not found in schema. ignoring", bf.IndexName, entry.Field)
						ignoredFieldCount++
						continue
					}
					manifestBuilder.addBSIPackFile(path, info, bf.IndexName, entry.Field, shardTime)
					if err := fragMap.add(&BitmapFragment{
						IndexName:   bf.IndexName,
						FieldName:   entry.Field,
						RowIDOrBits: -1,
						Time:        shardTime,
						BitData:     entry.Data,
						ModTime:     info.ModTime(),
						IsBSI:       true,
						IsInit:      true,
					}); err != nil {
						return err
					}
				}
				return nil
			}
			attr, err := m.getFieldConfig(bf.IndexName, bf.FieldName)
			if err != nil {
				u.Errorf("Attribute %s.%s not found in schema. ignoring", bf.IndexName, bf.FieldName)
				ignoredFieldCount++
				return nil
			}

			var tq string
			if attr != nil {
				tq = attr.TimeQuantumType
			}
			bf.IsBSI = isBSIBitmapPath(s)

			if bf.IsBSI {
				bsiFileCount++
				if tq != "" {
					ts, err := time.Parse(timeFmt, s[len(s)-2])
					if err != nil {
						err := fmt.Errorf("readBitmapFiles: %s[%s] Could not parse '%s' Time[%s] - %v",
							bf.IndexName, bf.FieldName, s[len(s)-2], tq, err)
						u.Error(err)
						return err
					}
					bf.Time = ts
				} else if s[len(s)-2] == "default" {
					bf.Time = time.Unix(0, 0)
				}
				if s[len(s)-1] == bsiBundleFileName {
					chunks, err := decodeBSIBundle(data)
					if err != nil {
						return fmt.Errorf("readBitmapFiles: decode BSI bundle %s: %w", path, err)
					}
					manifestBuilder.addBSIBundleFile(path, info, bf.IndexName, bf.FieldName, bf.Time)
					if err := fragMap.add(&BitmapFragment{
						IndexName:   bf.IndexName,
						FieldName:   bf.FieldName,
						RowIDOrBits: -1,
						Time:        bf.Time,
						BitData:     chunks,
						ModTime:     info.ModTime(),
						IsBSI:       true,
						IsInit:      true,
					}); err != nil {
						return err
					}
					return nil
				}
				bitSliceIndex := -1
				if s[len(s)-1] == "EBM" {
					bitSliceIndex = 0
				} else {
					val, err := strconv.ParseInt(s[len(s)-1], 10, 64)
					if err != nil {
						err := fmt.Errorf("readBitmapFiles: Could not parse BSI Bit file - %v", err)
						u.Error(err)
						return err
					}
					bitSliceIndex = int(val)
				}

				if _, ok := fragMap[bf.IndexName]; !ok {
					fragMap[bf.IndexName] = make(map[string]map[int64]map[int64]*BitmapFragment)
				}
				if _, ok := fragMap[bf.IndexName][bf.FieldName]; !ok {
					fragMap[bf.IndexName][bf.FieldName] = make(map[int64]map[int64]*BitmapFragment)
				}
				if _, ok := fragMap[bf.IndexName][bf.FieldName][int64(-1)]; !ok {
					fragMap[bf.IndexName][bf.FieldName][int64(-1)] = make(map[int64]*BitmapFragment)
				}
				manifestBuilder.addBSIFile(path, info, bf.IndexName, bf.FieldName, bf.Time, bitSliceIndex)
				if existFrag, ok := fragMap[bf.IndexName][bf.FieldName][int64(-1)][bf.Time.UnixNano()]; !ok {
					if bitSliceIndex == -1 {
						err := fmt.Errorf("readBitmapFiles: Should not be here bitslice must be zero here")
						u.Error(err)
						return err
					}
					// first bitslice start at bf.BitData[1].  bf.BitData[0] = EBM
					bf.BitData = make([][]byte, 2)
					for bitSliceIndex >= len(bf.BitData) {
						bf.BitData = append(bf.BitData, make([]byte, 0))
					}
					bf.BitData[bitSliceIndex] = data
					fragMap[bf.IndexName][bf.FieldName][int64(-1)][bf.Time.UnixNano()] = bf
				} else {
					// merge in new bits
					for bitSliceIndex >= len(existFrag.BitData) {
						existFrag.BitData = append(existFrag.BitData, make([]byte, 0))
					}
					existFrag.BitData[bitSliceIndex] = data
					existFrag.ModTime = info.ModTime().Add(time.Second * -10)
				}
			} else {
				standardFileCount++
				if isStandardBitmapBundlePath(s) {
					if s[len(s)-2] == "default" {
						bf.Time = time.Unix(0, 0)
					} else {
						ts, err := time.Parse(timeFmt, s[len(s)-2])
						if err != nil {
							err := fmt.Errorf("readBitmapFiles: %s[%s] Could not parse standard bundle Time[%s] - %v",
								bf.IndexName, bf.FieldName, s[len(s)-2], err)
							u.Error(err)
							return err
						}
						bf.Time = ts
					}
					entries, err := decodeStandardBitmapBundle(data)
					if err != nil {
						return fmt.Errorf("readBitmapFiles: decode standard bitmap bundle %s: %w", path, err)
					}
					manifestBuilder.addStandardBundleFile(path, info, bf.IndexName, bf.FieldName, bf.Time)
					for _, entry := range entries {
						if err := fragMap.add(&BitmapFragment{
							IndexName:   bf.IndexName,
							FieldName:   bf.FieldName,
							RowIDOrBits: int64(entry.RowID),
							Time:        bf.Time,
							BitData:     [][]byte{entry.Data},
							ModTime:     info.ModTime(),
							IsInit:      true,
						}); err != nil {
							return err
						}
					}
					return nil
				}
				bf.RowIDOrBits, err = strconv.ParseInt(s[2], 10, 64)
				if err != nil {
					err := fmt.Errorf("readBitmapFiles: Could not parse RowID - %v", err)
					u.Error(err)
					return err
				}
				if s[len(s)-1] == "default" {
					bf.Time = time.Unix(0, 0)
				} else {
					ts, err := time.Parse(timeFmt, s[len(s)-1])
					if err != nil {
						err := fmt.Errorf("readBitmapFiles: %s[%s] Could not parse '%s' - %v",
							bf.IndexName, bf.FieldName, s[len(s)-1], err)
						u.Error(err)
						return err
					}
					bf.Time = ts
				}
				if _, ok := fragMap[bf.IndexName]; !ok {
					fragMap[bf.IndexName] = make(map[string]map[int64]map[int64]*BitmapFragment)
				}
				if _, ok := fragMap[bf.IndexName][bf.FieldName]; !ok {
					fragMap[bf.IndexName][bf.FieldName] = make(map[int64]map[int64]*BitmapFragment)
				}
				rID := bf.RowIDOrBits
				if _, ok := fragMap[bf.IndexName][bf.FieldName][rID]; !ok {
					fragMap[bf.IndexName][bf.FieldName][rID] = make(map[int64]*BitmapFragment)
				}
				manifestBuilder.addStandardFile(path, info, bf.IndexName, bf.FieldName, rID, bf.Time)
				if _, ok := fragMap[bf.IndexName][bf.FieldName][rID][bf.Time.UnixNano()]; !ok {
					bf.BitData = [][]byte{data}
					fragMap[bf.IndexName][bf.FieldName][rID][bf.Time.UnixNano()] = bf
				} else {
					err := fmt.Errorf("readBitmapFiles: Should not be here for standard bitmaps! [%s/%s]",
						bf.IndexName, bf.FieldName)
					u.Error(err)
					return err
				}
			}
			return nil
		})
	walkElapsed := time.Since(walkStart)
	if err != nil {
		u.Errorf("filepath.Walk - %v", err)
		return err
	}
	manifest := manifestBuilder.manifest(time.Now().UTC(), "startup_slow_scan")
	manifestObservation := m.observeBitmapShardManifest(manifest)
	manifestWriteStart := time.Now()
	manifestWriteElapsed := time.Duration(0)
	if err := m.saveBitmapShardManifest(manifest); err != nil {
		u.Warnf("BitmapIndex startup manifest write failed: %v", err)
	} else {
		manifestWriteElapsed = time.Since(manifestWriteStart)
	}

	if len(fragMap) == 0 {
		fmt.Printf("BitmapIndex startup load path=%s files=%d standard_files=%d bsi_files=%d ignored_fields=%d manifest_status=%s manifest_detail=%q manifest_entries=%d manifest_files=%d manifest_scan_entries=%d manifest_scan_files=%d manifest_missing_files=%d manifest_observe_elapsed=%v manifest_write_elapsed=%v fragments=0 walk_elapsed=%v enqueue_elapsed=0s flush_elapsed=0s total_elapsed=%v\n",
			baseDir, fileCount, standardFileCount, bsiFileCount, ignoredFieldCount, manifestObservation.Status, manifestObservation.Detail, manifestObservation.ManifestEntries, manifestObservation.ManifestFiles, manifestObservation.ScanEntries, manifestObservation.ScanFiles, manifestObservation.MissingFileCount, manifestObservation.Elapsed, manifestWriteElapsed, walkElapsed, time.Since(start))
		return nil
	}

	enqueueStart := time.Now()
	fragmentCount, standardFragmentCount, bsiFragmentCount, err := enqueueBitmapStartupFragments(fragMap, fragQueue)
	if err != nil {
		return err
	}
	enqueueElapsed := time.Since(enqueueStart)
	flushStart := time.Now()
	if err := m.flush(); err != nil {
		return err
	}
	flushElapsed := time.Since(flushStart)
	fmt.Printf("BitmapIndex startup load path=%s files=%d standard_files=%d bsi_files=%d ignored_fields=%d manifest_status=%s manifest_detail=%q manifest_entries=%d manifest_files=%d manifest_scan_entries=%d manifest_scan_files=%d manifest_missing_files=%d manifest_observe_elapsed=%v manifest_write_elapsed=%v fragments=%d standard_fragments=%d bsi_fragments=%d walk_elapsed=%v enqueue_elapsed=%v flush_elapsed=%v total_elapsed=%v\n",
		baseDir, fileCount, standardFileCount, bsiFileCount, ignoredFieldCount, manifestObservation.Status, manifestObservation.Detail, manifestObservation.ManifestEntries, manifestObservation.ManifestFiles, manifestObservation.ScanEntries, manifestObservation.ScanFiles, manifestObservation.MissingFileCount, manifestObservation.Elapsed, manifestWriteElapsed, fragmentCount, standardFragmentCount, bsiFragmentCount, walkElapsed, enqueueElapsed, flushElapsed, time.Since(start))
	return nil
}

func (m *BitmapIndex) readBitmapFilesFromManifest(manifest BitmapShardManifest, observation BitmapShardManifestObservation, fragQueue chan *BitmapFragment, startedAt time.Time) error {
	loadStart := time.Now()
	fragMap := newBitmapStartupFragmentMap()
	fileCount := 0
	standardFileCount := 0
	bsiFileCount := 0
	nextProgressFileCount := 50000
	bsiPackCache := make(map[string][]bsiPackBundleEntry)

	for _, entry := range manifest.Entries {
		if _, err := m.getFieldConfig(entry.Table, entry.Field); err != nil {
			return fmt.Errorf("manifest references field outside active schema: %s.%s: %w", entry.Table, entry.Field, err)
		}
		switch entry.Kind {
		case bitmapShardKindStandard:
			if isStandardBundleManifestEntry(entry) {
				loadedFiles, err := m.loadManifestStandardBundleEntry(entry, fragMap)
				if err != nil {
					return err
				}
				fileCount += loadedFiles
				standardFileCount += loadedFiles
				continue
			}
			if len(entry.Files) != 1 {
				return fmt.Errorf("manifest standard shard %s.%s row=%d shard=%s has %d files",
					entry.Table, entry.Field, entry.RowIDOrBits, entry.Shard, len(entry.Files))
			}
			file := entry.Files[0]
			data, err := os.ReadFile(filepath.Join(m.dataDir, filepath.FromSlash(file.RelativePath)))
			if err != nil {
				return fmt.Errorf("read manifest standard bitmap %s: %w", file.RelativePath, err)
			}
			fileCount++
			standardFileCount++
			if err := fragMap.add(&BitmapFragment{
				IndexName:   entry.Table,
				FieldName:   entry.Field,
				RowIDOrBits: entry.RowIDOrBits,
				Time:        entry.ShardTime,
				BitData:     [][]byte{data},
				ModTime:     file.ModTime,
				IsInit:      true,
			}); err != nil {
				return err
			}
		case bitmapShardKindBSI:
			frag := &BitmapFragment{
				IndexName:   entry.Table,
				FieldName:   entry.Field,
				RowIDOrBits: -1,
				Time:        entry.ShardTime,
				BitData:     make([][]byte, 2),
				ModTime:     entry.ShardTime,
				IsBSI:       true,
				IsInit:      true,
			}
			loadedFiles, err := m.loadManifestBSIEntry(entry, frag, bsiPackCache)
			if err != nil {
				return err
			}
			fileCount += loadedFiles
			bsiFileCount += loadedFiles
			if fileCount >= nextProgressFileCount {
				fmt.Printf("BitmapIndex startup manifest load progress files=%d standard_files=%d bsi_files=%d elapsed=%v\n",
					fileCount, standardFileCount, bsiFileCount, time.Since(loadStart))
				for nextProgressFileCount <= fileCount {
					nextProgressFileCount += 50000
				}
			}
			if len(frag.BitData) == 0 || len(frag.BitData[0]) == 0 {
				return fmt.Errorf("manifest BSI shard %s.%s shard=%s has no existence bitmap",
					entry.Table, entry.Field, entry.Shard)
			}
			if err := fragMap.add(frag); err != nil {
				return err
			}
		default:
			return fmt.Errorf("manifest has unknown bitmap shard kind %s", entry.Kind)
		}
	}

	enqueueStart := time.Now()
	fragmentCount, standardFragmentCount, bsiFragmentCount, err := enqueueBitmapStartupFragments(fragMap, fragQueue)
	if err != nil {
		return err
	}
	enqueueElapsed := time.Since(enqueueStart)
	flushStart := time.Now()
	if err := m.flush(); err != nil {
		return err
	}
	flushElapsed := time.Since(flushStart)
	fmt.Printf("BitmapIndex startup load_source=manifest opt_in=true files=%d standard_files=%d bsi_files=%d manifest_status=%s manifest_detail=%q manifest_entries=%d manifest_files=%d manifest_missing_files=%d manifest_observe_elapsed=%v fragments=%d standard_fragments=%d bsi_fragments=%d manifest_load_elapsed=%v enqueue_elapsed=%v flush_elapsed=%v total_elapsed=%v\n",
		fileCount, standardFileCount, bsiFileCount, observation.Status, observation.Detail, observation.ManifestEntries, observation.ManifestFiles, observation.MissingFileCount, observation.Elapsed, fragmentCount, standardFragmentCount, bsiFragmentCount, time.Since(loadStart), enqueueElapsed, flushElapsed, time.Since(startedAt))
	return nil
}

func isStandardBundleManifestEntry(entry BitmapShardManifestEntry) bool {
	return entry.Kind == bitmapShardKindStandard &&
		len(entry.Files) == 1 &&
		(entry.Files[0].Role == bitmapShardFileRoleBundle || filepath.Base(entry.Files[0].RelativePath) == standardBundleFileName)
}

func (m *BitmapIndex) loadManifestStandardBundleEntry(entry BitmapShardManifestEntry, fragMap bitmapStartupFragmentMap) (int, error) {
	file := entry.Files[0]
	data, err := os.ReadFile(filepath.Join(m.dataDir, filepath.FromSlash(file.RelativePath)))
	if err != nil {
		return 0, fmt.Errorf("read manifest standard bitmap bundle %s: %w", file.RelativePath, err)
	}
	entries, err := decodeStandardBitmapBundle(data)
	if err != nil {
		return 0, fmt.Errorf("decode manifest standard bitmap bundle %s: %w", file.RelativePath, err)
	}
	for _, bundled := range entries {
		if err := fragMap.add(&BitmapFragment{
			IndexName:   entry.Table,
			FieldName:   entry.Field,
			RowIDOrBits: int64(bundled.RowID),
			Time:        entry.ShardTime,
			BitData:     [][]byte{bundled.Data},
			ModTime:     file.ModTime,
			IsInit:      true,
		}); err != nil {
			return 1, err
		}
	}
	return 1, nil
}

func (m *BitmapIndex) loadManifestBSIEntry(entry BitmapShardManifestEntry, frag *BitmapFragment, bsiPackCache map[string][]bsiPackBundleEntry) (int, error) {
	if len(entry.Files) == 1 && entry.Files[0].Role == bitmapShardFileRoleBSIPack {
		file := entry.Files[0]
		path := filepath.Join(m.dataDir, filepath.FromSlash(file.RelativePath))
		packEntries, ok := bsiPackCache[file.RelativePath]
		loadedFiles := 0
		if !ok {
			data, err := os.ReadFile(path)
			if err != nil {
				return 0, fmt.Errorf("read manifest BSI pack %s: %w", file.RelativePath, err)
			}
			var decodeErr error
			packEntries, decodeErr = decodeBSIPackBundle(data)
			if decodeErr != nil {
				return 0, fmt.Errorf("decode manifest BSI pack %s: %w", file.RelativePath, decodeErr)
			}
			bsiPackCache[file.RelativePath] = packEntries
			loadedFiles = 1
		}
		packed, ok := findBSIPackBundleEntry(packEntries, entry.Field)
		if !ok {
			return loadedFiles, fmt.Errorf("manifest BSI pack %s does not contain field %s", file.RelativePath, entry.Field)
		}
		frag.BitData = packed.Data
		if file.ModTime.After(frag.ModTime) {
			frag.ModTime = file.ModTime
		}
		return loadedFiles, nil
	}
	if len(entry.Files) == 1 && (entry.Files[0].Role == bitmapShardFileRoleBundle || filepath.Base(entry.Files[0].RelativePath) == bsiBundleFileName) {
		file := entry.Files[0]
		data, err := os.ReadFile(filepath.Join(m.dataDir, filepath.FromSlash(file.RelativePath)))
		if err != nil {
			return 0, fmt.Errorf("read manifest BSI bundle %s: %w", file.RelativePath, err)
		}
		chunks, err := decodeBSIBundle(data)
		if err != nil {
			return 0, fmt.Errorf("decode manifest BSI bundle %s: %w", file.RelativePath, err)
		}
		frag.BitData = chunks
		if file.ModTime.After(frag.ModTime) {
			frag.ModTime = file.ModTime
		}
		return 1, nil
	}

	if entry.BaseRelativePath != "" {
		basePath := filepath.Join(m.dataDir, filepath.FromSlash(entry.BaseRelativePath))
		loadedFiles := 0
		if !entry.ModTime.IsZero() {
			frag.ModTime = entry.ModTime
		}
		for bitSliceIndex := 0; bitSliceIndex <= entry.MaxBitSlice; bitSliceIndex++ {
			name := strconv.Itoa(bitSliceIndex)
			if bitSliceIndex == 0 {
				name = "EBM"
			}
			path := filepath.Join(basePath, name)
			data, err := os.ReadFile(path)
			if err != nil {
				return loadedFiles, fmt.Errorf("read compact manifest BSI bitmap %s: %w", filepath.ToSlash(path), err)
			}
			loadedFiles++
			for bitSliceIndex >= len(frag.BitData) {
				frag.BitData = append(frag.BitData, make([]byte, 0))
			}
			frag.BitData[bitSliceIndex] = data
		}
		if entry.FileCount > 0 && loadedFiles != entry.FileCount {
			return loadedFiles, fmt.Errorf("compact manifest BSI shard %s.%s shard=%s loaded %d files, manifest expected %d",
				entry.Table, entry.Field, entry.Shard, loadedFiles, entry.FileCount)
		}
		return loadedFiles, nil
	}

	loadedFiles := 0
	for _, file := range entry.Files {
		data, err := os.ReadFile(filepath.Join(m.dataDir, filepath.FromSlash(file.RelativePath)))
		if err != nil {
			return loadedFiles, fmt.Errorf("read manifest BSI bitmap %s: %w", file.RelativePath, err)
		}
		loadedFiles++
		for file.BitSlice >= len(frag.BitData) {
			frag.BitData = append(frag.BitData, make([]byte, 0))
		}
		frag.BitData[file.BitSlice] = data
		if file.ModTime.After(frag.ModTime) {
			frag.ModTime = file.ModTime
		}
	}
	return loadedFiles, nil
}

type bitmapStartupFragmentMap map[string]map[string]map[int64]map[int64]*BitmapFragment

func newBitmapStartupFragmentMap() bitmapStartupFragmentMap {
	return make(map[string]map[string]map[int64]map[int64]*BitmapFragment)
}

func (m bitmapStartupFragmentMap) add(f *BitmapFragment) error {
	if _, ok := m[f.IndexName]; !ok {
		m[f.IndexName] = make(map[string]map[int64]map[int64]*BitmapFragment)
	}
	if _, ok := m[f.IndexName][f.FieldName]; !ok {
		m[f.IndexName][f.FieldName] = make(map[int64]map[int64]*BitmapFragment)
	}
	rID := f.RowIDOrBits
	if f.IsBSI {
		rID = -1
	}
	if _, ok := m[f.IndexName][f.FieldName][rID]; !ok {
		m[f.IndexName][f.FieldName][rID] = make(map[int64]*BitmapFragment)
	}
	t := f.Time.UnixNano()
	if existing, ok := m[f.IndexName][f.FieldName][rID][t]; ok {
		if f.IsInit && existing.IsInit {
			if f.ModTime.After(existing.ModTime) {
				m[f.IndexName][f.FieldName][rID][t] = f
			}
			return nil
		}
		return fmt.Errorf("duplicate startup bitmap fragment %s.%s row=%d shard=%s",
			f.IndexName, f.FieldName, rID, f.Time.Format(timeFmt))
	}
	m[f.IndexName][f.FieldName][rID][t] = f
	return nil
}

func enqueueBitmapStartupFragments(fragMap bitmapStartupFragmentMap, fragQueue chan *BitmapFragment) (int, int, int, error) {
	fragmentCount := 0
	standardFragmentCount := 0
	bsiFragmentCount := 0
	for _, index := range fragMap {
		for _, field := range index {
			for _, ts := range field {
				for _, frag := range ts {
					fragmentCount++
					if frag.IsBSI {
						bsiFragmentCount++
					} else {
						standardFragmentCount++
					}
					select {
					case fragQueue <- frag:
					default:
						return fragmentCount, standardFragmentCount, bsiFragmentCount, fmt.Errorf("Update: fragment queue is full")
					}
				}
			}
		}
	}
	return fragmentCount, standardFragmentCount, bsiFragmentCount, nil
}

func isBSIBitmapPath(parts []string) bool {
	return len(parts) > 2 && parts[2] == "bsi"
}

func isStandardBitmapBundlePath(parts []string) bool {
	return len(parts) > 4 && parts[2] == standardBundleLeafDir && parts[len(parts)-1] == standardBundleFileName
}

// Purge a partition from cache
func (m *BitmapIndex) purgePartition(aop *Partition) {

	t := aop.Time.UnixNano()
	if aop.RowIDOrBits >= 0 {
		rowID := uint64(aop.RowIDOrBits)
		m.bitmapCacheLock.Lock()
		defer m.bitmapCacheLock.Unlock()
		_, ok := m.bitmapCache[aop.Index][aop.Field][rowID][t]
		if ok {
			delete(m.bitmapCache[aop.Index][aop.Field][rowID], t)
			u.Infof("Purged standard bitmap %s.%s, ts = %v, rowID = %d", aop.Index, aop.Field,
				aop.Time.Format(timeFmt), rowID)
		}
	} else {
		m.bsiCacheLock.Lock()
		defer m.bsiCacheLock.Unlock()
		bsi, ok := m.bsiCache[aop.Index][aop.Field][t]
		if ok {
			removed := bsi.GetExistenceBitmap().Clone()
			delete(m.bsiCache[aop.Index][aop.Field], t)
			m.updateSeedCacheForBSIFragment(aop.Index, aop.Field, t, nil, removed)
			u.Infof("Purged BSI %s.%s, ts = %v", aop.Index, aop.Field, aop.Time.Format(timeFmt))
		}
	}
}
