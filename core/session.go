package core

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsexpr"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/xitongsys/parquet-go/reader"
)

var (
	leadingInt = regexp.MustCompile(`^[-+]?\d+`)
)

const (
	timeFmt = "2006-01-02T15"
)

const (
	reservationSize        = 1000
	ifDelim                = "/"
	primaryKey             = "P"
	secondaryKey           = "S"
	julianDayOfEpoch int64 = 2440588
	microsPerDay     int64 = 3600 * 24 * 1000 * 1000
	batchBufferSize        = 90000000 // This is a stopgap to prevent overrunning memory
)

// Session - State for session (non-threadsafe)
type Session struct {
	BasePath       string // path to schema directory
	BitIndex       *shared.BitmapIndex
	BatchBuffer    *shared.BatchBuffer
	StringIndex    *shared.StringSearch
	KVStore        *shared.KVStore
	TableBuffers   map[string]*TableBuffer
	Nested         bool
	DateFilter     *time.Time // optional filter to only include records matching timestamp
	BytesRead      int        // Bytes read for a row (record)
	CreatedAt      time.Time
	poolGeneration uint64
	stateLock      sync.Mutex
	flushing       bool

	tableCache         *TableCacheStruct
	primaryKeyResolver PrimaryKeyResolver
	lastFlushProfile   shared.BatchBufferFlushProfile
}

// PrimaryKeyResolver owns primary-key lookup and rownum assignment.
type PrimaryKeyResolver interface {
	ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest) (PrimaryKeyResolveResult, error)
}

// PrimaryKeyResolveRequest carries the current primary-key resolution context.
type PrimaryKeyResolveRequest struct {
	Session          *Session
	TableBuffer      *TableBuffer
	LookupValue      string
	PrimaryKeyValues []interface{}
	ProvidedColumnID uint64
	DirectColumnID   bool
	PrimaryKeyMode   PrimaryKeyMode
}

// PrimaryKeyResolveResult describes the row identity selected by a resolver.
type PrimaryKeyResolveResult struct {
	ColumnID    uint64
	ExistingRow bool
	Profile     PrimaryKeyResolveProfile
}

// KVPrimaryKeyResolver preserves the older KV-backed primary-key lookup and
// rownum assignment behavior while BSI-backed authority is promoted.
type KVPrimaryKeyResolver struct{}

// PrimaryKeyMode selects how PutRow resolves lookup-backed primary keys.
type PrimaryKeyMode string

const (
	// PrimaryKeyModeVerifyExisting preserves the default idempotent lookup path.
	PrimaryKeyModeVerifyExisting PrimaryKeyMode = "verify_existing"
	// PrimaryKeyModeAssumeNew skips existing-row lookup for validated fresh loads.
	PrimaryKeyModeAssumeNew PrimaryKeyMode = "assume_new"
)

func (m PrimaryKeyMode) assumeNew() bool {
	return strings.EqualFold(string(m), string(PrimaryKeyModeAssumeNew))
}

// Normalize returns a supported primary-key mode, defaulting to verify-existing.
func (m PrimaryKeyMode) Normalize() PrimaryKeyMode {
	if m.assumeNew() {
		return PrimaryKeyModeAssumeNew
	}
	return PrimaryKeyModeVerifyExisting
}

func (m PrimaryKeyMode) normalize() PrimaryKeyMode {
	return m.Normalize()
}

// PutRowOptions carries optional ingestion metadata for future streaming
// idempotency and observability. The current load path preserves existing
// behavior and does not yet enforce deduplication from these fields.
type PutRowOptions struct {
	EventID        string
	Source         string
	EventTime      time.Time
	SourceOffset   string
	PayloadHash    uint64
	DedupTTL       time.Duration
	PrimaryKeyMode PrimaryKeyMode
}

// PutRowResult describes the row identity observed by the load path. Duplicate
// and conflict semantics are reserved for the streaming dedup boundary.
type PutRowResult struct {
	TableName             string
	ColumnID              uint64
	ChildRowCount         int
	LogicalRowCount       int
	Inserted              bool
	ExistingRow           bool
	Duplicate             bool
	Conflict              bool
	SourceElapsed         time.Duration
	IdentityElapsed       time.Duration
	AlternateKeysElapsed  time.Duration
	ChildExpansionElapsed time.Duration
	ChildTraversalElapsed time.Duration
	RelationElapsed       time.Duration
	AttributeElapsed      time.Duration
	TotalElapsed          time.Duration
	PrimaryKey            PrimaryKeyResolveProfile
	PrimaryKeyByTable     map[string]PrimaryKeyResolveProfile `json:"primary_key_by_table,omitempty"`
}

type putRowRequest struct {
	tableName             string
	row                   interface{}
	pqTablePath           string
	startedAt             time.Time
	providedColID         uint64
	isChild               bool
	ignoreSourcePath      bool
	useNerdCapitalization bool
	primaryKeyMode        PrimaryKeyMode
	options               PutRowOptions
	timings               *putRowStageTimings
}

type putRowIdentity struct {
	columnID          uint64
	timestamp         time.Time
	primaryKeyValues  []interface{}
	lookupValue       string
	updateExisting    bool
	primaryKeyProfile PrimaryKeyResolveProfile
}

type putRowStageTimings struct {
	sourceElapsed         time.Duration
	identityElapsed       time.Duration
	alternateKeysElapsed  time.Duration
	childExpansionElapsed time.Duration
	childTraversalElapsed time.Duration
	relationElapsed       time.Duration
	attributeElapsed      time.Duration
	childRowCount         int
	primaryKeyProfile     PrimaryKeyResolveProfile
	primaryKeyByTable     map[string]PrimaryKeyResolveProfile
}

type putRowStageName string

const (
	putRowStageIdentity        putRowStageName = "identity"
	putRowStageAlternateKeys   putRowStageName = "alternate_keys"
	putRowStageChildExpansion  putRowStageName = "expand_children"
	putRowStageParentRelations putRowStageName = "map_parent_relations"
	putRowStageAttributes      putRowStageName = "map_attributes"
)

type putRowPipelineStage struct {
	name   putRowStageName
	record func(*putRowStageTimings, time.Duration)
	run    func() error
}

// TableBuffer - State info for table.
type TableBuffer struct {
	Table            *Table // table schema
	sequencerCache   map[int64]*shared.Sequencer
	CurrentColumnID  uint64
	CurrentTimestamp time.Time // Time quantum value
	CurrentPKValue   []interface{}
	PKMap            map[string]*Attribute
	PKAttributes     []*Attribute
	SKMap            map[string][]*Attribute
	rowCache         map[string]interface{} // row value cache ensures parquet data is only read once
}

func formatShardTime(t time.Time) string {
	return t.UTC().Format(timeFmt)
}

// NewTableBuffer - Construct a TableBuffer
func NewTableBuffer(table *Table) (*TableBuffer, error) {

	if table == nil {
		return nil, fmt.Errorf("table is nil")
	}
	tb := &TableBuffer{Table: table}
	tb.sequencerCache = make(map[int64]*shared.Sequencer)
	tb.PKMap = make(map[string]*Attribute)
	tb.PKAttributes = make([]*Attribute, 0)
	tb.CurrentTimestamp = time.Unix(0, 0)
	pka, errx := table.GetPrimaryKeyInfo()
	if errx != nil {
		return nil, errx
	}
	tb.PKAttributes = pka
	for _, v := range pka {
		tb.PKMap[v.FieldName] = v
	}
	tb.rowCache = make(map[string]interface{})
	var err error
	if table.SecondaryKeys != "" {
		tb.SKMap, err = table.GetAlternateKeyInfo()
	}
	return tb, err
}

// NextColumnID - Get a new column ID in the sequence for a given Time Quantum
func (t *TableBuffer) NextColumnID(bi *shared.BitmapIndex) error {

	sequencer, ok := t.sequencerCache[t.CurrentTimestamp.UnixNano()]
	if !ok || sequencer.IsFullySubscribed() {
		seq, err := bi.CheckoutSequence(t.Table.Name, t.PKAttributes[0].FieldName,
			t.CurrentTimestamp, reservationSize)
		if err != nil {
			t.CurrentColumnID = 0
			return fmt.Errorf("Sequencer checkout error for %s.%s - %v]", t.Table.Name,
				t.PKAttributes[0].FieldName, err)
		}
		sequencer = seq
		t.sequencerCache[t.CurrentTimestamp.UnixNano()] = sequencer
	}
	t.CurrentColumnID, _ = sequencer.Next()
	return nil
}

// ShouldLookupPrimaryKey - Does this table have a primary key
func (t *TableBuffer) ShouldLookupPrimaryKey() bool {

	if t.PKAttributes[0].ColumnID {
		return false
	}

	if t.Table.TimeQuantumType != "" && len(t.PKAttributes) > 1 {
		if t.PKAttributes[1].ColumnID {
			return false
		}
		return true
	}
	if t.Table.TimeQuantumType == "" && len(t.PKAttributes) > 0 {
		return true
	}
	return false
}

// OpenSession - Creates a connected session to the underlying core.
// (This is intentionally not thread-safe for maximum throughput.)
func OpenSession(tableCache *TableCacheStruct, path, name string, nested bool, conn *shared.Conn) (*Session, error) {

	// FIXME - is the nested flag necessary?
	if name == "" {
		return nil, fmt.Errorf("table name is nil")
	}
	if tableCache == nil {
		return nil, fmt.Errorf("table cache is nil")
	}

	consul := conn.Consul
	kvStore := shared.NewKVStore(conn)

	tableBuffers := make(map[string]*TableBuffer, 0)
	tab, err := LoadTable(tableCache, path, kvStore, name, consul)
	if err != nil {
		return nil, err
	} else if nested {
		if err = recurseAndLoadTable(path, kvStore, tableBuffers, tab); err != nil {
			return nil, fmt.Errorf("Error loading child tables %v", err)
		}
	}
	// Do scan to see if there are parent relations.   If so, open the parent too.
	for _, v := range tab.Attributes {
		if v.MappingStrategy == "ParentRelation" && v.ForeignKey != "" {
			fkTable, _, _ := v.GetFKSpec()
			parent, err2 := LoadTable(tableCache, path, kvStore, fkTable, consul)
			if err2 != nil {
				return nil, fmt.Errorf("Error loading parent schema %s - %v", fkTable, err2)
			}
			if parent == nil {
				return nil, fmt.Errorf("Error loading parent schema %s - table metadata is nil", fkTable)
			}
			if tb, ok := tableBuffers[fkTable]; !ok {
				if tb, err = NewTableBuffer(parent); err == nil {
					tableBuffers[fkTable] = tb
				} else {
					return nil, fmt.Errorf("OpenSession error - %v", err)
				}
			}
		}
	}

	if tb, ok := tableBuffers[name]; !ok {
		if tb, err = NewTableBuffer(tab); err == nil {
			tableBuffers[name] = tb
		} else {
			return nil, fmt.Errorf("OpenSession error - %v", err)
		}
	}
	s := &Session{BasePath: path, TableBuffers: tableBuffers, Nested: nested}
	s.StringIndex = shared.NewStringSearch(conn, 1000)
	s.KVStore = kvStore
	s.BitIndex = shared.NewBitmapIndex(conn)
	s.BatchBuffer = shared.NewBatchBuffer(s.BitIndex, s.KVStore, batchBufferSize)
	s.CreatedAt = time.Now().UTC()
	s.tableCache = tableCache
	s.primaryKeyResolver = KVPrimaryKeyResolver{}

	return s, nil
}

func recurseAndLoadTable(basePath string, kvStore *shared.KVStore, tableBuffers map[string]*TableBuffer, curTable *Table) error {
	tableCache := curTable.tableCache

	for _, v := range curTable.Attributes {
		_, ok := tableBuffers[v.ChildTable]
		if v.ChildTable != "" && !ok {
			table, err := LoadTable(tableCache, basePath, kvStore, v.ChildTable, curTable.ConsulClient)
			if err != nil {
				return err
			}
			err = recurseAndLoadTable(basePath, kvStore, tableBuffers, table)
			if err != nil {
				return fmt.Errorf("while loading %s, %v", table.Name, err)
			}
			if tb, err := NewTableBuffer(table); err == nil {
				tableBuffers[v.ChildTable] = tb
			} else {
				return fmt.Errorf("recurseAndLoadTable error - %v", err)
			}
		}
		if v.ForeignKey != "" {
			fkTable, _, _ := v.GetFKSpec()
			_, ok = tableBuffers[v.ChildTable]
			if !ok {
				table, err := LoadTable(tableCache, basePath, kvStore, fkTable, curTable.ConsulClient)
				if err != nil {
					return err
				}
				if tb, err := NewTableBuffer(table); err == nil {
					tableBuffers[fkTable] = tb
				} else {
					return fmt.Errorf("recurseAndLoadTable error - %v", err)
				}
			}
		}
	}
	return nil
}

// IsDriverForTables - Is this the driver table?
func (s *Session) IsDriverForTables(tables []string) bool {

	for _, v := range tables {
		if _, ok := s.TableBuffers[v]; !ok {
			return false
		}
	}
	return true
}

// IsDriverForJoin - Is this the driver table?
func (s *Session) IsDriverForJoin(table, joinCol string) bool {

	tbuf, ok := s.TableBuffers[table]
	if !ok {
		return false
	}
	attr, err := tbuf.Table.GetAttribute(joinCol)
	if err != nil {
		return false
	}
	if attr.ForeignKey == "" {
		return false
	}

	return true
}

// CurrentColumnID - Returns the current column ID after call to PutRow
func (s *Session) CurrentColumnID(name string) (uint64, error) {
	tbuf, ok := s.TableBuffers[name]
	if !ok {
		return 0, fmt.Errorf("cannot locate buffer for table %s", name)
	}
	return tbuf.CurrentColumnID, nil
}

// PutRow - Entry point.  Load a row of data from source (Parquet/Kinesis/Kafka)
func (s *Session) PutRow(name string, row interface{}, providedColID uint64, ignoreSourcePath, useNerd bool) error {

	_, err := s.PutRowWithOptions(name, row, providedColID, ignoreSourcePath, useNerd, PutRowOptions{})
	return err
}

// PutRowWithOptions loads a row while accepting optional ingestion metadata for
// future streaming idempotency. Today it preserves PutRow storage behavior.
func (s *Session) PutRowWithOptions(name string, row interface{}, providedColID uint64, ignoreSourcePath, useNerd bool,
	options PutRowOptions) (PutRowResult, error) {

	totalStart := time.Now()
	var timings putRowStageTimings
	s.ResetRowCache()
	req := putRowRequest{
		tableName:             name,
		row:                   row,
		pqTablePath:           "/",
		startedAt:             totalStart,
		providedColID:         providedColID,
		ignoreSourcePath:      ignoreSourcePath,
		useNerdCapitalization: useNerd,
		primaryKeyMode:        options.PrimaryKeyMode.normalize(),
		options:               options,
		timings:               &timings,
	}
	sourceStart := time.Now()
	if err := s.normalizePutRowSource(&req); err != nil {
		return PutRowResult{}, err
	}
	timings.sourceElapsed = time.Since(sourceStart)
	return s.putRow(req)
}

func (s *Session) normalizePutRowSource(req *putRowRequest) error {

	if req == nil {
		return fmt.Errorf("put row request is nil")
	}
	if req.pqTablePath == "" {
		req.pqTablePath = "/"
	}
	if r, ok := req.row.(*reader.ParquetReader); ok {
		req.pqTablePath = fmt.Sprintf("%s.%s", r.SchemaHandler.GetRootExName(), req.tableName)
	} else if r, ok := req.row.(map[string]interface{}); ok {
		if tbuf, ok2 := s.TableBuffers[req.tableName]; ok2 {
			tbuf.rowCache = r
		} else {
			return fmt.Errorf("cannot locate buffer for table %s", req.tableName)
		}
	} else {
		return fmt.Errorf("cannot process row type %T", req.row)
	}
	return nil
}

func (s *Session) recursivePutRow(name string, row interface{}, pqTablePath string, providedColID uint64,
	isChild, ignoreSourcePath, useNerdCapitalization bool, primaryKeyMode PrimaryKeyMode) (PutRowResult, error) {

	var timings putRowStageTimings
	return s.putRow(putRowRequest{
		tableName:             name,
		row:                   row,
		pqTablePath:           pqTablePath,
		providedColID:         providedColID,
		isChild:               isChild,
		ignoreSourcePath:      ignoreSourcePath,
		useNerdCapitalization: useNerdCapitalization,
		primaryKeyMode:        primaryKeyMode.normalize(),
		timings:               &timings,
	})
}

func (s *Session) putRow(req putRowRequest) (PutRowResult, error) {

	totalStart := req.startTime()
	tbuf, err := s.putRowTableBuffer(req)
	if err != nil {
		return PutRowResult{}, err
	}

	var identity putRowIdentity
	if err := s.runPutRowPipeline(req,
		putRowPipelineStage{
			name: putRowStageIdentity,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.identityElapsed += elapsed
			},
			run: func() error {
				var err error
				identity, err = s.establishRowIdentity(req, tbuf)
				return err
			},
		},
		putRowPipelineStage{
			name: putRowStageAlternateKeys,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.alternateKeysElapsed += elapsed
			},
			run: func() error {
				return s.mapAlternateKeys(req, tbuf)
			},
		},
		putRowPipelineStage{
			name: putRowStageChildExpansion,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.childExpansionElapsed += elapsed
			},
			run: func() error {
				return s.expandChildRelations(req, tbuf)
			},
		},
		putRowPipelineStage{
			name: putRowStageParentRelations,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.relationElapsed += elapsed
			},
			run: func() error {
				return s.mapParentRelations(req, tbuf)
			},
		},
		putRowPipelineStage{
			name: putRowStageAttributes,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.attributeElapsed += elapsed
			},
			run: func() error {
				return s.mapAttributeValues(req, tbuf, identity)
			},
		},
	); err != nil {
		return PutRowResult{}, err
	}

	return req.putRowResult(tbuf, identity, time.Since(totalStart)), nil
}

func (req putRowRequest) startTime() time.Time {
	if req.startedAt.IsZero() {
		return time.Now()
	}
	return req.startedAt
}

func (s *Session) putRowTableBuffer(req putRowRequest) (*TableBuffer, error) {
	tbuf, ok := s.TableBuffers[req.tableName]
	if !ok {
		return nil, fmt.Errorf("table %s invalid or not opened. (recursivePutRow) %s", req.tableName,
			req.pqTablePath)
	}
	return tbuf, nil
}

func (s *Session) runPutRowPipeline(req putRowRequest, stages ...putRowPipelineStage) error {
	for _, stage := range stages {
		if stage.run == nil {
			return fmt.Errorf("put row stage %s has no runner", stage.name)
		}
		stageStart := time.Now()
		err := stage.run()
		if stage.record != nil {
			req.addStageTiming(stage.record, time.Since(stageStart))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (req putRowRequest) addStageTiming(apply func(*putRowStageTimings, time.Duration), elapsed time.Duration) {
	if req.timings == nil {
		return
	}
	apply(req.timings, elapsed)
}

func (req putRowRequest) addChildRows(count int) {
	if req.timings == nil || count <= 0 {
		return
	}
	req.timings.childRowCount += count
}

func (req putRowRequest) addChildTraversalTiming(elapsed time.Duration) {
	if req.timings == nil || elapsed <= 0 {
		return
	}
	req.timings.childTraversalElapsed += elapsed
}

func (req putRowRequest) addPrimaryKeyProfile(profile PrimaryKeyResolveProfile) {
	if req.timings == nil {
		return
	}
	req.timings.primaryKeyProfile = req.timings.primaryKeyProfile.add(profile)
}

func (req putRowRequest) addPrimaryKeyProfileForTable(tableName string, profile PrimaryKeyResolveProfile) {
	if req.timings == nil || tableName == "" {
		return
	}
	if req.timings.primaryKeyByTable == nil {
		req.timings.primaryKeyByTable = map[string]PrimaryKeyResolveProfile{}
	}
	req.timings.primaryKeyByTable[tableName] = req.timings.primaryKeyByTable[tableName].add(profile)
}

func (req putRowRequest) addPrimaryKeyProfilesByTable(profiles map[string]PrimaryKeyResolveProfile) {
	if req.timings == nil {
		return
	}
	for tableName, profile := range profiles {
		req.addPrimaryKeyProfileForTable(tableName, profile)
	}
}

func (req putRowRequest) putRowResult(tbuf *TableBuffer, identity putRowIdentity, totalElapsed time.Duration) PutRowResult {
	result := PutRowResult{
		TableName:       req.tableName,
		ColumnID:        tbuf.CurrentColumnID,
		Inserted:        !identity.updateExisting,
		ExistingRow:     identity.updateExisting,
		LogicalRowCount: 1,
		TotalElapsed:    totalElapsed,
	}
	if req.timings == nil {
		return result
	}
	result.ChildRowCount = req.timings.childRowCount
	result.LogicalRowCount += req.timings.childRowCount
	result.SourceElapsed = req.timings.sourceElapsed
	result.IdentityElapsed = req.timings.identityElapsed
	result.AlternateKeysElapsed = req.timings.alternateKeysElapsed
	result.ChildExpansionElapsed = req.timings.childExpansionElapsed
	result.ChildTraversalElapsed = req.timings.childTraversalElapsed
	result.RelationElapsed = req.timings.relationElapsed
	result.AttributeElapsed = req.timings.attributeElapsed
	result.PrimaryKey = req.timings.primaryKeyProfile
	result.PrimaryKeyByTable = copyPrimaryKeyResolveProfileMap(req.timings.primaryKeyByTable)
	return result
}

func (s *Session) establishRowIdentity(req putRowRequest, tbuf *TableBuffer) (putRowIdentity, error) {

	identity, err := s.processPrimaryKey(tbuf, req.row, req.pqTablePath, req.providedColID, req.isChild,
		req.ignoreSourcePath, req.useNerdCapitalization, req.primaryKeyMode)
	req.addPrimaryKeyProfile(identity.primaryKeyProfile)
	req.addPrimaryKeyProfileForTable(tbuf.Table.Name, identity.primaryKeyProfile)
	return identity, err
}

func (s *Session) mapAlternateKeys(req putRowRequest, tbuf *TableBuffer) error {

	if tbuf.Table.SecondaryKeys == "" {
		return nil
	}
	// Preserved for existing catalog behavior. Streaming ingest should not rely
	// on alternate keys until this path is intentionally finished behind tests.
	return s.processAlternateKeys(tbuf, req.row, req.pqTablePath, req.isChild, req.ignoreSourcePath,
		req.useNerdCapitalization)
}

func (s *Session) expandChildRelations(req putRowRequest, tbuf *TableBuffer) error {

	recurse := len(s.TableBuffers) > 1
	for _, v := range tbuf.Table.Attributes {
		if recurse && v.MappingStrategy == "ChildRelation" && v.ChildTable != "" {
			if err := s.expandChildRows(req, tbuf, &v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Session) mapParentRelations(req putRowRequest, tbuf *TableBuffer) error {

	for _, v := range tbuf.Table.Attributes {
		if v.MappingStrategy == "ParentRelation" && v.ForeignKey != "" {
			if err := s.mapParentRelation(req, tbuf, &v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Session) mapAttributeValues(req putRowRequest, tbuf *TableBuffer, identity putRowIdentity) error {

	for _, v := range tbuf.Table.Attributes {
		if shouldSkipPutRowAttributeMapping(tbuf, &v) {
			continue
		}
		if err := s.mapAttributeValue(req, &v, identity.updateExisting); err != nil {
			return err
		}
	}
	return nil
}

func shouldSkipPutRowAttributeMapping(tbuf *TableBuffer, attr *Attribute) bool {
	if attr.System {
		return true
	}
	if _, found := tbuf.PKMap[attr.FieldName]; found {
		return true
	}
	return attr.MappingStrategy == "ChildRelation" || attr.MappingStrategy == "ParentRelation"
}

func (s *Session) expandChildRows(req putRowRequest, tbuf *TableBuffer, attr *Attribute) error {

	traversalStart := time.Now()
	expansion, ok, err := s.preparePutRowChildExpansion(tbuf, attr)
	req.addChildTraversalTiming(time.Since(traversalStart))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	for _, childRow := range expansion.childRows {
		traversalStart = time.Now()
		childPayload, err := buildPutRowChildPayload(req.row, expansion.sourcePath, childRow)
		req.addChildTraversalTiming(time.Since(traversalStart))
		if err != nil {
			return err
		}
		traversalStart = time.Now()
		expansion.childBuffer.rowCache = childPayload
		req.addChildTraversalTiming(time.Since(traversalStart))
		childResult, err := s.recursivePutRow(expansion.childTable, childPayload, expansion.sourcePath,
			req.providedColID, true, req.ignoreSourcePath, req.useNerdCapitalization, req.primaryKeyMode)
		if err != nil {
			return err
		}
		childLogicalRows := childResult.LogicalRowCount
		if childLogicalRows <= 0 {
			childLogicalRows = 1
		}
		req.addPrimaryKeyProfile(childResult.PrimaryKey)
		req.addPrimaryKeyProfilesByTable(childResult.PrimaryKeyByTable)
		req.addChildRows(childLogicalRows)
	}
	return nil
}

func copyPrimaryKeyResolveProfileMap(src map[string]PrimaryKeyResolveProfile) map[string]PrimaryKeyResolveProfile {
	if src == nil {
		return nil
	}
	dst := make(map[string]PrimaryKeyResolveProfile, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

type putRowChildExpansion struct {
	childTable  string
	sourcePath  string
	childRows   []interface{}
	childBuffer *TableBuffer
}

func (s *Session) preparePutRowChildExpansion(tbuf *TableBuffer, attr *Attribute) (putRowChildExpansion, bool, error) {

	val, err := shared.GetPath(attr.SourceName, tbuf.rowCache, false, false)
	if err != nil {
		u.Errorf("recursion into child  = %s, %v, %#v", attr.SourceName, err, tbuf.rowCache)
		return putRowChildExpansion{}, false, nil
	}
	childRows, ok := val.([]interface{})
	if !ok {
		return putRowChildExpansion{}, false, nil
	}
	childBuf, ok := s.TableBuffers[attr.ChildTable]
	if !ok {
		return putRowChildExpansion{}, false, fmt.Errorf("child table %s invalid or not opened. (recursivePutRow) %s",
			attr.ChildTable, attr.SourceName)
	}
	return putRowChildExpansion{
		childTable:  attr.ChildTable,
		sourcePath:  attr.SourceName,
		childRows:   childRows,
		childBuffer: childBuf,
	}, true, nil
}

func buildPutRowChildPayload(parentRow interface{}, sourcePath string, childRow interface{}) (map[string]interface{}, error) {

	parent, ok := parentRow.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("child expansion requires map row, got %T", parentRow)
	}
	childPayload := make(map[string]interface{}, len(parent)+1)
	for key, value := range parent {
		childPayload[key] = value
	}
	childPayload[sourcePath] = childRow
	return childPayload, nil
}

func (s *Session) mapParentRelation(req putRowRequest, tbuf *TableBuffer, attr *Attribute) error {

	fkTable, fkFieldSpec, _ := attr.GetFKSpec()
	relBuf, ok := s.TableBuffers[fkTable]
	if !ok {
		return fmt.Errorf("Could not locate parent table buffer for [%s]", fkTable)
	}
	relColumnID, okToMap, err := s.resolveParentRelationColumnID(req, tbuf, relBuf, attr, fkFieldSpec)
	if err != nil {
		return err
	}
	if !okToMap {
		return nil
	}
	// Store the parent table ColumnID in the IntBSI for join queries
	if _, err := attr.MapValue(relColumnID, s, false); err != nil {
		return fmt.Errorf("Error Mapping FK [%s].[%s] - %v", attr.Parent.Name, attr.FieldName, err)
	}
	return nil
}

func (s *Session) resolveParentRelationColumnID(req putRowRequest, tbuf, relBuf *TableBuffer, attr *Attribute,
	fkFieldSpec string) (uint64, bool, error) {

	if req.isChild {
		// TODO: Verify this with nested structure.
		return relBuf.CurrentColumnID, true, nil
	}
	if attr.Type == "Integer" && (!relBuf.ShouldLookupPrimaryKey() || fkFieldSpec == "@rownum") {
		vals, _, err := s.readColumn(req.row, req.pqTablePath, attr, false, req.ignoreSourcePath,
			req.useNerdCapitalization)
		if err != nil {
			return 0, false, err
		}
		if len(vals) != 1 {
			return 0, false, fmt.Errorf("Expected 1 value from direct parent id mapping.")
		}
		if vals[0] == nil {
			return 0, false, nil
		}
		switch reflect.ValueOf(vals[0]).Kind() {
		case reflect.String:
			colID, err := strconv.ParseInt(vals[0].(string), 10, 64)
			if err != nil {
				return 0, false, fmt.Errorf("cannot parse string %v for parent relation %v type is %T",
					vals[0], attr.FieldName, vals[0])
			}
			return uint64(colID), true, nil
		case reflect.Int64:
			return uint64(vals[0].(int64)), true, nil
		default:
			return 0, false, fmt.Errorf("cannot cast %v to uint64 for parent relation %v type is %T",
				vals[0], attr.FieldName, vals[0])
		}
	}

	lookupKey, err := s.resolveFKLookupKey(attr, tbuf, req.row, true, false)
	if err != nil {
		return 0, false, fmt.Errorf("resolveFKLookupKey %v", err)
	}
	// Not a nested import structure, must lookup the columnID of the relation.
	// TODO: Very expensive, implement lookup cache.
	colID, found, err := s.lookupColumnID(relBuf, lookupKey, fkFieldSpec)
	if err != nil {
		return 0, false, fmt.Errorf("lookupColumnID %s,  %v", lookupKey, err)
	}
	if !found {
		return 0, false, fmt.Errorf("cannot find value '%s' in parent table '%v' for column %s.%s",
			lookupKey, attr.ForeignKey, attr.Parent.Name, attr.FieldName)
	}
	return colID, true, nil
}

func (s *Session) mapAttributeValue(req putRowRequest, attr *Attribute, update bool) error {

	vals, pqps, err := s.readColumn(req.row, req.pqTablePath, attr, req.isChild, req.ignoreSourcePath,
		req.useNerdCapitalization)
	if err != nil {
		return fmt.Errorf("Parquet reader error - %v", err)
	}
	for _, cval := range vals {
		if cval == nil {
			continue
		}
		// Map and index the value
		if _, err := attr.MapValue(cval, s, update); err != nil {
			return fmt.Errorf("%s - %v", pqps[0], err)
		}
	}
	return nil
}

// // This function ensures that each parquet column is read once and only once for each row
func (s *Session) readColumn(row interface{}, pqTablePath string, v *Attribute,
	isChild, ignoreSourcePath, useNerdCapitalization bool) ([]interface{}, []string, error) {

	// If we are ignoring source path and it is not defined then this must be a defaulted value
	//if !ignoreSourcePath && v.SourceName == "" {
	if v.DefaultValue != "" {
		//if v.DefaultValue != "" {
		retVals := make([]interface{}, 0)
		retVals = append(retVals, s.getDefaultValueForColumn(v, row, ignoreSourcePath, useNerdCapitalization))
		pqColPaths := []string{""}
		return retVals, pqColPaths, nil
		//}
		//return nil, nil, fmt.Errorf("readColumn: attribute sourceName is empty for %s", v.FieldName)
		return nil, []string{""}, nil
	}
	// Compound foreign keys are comprised of multiple source references separated by +
	sources := strings.Split(v.SourceName, "+")
	pqColPaths := make([]string, len(sources))
	retVals := make([]interface{}, len(sources))
	for i, source := range sources {
		root := "/"
		isParquet := false
		pqColPath := source
		if r, ok := row.(*reader.ParquetReader); ok {
			root = r.SchemaHandler.GetRootExName()
			isParquet = true
		}
		if isParquet {
			pqColPath = fmt.Sprintf("%s.list.element.%s", pqTablePath, source)
			if !isChild {
				pqColPath = fmt.Sprintf("%s.%s", pqTablePath, source)
				if useNerdCapitalization {
					pqColPath = fmt.Sprintf("%s.%s", pqTablePath, strings.Title(source))
				}
			}
			if !ignoreSourcePath {
				if strings.HasPrefix(source, "/") {
					pqColPath = fmt.Sprintf("%s.%s", root, source[1:])
					if useNerdCapitalization {
						pqColPath = fmt.Sprintf("%s.%s", strings.Title(root), strings.Title(source[1:]))
					}
				} else if strings.HasPrefix(source, "^") {
					pqColPath = fmt.Sprintf("%s.%s.list.element.%s", root, v.Parent.Name, source[1:])
				}
			} else {
				if useNerdCapitalization {
					pqColPath = fmt.Sprintf("%s.%s", strings.Title(root), strings.Title(v.FieldName))
				} else {
					pqColPath = fmt.Sprintf("%s.%s", root, v.FieldName)
				}
			}
		}
		pqColPaths[i] = pqColPath
		// Check cache first
		tbuf, ok := s.TableBuffers[v.Parent.Name]
		if !ok {
			return nil, nil, fmt.Errorf("readColumn: table not open for %s", v.Parent.Name)
		}
		val, found := tbuf.rowCache[pqColPath]
		if !found && !isParquet && isChild {
			val, found = tbuf.rowCache[pqTablePath]
			if found {
				val, found = val.(map[string]interface{})[v.SourceName]
			}
		}
		if !found && !isParquet {
			//val, found = tbuf.rowCache[source[1:]]
			src := v.FieldName
			if len(source) > 1 {
				src = source[1:]
			}
			if isChild {
				src = pqColPath
			}
			var err error
			found = true
			if val, err = shared.GetPath(src, tbuf.rowCache, ignoreSourcePath, useNerdCapitalization); err != nil {
				found = false
				if v.Required {
					u.Warnf("field %s, source %s = %v", v.FieldName, source, err)
				}
			}
		}
		if !isParquet {
			if (found && v.Required && val == nil) || (!found && v.Required) {
				return nil, nil, fmt.Errorf("field %s - %s is required", v.FieldName, source)
			}
			if aryVal, ok := val.([]interface{}); ok { // JSON array is StringEnum multi value
				s := make([]string, len(aryVal))
				for x, y := range aryVal {
					s[x] = fmt.Sprint(y)
					retVals[i] = s
				}
			} else {
				retVals[i] = val
			}
			continue
		} else {
			if found {
				retVals[i] = val
				continue
			}
		}
		if r, ok := row.(*reader.ParquetReader); ok {
			vals, _, _, err := r.ReadColumnByPath(pqColPath, 1)
			if err != nil {
				return nil, nil, fmt.Errorf("Parquet reader error for %s [%v]", pqColPath, err)
			}
			s.BytesRead += int(unsafe.Sizeof(vals))
			if v.DefaultValue != "" {
				if len(vals) == 0 {
					vals = append(vals, []string{""})
				}
				if str, ok := vals[0].(string); ok {
					if str == "" {
						vals[0] = fmt.Sprintf("%v", s.getDefaultValueForColumn(v, row, ignoreSourcePath,
							useNerdCapitalization))
					}
				}
			}
			if len(vals) == 0 || (len(vals) == 1 && vals[0] == nil) {
				if !v.Required {
					return nil, nil, nil
				}
				return nil, nil, fmt.Errorf("field %s - %s is required", v.FieldName, pqColPath)
			}
			if v.Required && (v.Type == "String" || v.Type == "Date" || v.Type == "DateTime") {
				if str, ok := vals[0].(string); ok {
					if str == "" {
						return nil, nil, fmt.Errorf("for field [%s], source [%s] is required", v.FieldName, pqColPath)
					}
				}
			}
			if v.Type == "DateTime" {
				str, ok := vals[0].(string)
				if ok && len(str) == 12 { // Handle INT96
					ts := INT96ToTime(str)
					vals[0] = ts.Format(time.RFC3339)
				}
			}
			retVals[i] = vals[0]
			tbuf.rowCache[pqColPath] = vals[0]
		} else {
			return nil, nil,
				fmt.Errorf("for field [%s], source [%s] for non-parquet should have found cached data",
					v.FieldName, pqColPath)
		}
	}
	return retVals, pqColPaths, nil
}

// // Get the defalue value for a column (can be an expression)
func (s *Session) getDefaultValueForColumn(a *Attribute, row interface{}, ignoreSourcePath, useNerd bool) interface{} {

	// add ignoreSourcePath parameter

	var (
		//val value.Value
		val interface{}
		ok  bool
		r   interface{}
	)
	rm := make(map[string]interface{})

	// convert source paths to fieldname paths in incoming row
	if r, ok = row.(*reader.ParquetReader); ok {
		if r != nil {
			for _, v := range a.Parent.Attributes {
				if v.SourceName == "" {
					continue
				}
				var err error
				var val interface{}
				if val, err = shared.GetPath(v.SourceName, row, ignoreSourcePath, useNerd); err != nil {
					val = v.SourceName
				}
				rm[v.FieldName] = val
			}
		}

		evaluator := qsexpr.CatalogExpressionEvaluator{}
		cell, diag := evaluator.EvaluateDefault(qsbridge.ColumnDefaultExpression(a.DefaultValue), rm)
		val = cell.Value
		ok = diag.BlocksNative()

		/*
			var ctx *datasource.ContextSimple
			if r != nil {
				ctx = datasource.NewContextSimpleNative(rm)
			}
			exprNode, _ := expr.ParseExpression(a.DefaultValue)
			val, ok = vm.Eval(ctx, exprNode)
			if !ok {
				if exprNode != nil {
					switch exprNode.NodeType() {
					case "Func", "Identity":
						return nil
					}
				}
				val = value.NewValue(a.DefaultValue)
			}
		*/
		// return fmt.Sprintf("%v", val.Value())
	} else if r, ok = row.(map[string]interface{}); ok {
		if r != nil {
			for _, v := range a.Parent.Attributes {
				source := v.SourceName
				if source == "" {
					source = v.FieldName
				}
				var err error
				var val interface{}
				if val, err = shared.GetPath(source, row, ignoreSourcePath, useNerd); err == nil {
					rm[v.FieldName] = val
					if v.FieldName == a.FieldName {
						return fmt.Sprintf("%v", val)
					}
				}
			}
		}
		evaluator := qsexpr.CatalogExpressionEvaluator{}
		cell, diag := evaluator.EvaluateDefault(qsbridge.ColumnDefaultExpression(a.DefaultValue), rm)
		val = cell.Value
		ok = diag.BlocksNative()
		/*
			var ctx *datasource.ContextSimple
			if r != nil {
				ctx = datasource.NewContextSimpleNative(rm)
			}
			exprNode, _ := expr.ParseExpression(a.DefaultValue)
			val, ok = vm.Eval(ctx, exprNode)
			if !ok {
				if exprNode != nil {
					switch exprNode.NodeType() {
					case "Func", "Identity":
						return nil
					}
				}
				val = value.NewValue(a.DefaultValue)
			}
		*/
	}
	//return fmt.Sprintf("%v", val.Value())
	return fmt.Sprintf("%v", val)
}

// Complete handling of primary key.
//  1. Uniqueness check against value in KVStore
//  2. ColumnID establishment for all fields in this row.  Generate if provided value = 0
//  3. Value mapping.
func (s *Session) processPrimaryKey(tbuf *TableBuffer, row interface{}, pqTablePath string,
	providedColID uint64, isChild, ignoreSourcePath, useNerdCapitalization bool,
	primaryKeyMode PrimaryKeyMode) (putRowIdentity, error) {

	if tbuf.Table.TimeQuantumType == "" {
		tbuf.CurrentTimestamp = time.Unix(0, 0)
	}

	directColumnID := false
	tbuf.CurrentPKValue = make([]interface{}, len(tbuf.PKAttributes))
	pqColPaths := make([]string, len(tbuf.PKAttributes))
	var pkLookupVal strings.Builder
	for i, pk := range tbuf.PKAttributes {
		var cval interface{}
		vals, pqps, err := s.readColumn(row, pqTablePath, pk, isChild, ignoreSourcePath, useNerdCapitalization)
		if err != nil {
			return putRowIdentity{}, fmt.Errorf("readColumn for PK - %v", err)
		}
		pqColPaths[i] = pqps[0]
		if vals == nil || len(vals) == 0 || (len(vals) == 1 && vals[0] == nil) {
			if isChild { // Nothing to do here, no child value
				return putRowIdentity{}, nil
			}
			return putRowIdentity{}, fmt.Errorf("empty or nil value for PK field %s - %s, len %d", pk.FieldName, pqColPaths[i],
				len(vals))
		}
		if len(vals) > 1 {
			return putRowIdentity{}, fmt.Errorf("multiple values for PK field %s [%v], Schema mapping issue?",
				pqColPaths[0], err)
		}
		cval = vals[0]
		tbuf.CurrentPKValue[i] = cval

		var strVal string
		mval, err := pk.MapValue(cval, nil, false)
		if err != nil {
			return putRowIdentity{}, fmt.Errorf("error mapping PK field %s [%v], Schema mapping issue?",
				pqColPaths[0], err)
		}
		switch shared.TypeFromString(pk.Type) {
		case shared.String:
			var ok bool
			if strVal, ok = cval.(string); !ok {
				strVal = pk.Render(mval)
			}
		case shared.Date, shared.DateTime:
			strVal = pk.Render(mval)
			if i == 0 { // First field in PK is TQ (if TQ != "")
				tbuf.CurrentTimestamp, _, _ = shared.ToTQTimestamp(tbuf.Table.TimeQuantumType, strVal)
			}
			if pk.ColumnID {
				if cID, err := strconv.ParseInt(cval.(string), 10, 64); err == nil {
					tbuf.CurrentColumnID = uint64(cID)
					directColumnID = true
				}
			}
		case shared.Integer:
			strVal = pk.Render(mval)
			if pk.ColumnID {
				if cID, err := strconv.ParseInt(strVal, 10, 64); err == nil {
					tbuf.CurrentColumnID = uint64(cID)
					directColumnID = true
				}
			}
		default:
			strVal = pk.Render(mval)
		}

		if pkLookupVal.Len() == 0 {
			pkLookupVal.WriteString(strVal)
		} else {
			pkLookupVal.WriteString(fmt.Sprintf("+%s", strVal))
		}
	}

	updateExisting, primaryKeyProfile, err := s.resolvePrimaryKeyColumnID(
		tbuf, pkLookupVal.String(), providedColID, directColumnID, primaryKeyMode)
	if err != nil {
		return putRowIdentity{}, err
	}
	if updateExisting {
		return putRowIdentity{
			columnID:          tbuf.CurrentColumnID,
			timestamp:         tbuf.CurrentTimestamp,
			primaryKeyValues:  tbuf.CurrentPKValue,
			lookupValue:       pkLookupVal.String(),
			updateExisting:    true,
			primaryKeyProfile: primaryKeyProfile,
		}, nil
	}

	// Map the value(s) and update table
	for i, v := range tbuf.CurrentPKValue {
		if v == nil {
			return putRowIdentity{}, fmt.Errorf("PK mapping error %s - nil value", pqColPaths[i])
		}
		if _, err := tbuf.PKAttributes[i].MapValue(v, s, false); err != nil {
			return putRowIdentity{}, fmt.Errorf("PK mapping error %s - %v", pqColPaths[i], err)
		}
	}

	return putRowIdentity{
		columnID:          tbuf.CurrentColumnID,
		timestamp:         tbuf.CurrentTimestamp,
		primaryKeyValues:  tbuf.CurrentPKValue,
		lookupValue:       pkLookupVal.String(),
		primaryKeyProfile: primaryKeyProfile,
	}, nil
}

func (s *Session) resolvePrimaryKeyColumnID(tbuf *TableBuffer, lookupValue string, providedColID uint64,
	directColumnID bool, primaryKeyMode PrimaryKeyMode) (bool, PrimaryKeyResolveProfile, error) {

	resolver := s.primaryKeyColumnIDResolver()
	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          s,
		TableBuffer:      tbuf,
		LookupValue:      lookupValue,
		PrimaryKeyValues: append([]interface{}(nil), tbuf.CurrentPKValue...),
		ProvidedColumnID: providedColID,
		DirectColumnID:   directColumnID,
		PrimaryKeyMode:   primaryKeyMode.normalize(),
	})
	if err != nil {
		return false, result.Profile, err
	}
	if result.ColumnID != 0 {
		tbuf.CurrentColumnID = result.ColumnID
	}
	return result.ExistingRow, result.Profile, nil
}

func (s *Session) primaryKeyColumnIDResolver() PrimaryKeyResolver {
	if s.primaryKeyResolver != nil {
		return s.primaryKeyResolver
	}
	return KVPrimaryKeyResolver{}
}

// SetPrimaryKeyResolver replaces the resolver used for primary-key lookup and
// rownum assignment. Passing nil restores the temporary KV-backed fallback;
// new write paths should inject an explicit resolver instead.
func (s *Session) SetPrimaryKeyResolver(resolver PrimaryKeyResolver) {
	if resolver == nil {
		s.primaryKeyResolver = KVPrimaryKeyResolver{}
		return
	}
	s.primaryKeyResolver = resolver
}

func (KVPrimaryKeyResolver) ResolvePrimaryKeyColumnID(req PrimaryKeyResolveRequest) (PrimaryKeyResolveResult, error) {
	totalStart := time.Now()
	profile := PrimaryKeyResolveProfile{ResolveCount: 1}

	finish := func(columnID uint64, existingRow bool) PrimaryKeyResolveResult {
		profile.TotalElapsed = time.Since(totalStart)
		return PrimaryKeyResolveResult{
			ColumnID:    columnID,
			ExistingRow: existingRow,
			Profile:     profile,
		}
	}

	session := req.Session
	tbuf := req.TableBuffer
	if session == nil {
		return finish(0, false), fmt.Errorf("primary key resolver session is nil")
	}
	if tbuf == nil {
		return finish(0, false), fmt.Errorf("primary key resolver table buffer is nil")
	}
	if tbuf.ShouldLookupPrimaryKey() {
		profile.LookupRequiredCount++
		localKey := indexPath(tbuf, tbuf.PKAttributes[0].FieldName, tbuf.Table.PrimaryKey+".PK")
		if req.PrimaryKeyMode.assumeNew() {
			profile.AssumeNewCount++
			profile.SkippedLocalCacheLookupCount++
			profile.SkippedKVLookupCount++
		} else {
			// Can't use batch operation here unfortunately, but at least we have local batch cache.
			localLookupStart := time.Now()
			profile.LocalCacheLookupCount++
			if lColID, ok := session.BatchBuffer.LookupLocalCIDForString(localKey, req.LookupValue); ok {
				profile.LocalCacheLookupElapsed += time.Since(localLookupStart)
				profile.LocalCacheHitCount++
				tbuf.CurrentColumnID = lColID
				u.Warnf("PK %s found in cache.  PK mapping error for %s?", req.LookupValue, tbuf.Table.Name)
				return finish(tbuf.CurrentColumnID, false), nil
			}
			profile.LocalCacheLookupElapsed += time.Since(localLookupStart)
			kvLookupStart := time.Now()
			profile.KVLookupCount++
			colID, found, err := session.lookupColumnID(tbuf, req.LookupValue, "")
			profile.KVLookupElapsed += time.Since(kvLookupStart)
			if err != nil {
				return finish(0, false), fmt.Errorf("Dedup lookup error - %v", err)
			}
			if found {
				profile.KVHitCount++
				tbuf.CurrentColumnID = colID
				return finish(colID, true), nil
			}
		}
		if req.ProvidedColumnID == 0 {
			allocationStart := time.Now()
			profile.RownumAllocationCount++
			if err := tbuf.NextColumnID(session.BitIndex); err != nil {
				profile.RownumAllocationElapsed += time.Since(allocationStart)
				return finish(0, false), err
			}
			profile.RownumAllocationElapsed += time.Since(allocationStart)
		} else {
			profile.ProvidedColumnIDCount++
			tbuf.CurrentColumnID = req.ProvidedColumnID
		}
		// Add the PK via local cache batch operation.
		batchCacheWriteStart := time.Now()
		profile.BatchCacheWriteCount++
		session.BatchBuffer.SetPartitionedString(localKey, req.LookupValue, tbuf.CurrentColumnID)
		profile.BatchCacheWriteElapsed += time.Since(batchCacheWriteStart)
		return finish(tbuf.CurrentColumnID, false), nil
	}

	if req.DirectColumnID {
		profile.DirectColumnIDCount++
		return finish(tbuf.CurrentColumnID, false), nil
	}
	if req.ProvidedColumnID == 0 {
		allocationStart := time.Now()
		profile.RownumAllocationCount++
		if err := tbuf.NextColumnID(session.BitIndex); err != nil {
			profile.RownumAllocationElapsed += time.Since(allocationStart)
			return finish(0, false), err
		}
		profile.RownumAllocationElapsed += time.Since(allocationStart)
	} else {
		profile.ProvidedColumnIDCount++
		tbuf.CurrentColumnID = req.ProvidedColumnID
	}
	return finish(tbuf.CurrentColumnID, false), nil
}

// Handle Secondary Keys.  Create the index in backing store
func (s *Session) processAlternateKeys(tbuf *TableBuffer, row interface{}, pqTablePath string,
	isChild, ignoreSourcePath, useNerdCapitalization bool) error {

	pqColPaths := make([]string, len(tbuf.SKMap))
	var skLookupVal strings.Builder
	i := 0
	for k, keyAttrs := range tbuf.SKMap {
		for _, v := range keyAttrs {
			var cval interface{}
			vals, pqps, err := s.readColumn(row, pqTablePath, v, isChild, ignoreSourcePath, useNerdCapitalization)
			if err != nil {
				return fmt.Errorf("readColumn for SK - %v", err)
			}
			pqColPaths[i] = pqps[0]
			if vals == nil || len(vals) == 0 || (len(vals) == 1 && vals[0] == nil) {

				if isChild { // Nothing to do here, no child value
					return nil
				}
				return fmt.Errorf("Empty or nil value for SK field %s, len %d", pqColPaths[i],
					len(vals))
			}
			if len(vals) > 1 {
				return fmt.Errorf("Multiple values for SK field %s [%v], Schema mapping issue?",
					pqColPaths[0], err)
			}
			cval = vals[0]

			switch reflect.ValueOf(cval).Kind() {
			case reflect.String:
				// Do nothing already a string
				if v.MappingStrategy == "TimestampBSI" || v.MappingStrategy == "TimeStampBSI" ||
					v.MappingStrategy == "SysMillisBSI" || v.MappingStrategy == "SysMicroBSI" {
					strVal := cval.(string)
					loc, _ := time.LoadLocation("Local")
					ts, err := dateparse.ParseIn(strVal, loc)
					if err != nil {
						return fmt.Errorf("Date parse error for SK field %s - value %s - %v",
							pqColPaths[i], strVal, err)
					}
					cval = fmt.Sprintf("%d", ts.UnixNano())
				}
			case reflect.Int64:
				orig := cval.(int64)
				cval = fmt.Sprintf("%d", orig)

			default:
				return fmt.Errorf("SK Lookup value [%v] unknown type, it is [%v]", cval,
					reflect.ValueOf(cval).Kind())
			}
			if skLookupVal.Len() == 0 {
				skLookupVal.WriteString(cval.(string))
			} else {
				skLookupVal.WriteString(fmt.Sprintf("+%s", cval.(string)))
			}
		}
		//s.BatchBuffer.SetKeyString(tbuf.Table.Name, k, secondaryKey, skLookupVal.String(),
		//	tbuf.CurrentColumnID)
		lookupKey := indexPath(tbuf, tbuf.PKAttributes[0].FieldName, k+".SK")
		s.BatchBuffer.SetPartitionedString(lookupKey, skLookupVal.String(), tbuf.CurrentColumnID)
		i++
	}
	return nil
}

func (s *Session) lookupColumnID(tbuf *TableBuffer, lookupVal, fkFieldSpec string) (uint64, bool, error) {

	kvIndex := indexPath(tbuf, tbuf.PKAttributes[0].FieldName, tbuf.Table.PrimaryKey+".PK")

	if fkFieldSpec != "" {
		// Use the secondary/alternate key specification.  In this case tbuf is the FK table
		kvIndex = indexPath(tbuf, tbuf.PKAttributes[0].FieldName, fkFieldSpec+".SK")
	}
	kvResult, err := s.KVStore.Lookup(kvIndex, lookupVal, reflect.Uint64, true)
	if err != nil {
		return 0, false, fmt.Errorf("KVStore error for [%s] = [%s], [%v]", kvIndex, lookupVal, err)
	}
	if kvResult == nil {
		return 0, false, nil
	}
	return kvResult.(uint64), true, nil
}

func indexPath(tbuf *TableBuffer, field, path string) string {

	lookupPath := fmt.Sprintf("%s/%s/%s,%s", tbuf.Table.Name, field, path,
		formatShardTime(tbuf.CurrentTimestamp))
	if tbuf.Table.TimeQuantumType == "YMDH" {
		ts := tbuf.CurrentTimestamp
		key := fmt.Sprintf("%s/%s/%s", tbuf.Table.Name, field, formatShardTime(ts))
		utcTime := ts.UTC()
		fpath := fmt.Sprintf("/%s/%s/%s/%s/%s", tbuf.Table.Name, field, path,
			fmt.Sprintf("%d%02d%02d", utcTime.Year(), utcTime.Month(), utcTime.Day()), formatShardTime(ts))
		lookupPath = key + "," + fpath
	}
	return lookupPath
}

// LookupKeyBatch - Process a batch of keys.
/*
func (s *Session) LookupKeyBatch(tbuf *TableBuffer, lookupVals map[interface{}]interface{},
	fkFieldSpec string) (map[interface{}]interface{}, error) {

	kvIndex := fmt.Sprintf("%s%s%s.PK", tbuf.Table.Name, ifDelim, tbuf.Table.PrimaryKey)
	if fkFieldSpec != "" {
		// Use the secondary/alternate key specification
		kvIndex = fmt.Sprintf("%s%s%s.SK", tbuf.Table.Name, ifDelim, fkFieldSpec)
	}
	lookupVals, err := s.KVStore.BatchLookup(kvIndex, lookupVals, false)
	if err != nil {
		return nil, fmt.Errorf("KVStore.LookupBatch error for [%s] - [%v]", kvIndex, err)
	}
	return lookupVals, nil
}
*/

func (s *Session) resolveFKLookupKey(v *Attribute, tbuf *TableBuffer, row interface{},
	ignoreSourcePath, useNerdCapitalization bool) (string, error) {

	var retVal strings.Builder
	root := "/"
	pqTablePath := fmt.Sprintf("%s%s", root, tbuf.Table.Name)
	if r, ok := row.(*reader.ParquetReader); ok {
		root = r.SchemaHandler.GetRootExName()
		pqTablePath = fmt.Sprintf("%s.%s", root, tbuf.Table.Name)
	}
	vals, _, err := s.readColumn(row, pqTablePath, v, false, ignoreSourcePath, useNerdCapitalization)
	if err != nil {
		return "", err
	}
	for _, val := range vals {
		if val != nil {
			if retVal.Len() == 0 {
				retVal.WriteString(fmt.Sprintf("%v", val))
			} else {
				retVal.WriteString(fmt.Sprintf("+%v", val))
			}
		}
	}
	return retVal.String(), nil
}

// ResetRowCache - Clear cache.
func (s *Session) ResetRowCache() {
	for _, v := range s.TableBuffers {
		v.rowCache = make(map[string]interface{})
	}
	s.BytesRead = 0
}

// Flushing - Flush in progress
func (s *Session) IsFlushing() bool {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.flushing
}

func (s *Session) flush() error {

	if s.BatchBuffer != nil && !s.BatchBuffer.IsEmpty() {
		s.flushing = true
		defer func() { s.flushing = false }()
		start := time.Now()
		fb := shared.NewBatchBuffer(s.BitIndex, s.KVStore, batchBufferSize)
		s.BatchBuffer.MergeInto(fb)
		mergeTime := time.Since(start)
		s.BatchBuffer = shared.NewBatchBuffer(s.BitIndex, s.KVStore, batchBufferSize)
		if err := fb.Flush(); err != nil {
			s.lastFlushProfile = fb.LastFlushProfile()
			u.Error(err)
			return err
		}
		s.lastFlushProfile = fb.LastFlushProfile()
		duration := time.Since(start)
		if duration > time.Duration(30*time.Second) {
			u.Debugf("FLUSH DURATION %v, MERGE TIME = %v", duration, mergeTime)
		}
	}
	if s.StringIndex != nil {
		if err := s.StringIndex.Flush(); err != nil {
			u.Error(err)
			return err
		}
	}
	return nil
}

// Flush - Flush data to backend.
func (s *Session) Flush() error {

	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.flush()
}

// LastFlushProfile returns the most recent BatchBuffer flush profile observed
// by this session.
func (s *Session) LastFlushProfile() shared.BatchBufferFlushProfile {
	if s == nil {
		return shared.BatchBufferFlushProfile{}
	}
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.lastFlushProfile
}

// CloseSession - Close the session, flushing if necessary..
func (s *Session) CloseSession() error {

	if s == nil {
		u.Warn("attempt to close a session already closed")
		return nil
	}
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	err := s.flush()
	if err == nil {
		s.StringIndex = nil
		s.BitIndex = nil
	}
	return err

}

// UpdateRow - Perform an in-place update of a row.
func (s *Session) UpdateRow(table string, columnID uint64, updValueMap map[string]*qsbridge.ResultCell,
	timePartition time.Time) error {

	tbuf, ok := s.TableBuffers[table]
	if !ok {
		return fmt.Errorf("table %s is not open for this session", table)
	}
	tbuf.CurrentColumnID = columnID
	tbuf.CurrentTimestamp = timePartition
	for k, vc := range updValueMap {
		if _, found := tbuf.PKMap[k]; found {
			return fmt.Errorf("cannot update PK column %s.%s", table, k)
		}
		//_, err := s.MapValue(table, k, vc.Value.Value(), true)
		_, err := s.MapValue(table, k, vc.Value, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Commit - Block until the server nodes have persisted their work queues to a savepoint.
func (s *Session) Commit() error {

	if s.BitIndex == nil {
		return fmt.Errorf("attempting commit of a closed session")
	}

	return s.BitIndex.Commit()

}

// MapValue - Convenience function for Mapper interface.
func (s *Session) MapValue(tableName, fieldName string, value interface{}, update bool) (val *big.Int, err error) {

	var table *Table
	var attr *Attribute
	table, err = LoadTable(s.tableCache, s.BasePath, s.KVStore, tableName, s.KVStore.Conn.Consul)
	if err != nil {
		return
	}
	attr, err = table.GetAttribute(fieldName)
	if err != nil {
		return nil, fmt.Errorf("attribute '%s' not found", fieldName)
	}

	/*
		if attr.SkipIndex {
			if update {
				return 0, nil
			} else {
				return 0, fmt.Errorf("attribute '%s' is not indexed and can't be used in a query", fieldName)
			}
		}
	*/
	if update {
		return attr.MapValue(value, s, update)
	}
	return attr.MapValue(value, nil, update) // Non load use case pass nil connection context
}

func fromJulianDay(days int32, nanos int64) time.Time {
	nanos = ((int64(days)-julianDayOfEpoch)*microsPerDay + nanos/1000) * 1000
	sec, nsec := nanos/time.Second.Nanoseconds(), nanos%time.Second.Nanoseconds()
	t := time.Unix(sec, nsec)
	return t.UTC()
}

// INT96ToTime - Handle parquet INT96 values.
func INT96ToTime(int96 string) time.Time {
	nanos := binary.LittleEndian.Uint64([]byte(int96[:8]))
	days := binary.LittleEndian.Uint32([]byte(int96[8:]))
	return fromJulianDay(int32(days), int64(nanos))
}
