package core

import (
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingPrimaryKeyResolver struct {
	called  bool
	request PrimaryKeyResolveRequest
	result  PrimaryKeyResolveResult
	err     error
}

func (r *recordingPrimaryKeyResolver) ResolvePrimaryKeyColumnID(req PrimaryKeyResolveRequest) (PrimaryKeyResolveResult, error) {
	r.called = true
	r.request = req
	if r.result.ColumnID != 0 && req.TableBuffer != nil {
		req.TableBuffer.CurrentColumnID = r.result.ColumnID
	}
	return r.result, r.err
}

type recordingPrimaryKeyResolverRequests struct {
	requests []PrimaryKeyResolveRequest
	result   PrimaryKeyResolveResult
	err      error
}

func (r *recordingPrimaryKeyResolverRequests) ResolvePrimaryKeyColumnID(req PrimaryKeyResolveRequest) (PrimaryKeyResolveResult, error) {
	r.requests = append(r.requests, req)
	if r.result.ColumnID != 0 && req.TableBuffer != nil {
		req.TableBuffer.CurrentColumnID = r.result.ColumnID
	}
	return r.result, r.err
}

type recordingMapper struct {
	values   []interface{}
	sessions []*Session
	err      error
}

func (m *recordingMapper) Transform(_ *Attribute, val interface{}, _ *Session) (interface{}, error) {
	return val, nil
}

func (m *recordingMapper) MapValue(_ *Attribute, val interface{}, session *Session, _ bool) (*big.Int, error) {
	m.values = append(m.values, val)
	m.sessions = append(m.sessions, session)
	return big.NewInt(1), m.err
}

func (m *recordingMapper) MapValueReverse(_ *Attribute, _ uint64, _ *Session) (interface{}, error) {
	return nil, nil
}

func (m *recordingMapper) Render(_ *Attribute, value interface{}) string {
	return value.(string)
}

func (m *recordingMapper) GetMultiDelimiter() string {
	return ";"
}

// FIXME: make this work or delete. It never finishes. (nobody home at port 4000)
func xTestCreateSession(t *testing.T) {

	conn := shared.NewDefaultConnection("xTestCreateSession")
	conn.ServicePort = 0
	errx := conn.Connect(nil)
	assert.Nil(t, errx)
	tableCache := NewTableCacheStruct()
	c, err := OpenSession(tableCache, "./testdata", "cities", false, conn)
	assert.Nil(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, c.TableBuffers)
	assert.Equal(t, len(c.TableBuffers), 1)
	assert.NotNil(t, c.TableBuffers["cities"])
}

// func TestCreateRecursiveSession(t *testing.T) {
// 	os.RemoveAll("./testdata/metadata/user")
// 	os.RemoveAll("./testdata/metadata/events")
// 	os.RemoveAll("./testdata/metadata/ab_test")
// 	os.RemoveAll("./testdata/metadata/dss_id")
// 	os.RemoveAll("./testdata/metadata/guest_id")
// 	os.RemoveAll("./testdata/metadata/anonymous_id")
// 	os.RemoveAll("./testdata/metadata/session_id")
// 	os.RemoveAll("./testdata/metadata/subscription_id")
// 	c, err := OpenSession("./testdata", "./testdata/metadata", "user", true, 0, 0, nil)
// 	assert.Nil(t, err)
// 	assert.NotNil(t, c)
// 	assert.Equal(t, len(c.TableBuffers), 6)
// }

func TestNormalizePutRowSourceCachesMapRow(t *testing.T) {
	session := &Session{
		TableBuffers: map[string]*TableBuffer{
			"customers": {},
		},
	}
	row := map[string]interface{}{
		"id":   "1",
		"name": "Alice",
	}
	req := putRowRequest{
		tableName: "customers",
		row:       row,
	}

	err := session.normalizePutRowSource(&req)

	require.NoError(t, err)
	assert.Equal(t, "/", req.pqTablePath)
	assert.Equal(t, row, session.TableBuffers["customers"].rowCache)
}

func TestNormalizePutRowSourceRejectsMissingTableBuffer(t *testing.T) {
	session := &Session{TableBuffers: map[string]*TableBuffer{}}
	req := putRowRequest{
		tableName: "customers",
		row:       map[string]interface{}{"id": "1"},
	}

	err := session.normalizePutRowSource(&req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot locate buffer for table customers")
}

func TestResolveParentRelationColumnIDUsesParentCurrentColumnIDForChildRows(t *testing.T) {
	parentTable := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "o_orderkey"}}
	childTable := &Table{BasicTable: &shared.BasicTable{Name: "lineitem", PrimaryKey: "l_linenumber"}}
	parentBuffer := &TableBuffer{Table: parentTable, CurrentColumnID: 4242}
	childBuffer := &TableBuffer{Table: childTable}
	childForeignKey := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "l_orderkey",
			SourceName:      "l_orderkey",
			Type:            "Integer",
			MappingStrategy: "ParentRelation",
			ForeignKey:      "orders.o_orderkey",
		},
		Parent: childTable,
	}
	session := &Session{}

	columnID, ok, err := session.resolveParentRelationColumnID(putRowRequest{
		isChild:        true,
		primaryKeyMode: PrimaryKeyModeAssumeNew,
	}, childBuffer, parentBuffer, childForeignKey, "o_orderkey")

	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(4242), columnID)
}

func TestExpandChildRowsPropagatesAssumeNewPrimaryKeyMode(t *testing.T) {
	parentTable := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "o_orderkey"}}
	childTable := &Table{BasicTable: &shared.BasicTable{Name: "lineitem", PrimaryKey: "lineitem_id"}}
	childRelation := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "lineitems",
			SourceName:      "lineitems",
			MappingStrategy: "ChildRelation",
			ChildTable:      "lineitem",
		},
		Parent: parentTable,
	}
	childPKMapper, err := NewStringLexBSIMapper(map[string]string{"length": "8"})
	require.NoError(t, err)
	childPK := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "lineitem_id",
			SourceName:      "lineitem_id",
			Type:            "String",
			MappingStrategy: "StringLexBSI",
		},
		Parent:         childTable,
		mapperInstance: childPKMapper,
	}
	parentRow := map[string]interface{}{
		"o_orderkey": "order-1",
		"lineitems": []interface{}{
			map[string]interface{}{"lineitem_id": "line-1"},
			map[string]interface{}{"lineitem_id": "line-2"},
		},
	}
	childBuffer := &TableBuffer{
		Table:        childTable,
		PKAttributes: []*Attribute{childPK},
		PKMap:        map[string]*Attribute{"lineitem_id": childPK},
		rowCache:     map[string]interface{}{},
	}
	session := &Session{TableBuffers: map[string]*TableBuffer{
		"orders":   {Table: parentTable, CurrentColumnID: 9001, rowCache: parentRow},
		"lineitem": childBuffer,
	}}
	resolver := &recordingPrimaryKeyResolverRequests{
		result: PrimaryKeyResolveResult{
			ColumnID:    17,
			ExistingRow: true,
			Profile:     PrimaryKeyResolveProfile{ResolveCount: 1},
		},
	}
	session.SetPrimaryKeyResolver(resolver)
	timings := &putRowStageTimings{}

	err = session.expandChildRows(putRowRequest{
		row:            parentRow,
		primaryKeyMode: PrimaryKeyModeAssumeNew,
		timings:        timings,
	}, session.TableBuffers["orders"], childRelation)

	require.NoError(t, err)
	require.Len(t, resolver.requests, 2)
	assert.Equal(t, PrimaryKeyModeAssumeNew, resolver.requests[0].PrimaryKeyMode)
	assert.Equal(t, PrimaryKeyModeAssumeNew, resolver.requests[1].PrimaryKeyMode)
	assert.Equal(t, "line-1", resolver.requests[0].LookupValue)
	assert.Equal(t, "line-2", resolver.requests[1].LookupValue)
	assert.Equal(t, 2, timings.childRowCount)
	assert.Greater(t, timings.childTraversalElapsed, time.Duration(0))
	assert.Equal(t, 2, timings.primaryKeyProfile.ResolveCount)
}

func TestPutRowWithOptionsRejectsUnsupportedRowType(t *testing.T) {
	session := &Session{TableBuffers: map[string]*TableBuffer{}}

	result, err := session.PutRowWithOptions("customers", struct{}{}, 0, false, false, PutRowOptions{
		EventID: "event-1",
		Source:  "unit-test",
	})

	require.Error(t, err)
	assert.Equal(t, PutRowResult{}, result)
	assert.Contains(t, err.Error(), "cannot process row type")
}

func TestPutRowWithOptionsUsesTypedCompoundBSIPrimaryKeyIdentity(t *testing.T) {
	backend := newMapBSIPrimaryKeyBackend()
	session := newCompoundStringPrimaryKeyTestSession()
	session.SetPrimaryKeyResolver(NewBSIPrimaryKeyResolver(backend))

	first, err := session.PutRowWithOptions("compound", map[string]interface{}{
		"left_value":  "x+0",
		"right_value": "y",
	}, 42, false, false, PutRowOptions{PrimaryKeyMode: PrimaryKeyModeAssumeNew})
	require.NoError(t, err)
	assert.True(t, first.Inserted)
	assert.False(t, first.ExistingRow)
	assert.Equal(t, uint64(42), first.ColumnID)
	assert.Equal(t, 1, first.PrimaryKey.BSIStageWriteCount)

	second, err := session.PutRowWithOptions("compound", map[string]interface{}{
		"left_value":  "x",
		"right_value": "0+y",
	}, 99, false, false, PutRowOptions{PrimaryKeyMode: PrimaryKeyModeAssumeNew})
	require.NoError(t, err)
	assert.True(t, second.Inserted)
	assert.False(t, second.ExistingRow)
	assert.Equal(t, uint64(99), second.ColumnID)
	assert.Equal(t, 1, second.PrimaryKey.BSIStageWriteCount)

	replay, err := session.PutRowWithOptions("compound", map[string]interface{}{
		"left_value":  "x+0",
		"right_value": "y",
	}, 0, false, false, PutRowOptions{})
	require.NoError(t, err)
	assert.False(t, replay.Inserted)
	assert.True(t, replay.ExistingRow)
	assert.Equal(t, uint64(42), replay.ColumnID)
	assert.Equal(t, 1, replay.PrimaryKey.LocalCacheLookupCount)
	assert.Equal(t, 1, replay.PrimaryKey.LocalCacheHitCount)
	assert.Zero(t, replay.PrimaryKey.BSILookupCount)
	assert.Zero(t, replay.PrimaryKey.BSIHitCount)
	assert.Zero(t, replay.PrimaryKey.BSIStageWriteCount)
}

func TestPutRowResultIncludesStageTimings(t *testing.T) {
	req := putRowRequest{
		tableName: "customers",
		timings: &putRowStageTimings{
			sourceElapsed:         time.Millisecond,
			identityElapsed:       2 * time.Millisecond,
			childExpansionElapsed: 4 * time.Millisecond,
			childTraversalElapsed: 1500 * time.Microsecond,
			relationElapsed:       5 * time.Millisecond,
			attributeElapsed:      6 * time.Millisecond,
			childRowCount:         7,
		},
	}
	tbuf := &TableBuffer{CurrentColumnID: 42}
	identity := putRowIdentity{updateExisting: true}

	result := req.putRowResult(tbuf, identity, 21*time.Millisecond)

	assert.Equal(t, PutRowResult{
		TableName:             "customers",
		ColumnID:              42,
		ChildRowCount:         7,
		LogicalRowCount:       8,
		ExistingRow:           true,
		SourceElapsed:         time.Millisecond,
		IdentityElapsed:       2 * time.Millisecond,
		ChildExpansionElapsed: 4 * time.Millisecond,
		ChildTraversalElapsed: 1500 * time.Microsecond,
		RelationElapsed:       5 * time.Millisecond,
		AttributeElapsed:      6 * time.Millisecond,
		TotalElapsed:          21 * time.Millisecond,
	}, result)
}

func TestRunPutRowPipelineRunsStagesInOrderAndRecordsTimings(t *testing.T) {
	session := &Session{}
	req := putRowRequest{timings: &putRowStageTimings{}}
	var order []putRowStageName

	err := session.runPutRowPipeline(req,
		putRowPipelineStage{
			name: putRowStageIdentity,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.identityElapsed += elapsed
			},
			run: func() error {
				time.Sleep(time.Nanosecond)
				order = append(order, putRowStageIdentity)
				return nil
			},
		},
		putRowPipelineStage{
			name: putRowStageChildExpansion,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.childExpansionElapsed += elapsed
			},
			run: func() error {
				time.Sleep(time.Nanosecond)
				order = append(order, putRowStageChildExpansion)
				return nil
			},
		},
		putRowPipelineStage{
			name: putRowStageParentRelations,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.relationElapsed += elapsed
			},
			run: func() error {
				time.Sleep(time.Nanosecond)
				order = append(order, putRowStageParentRelations)
				return nil
			},
		},
		putRowPipelineStage{
			name: putRowStageAttributes,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.attributeElapsed += elapsed
			},
			run: func() error {
				time.Sleep(time.Nanosecond)
				order = append(order, putRowStageAttributes)
				return nil
			},
		},
	)

	require.NoError(t, err)
	assert.Equal(t, []putRowStageName{
		putRowStageIdentity,
		putRowStageChildExpansion,
		putRowStageParentRelations,
		putRowStageAttributes,
	}, order)
	assert.Greater(t, req.timings.identityElapsed, time.Duration(0))
	assert.Greater(t, req.timings.childExpansionElapsed, time.Duration(0))
	assert.Greater(t, req.timings.relationElapsed, time.Duration(0))
	assert.Greater(t, req.timings.attributeElapsed, time.Duration(0))
}

func TestRunPutRowPipelineStopsOnErrorAndRecordsFailedStageTiming(t *testing.T) {
	session := &Session{}
	req := putRowRequest{timings: &putRowStageTimings{}}
	stageErr := errors.New("identity failed")
	secondStageRan := false

	err := session.runPutRowPipeline(req,
		putRowPipelineStage{
			name: putRowStageIdentity,
			record: func(t *putRowStageTimings, elapsed time.Duration) {
				t.identityElapsed += elapsed
			},
			run: func() error {
				time.Sleep(time.Nanosecond)
				return stageErr
			},
		},
		putRowPipelineStage{
			name: putRowStageChildExpansion,
			run: func() error {
				secondStageRan = true
				return nil
			},
		},
	)

	require.ErrorIs(t, err, stageErr)
	assert.False(t, secondStageRan)
	assert.Greater(t, req.timings.identityElapsed, time.Duration(0))
}

func TestIngestRecordBuildsPutRowOptions(t *testing.T) {
	eventTime := time.Date(2026, 8, 4, 10, 11, 12, 0, time.UTC)
	record := IngestRecord{
		EventID:      "evt-1",
		Source:       "tpch-stream",
		EventTime:    eventTime,
		SourceOffset: "partition-1:99",
		PayloadHash:  12345,
		DedupTTL:     48 * time.Hour,
	}

	assert.Equal(t, PutRowOptions{
		EventID:      "evt-1",
		Source:       "tpch-stream",
		EventTime:    eventTime,
		SourceOffset: "partition-1:99",
		PayloadHash:  12345,
		DedupTTL:     48 * time.Hour,
	}, record.PutRowOptions())
}

func TestPrimaryKeyModeDefaultsToVerifyExisting(t *testing.T) {
	assert.Equal(t, PrimaryKeyModeVerifyExisting, PrimaryKeyMode("").Normalize())
	assert.Equal(t, PrimaryKeyModeVerifyExisting, PrimaryKeyMode("surprise").Normalize())
	assert.Equal(t, PrimaryKeyModeVerifyExisting, PrimaryKeyModeVerifyExisting.Normalize())
	assert.Equal(t, PrimaryKeyModeAssumeNew, PrimaryKeyMode("ASSUME_NEW").Normalize())
}

func TestMapAttributeValuesSkipsIdentityAndRelationshipFields(t *testing.T) {
	table := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "order_id"}}
	pk := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "order_id",
			SourceName:      "order_id",
			MappingStrategy: "StringLexBSI",
		},
		Parent: table,
	}
	parentRelation := Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "cust_id",
			SourceName:      "cust_id",
			MappingStrategy: "ParentRelation",
			ForeignKey:      "customer.c_custkey",
		},
		Parent: table,
	}
	childRelation := Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "lineitems",
			SourceName:      "lineitems",
			MappingStrategy: "ChildRelation",
			ChildTable:      "lineitem",
		},
		Parent: table,
	}
	scalarMapper := &recordingMapper{}
	scalar := Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "status",
			SourceName:      "status",
			MappingStrategy: "StringLexBSI",
		},
		Parent:         table,
		mapperInstance: scalarMapper,
	}
	table.Attributes = []Attribute{*pk, parentRelation, childRelation, scalar}
	tbuf := &TableBuffer{
		Table:    table,
		PKMap:    map[string]*Attribute{"order_id": pk},
		rowCache: map[string]interface{}{"status": "ready"},
	}
	session := &Session{TableBuffers: map[string]*TableBuffer{"orders": tbuf}}

	err := session.mapAttributeValues(putRowRequest{row: tbuf.rowCache}, tbuf, putRowIdentity{})

	require.NoError(t, err)
	assert.Equal(t, []interface{}{"ready"}, scalarMapper.values)
	require.Len(t, scalarMapper.sessions, 1)
	assert.Same(t, session, scalarMapper.sessions[0])
}

func TestResolvePrimaryKeyColumnIDUsesProvidedIDWhenLookupDisabled(t *testing.T) {
	session := &Session{}
	table := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "order_id"}}
	pk := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName: "order_id",
			ColumnID:  true,
		},
		Parent: table,
	}
	tbuf := &TableBuffer{
		Table:          table,
		PKAttributes:   []*Attribute{pk},
		CurrentPKValue: []interface{}{"1001"},
	}

	updateExisting, profile, err := session.resolvePrimaryKeyColumnID(tbuf, "1001", 99, false, "")

	require.NoError(t, err)
	assert.False(t, updateExisting)
	assert.Equal(t, uint64(99), tbuf.CurrentColumnID)
	assert.Equal(t, 1, profile.ResolveCount)
	assert.Equal(t, 1, profile.ProvidedColumnIDCount)
}

func TestResolvePrimaryKeyColumnIDPreservesDirectColumnID(t *testing.T) {
	session := &Session{}
	table := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "order_id"}}
	pk := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName: "order_id",
			ColumnID:  true,
		},
		Parent: table,
	}
	tbuf := &TableBuffer{Table: table, PKAttributes: []*Attribute{pk}, CurrentColumnID: 7}

	updateExisting, profile, err := session.resolvePrimaryKeyColumnID(tbuf, "1001", 99, true, "")

	require.NoError(t, err)
	assert.False(t, updateExisting)
	assert.Equal(t, uint64(7), tbuf.CurrentColumnID)
	assert.Equal(t, 1, profile.ResolveCount)
	assert.Equal(t, 1, profile.DirectColumnIDCount)
}

func TestResolvePrimaryKeyColumnIDDelegatesToConfiguredResolver(t *testing.T) {
	resolver := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{
			ColumnID:    55,
			ExistingRow: true,
			Profile: PrimaryKeyResolveProfile{
				ResolveCount:   1,
				BSILookupCount: 1,
				BSIHitCount:    1,
			},
		},
	}
	session := &Session{}
	session.SetPrimaryKeyResolver(resolver)
	table := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "order_id"}}
	pk := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName: "order_id",
			ColumnID:  true,
		},
		Parent: table,
	}
	tbuf := &TableBuffer{
		Table:          table,
		PKAttributes:   []*Attribute{pk},
		CurrentPKValue: []interface{}{"1001"},
	}

	updateExisting, profile, err := session.resolvePrimaryKeyColumnID(tbuf, "1001", 99, false, PrimaryKeyModeAssumeNew)

	require.NoError(t, err)
	assert.True(t, resolver.called)
	assert.True(t, updateExisting)
	assert.Equal(t, uint64(55), tbuf.CurrentColumnID)
	assert.Equal(t, resolver.result.Profile, profile)
	assert.Same(t, session, resolver.request.Session)
	assert.Same(t, tbuf, resolver.request.TableBuffer)
	assert.Equal(t, "1001", resolver.request.LookupValue)
	assert.Equal(t, []interface{}{"1001"}, resolver.request.PrimaryKeyValues)
	assert.Equal(t, uint64(99), resolver.request.ProvidedColumnID)
	assert.False(t, resolver.request.DirectColumnID)
	assert.Equal(t, PrimaryKeyModeAssumeNew, resolver.request.PrimaryKeyMode)
}

func TestResolvePrimaryKeyColumnIDDefaultsResolverRequestToVerifyExisting(t *testing.T) {
	resolver := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{
			ColumnID: 99,
			Profile:  PrimaryKeyResolveProfile{ResolveCount: 1},
		},
	}
	session := &Session{}
	session.SetPrimaryKeyResolver(resolver)
	table := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "order_id"}}
	pk := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName: "order_id",
			ColumnID:  true,
		},
		Parent: table,
	}
	tbuf := &TableBuffer{Table: table, PKAttributes: []*Attribute{pk}}

	_, _, err := session.resolvePrimaryKeyColumnID(tbuf, "1001", 0, false, "")

	require.NoError(t, err)
	require.True(t, resolver.called)
	assert.Equal(t, PrimaryKeyModeVerifyExisting, resolver.request.PrimaryKeyMode)
}

func TestSetPrimaryKeyResolverNilClearsAuthorityAndFailsClosed(t *testing.T) {
	session := &Session{}
	customResolver := &recordingPrimaryKeyResolver{}
	session.SetPrimaryKeyResolver(customResolver)
	session.SetPrimaryKeyResolver(nil)

	_, ok := session.primaryKeyColumnIDResolver().(MissingPrimaryKeyResolver)
	require.True(t, ok, "nil resolver should leave primary-key authority unconfigured")
}

func TestDeletedPrimaryKeyResolverSymbolDoesNotReturn(t *testing.T) {
	forbidden := strings.Join([]string{"KV", "Primary", "Key", "Resolver"}, "")
	var unexpected []string
	err := filepath.WalkDir("..", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "bin", "local", "node_modules", "tmp":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		rel, err := filepath.Rel("..", path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "core/session_test.go" {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(source), forbidden) {
			return nil
		}
		unexpected = append(unexpected, rel)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, unexpected, "deleted primary-key resolver symbol must not return")
}

func TestMissingPrimaryKeyResolverFailsClosed(t *testing.T) {
	table := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "order_id"}}
	tbuf := &TableBuffer{Table: table}
	session := &Session{}

	_, _, err := session.resolvePrimaryKeyColumnID(tbuf, "1001", 0, false, "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "primary key resolver is not configured")
	require.Contains(t, err.Error(), "orders")
}

func newCompoundStringPrimaryKeyTestSession() *Session {
	table := &Table{BasicTable: &shared.BasicTable{Name: "compound", PrimaryKey: "left_value+right_value"}}
	left := newCompoundStringPrimaryKeyTestAttribute(table, "left_value")
	right := newCompoundStringPrimaryKeyTestAttribute(table, "right_value")
	table.Attributes = []Attribute{*left, *right}
	tbuf := &TableBuffer{
		Table:        table,
		PKAttributes: []*Attribute{left, right},
		PKMap: map[string]*Attribute{
			"left_value":  left,
			"right_value": right,
		},
		rowCache: map[string]interface{}{},
	}
	return &Session{
		TableBuffers: map[string]*TableBuffer{"compound": tbuf},
		BatchBuffer:  shared.NewBatchBuffer(nil, nil, 1000),
	}
}

func newCompoundStringPrimaryKeyTestAttribute(table *Table, fieldName string) *Attribute {
	return &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       fieldName,
			SourceName:      fieldName,
			Type:            "String",
			MappingStrategy: "StringLexBSI",
		},
		Parent:         table,
		mapperInstance: &recordingMapper{},
	}
}

func TestReadColumnEvaluatesBlindDefaultExpression(t *testing.T) {
	session := &Session{}
	table := &Table{BasicTable: &shared.BasicTable{Name: "order_line"}}
	price := Attribute{BasicAttribute: &shared.BasicAttribute{FieldName: "price", SourceName: "price"}, Parent: table}
	quantity := Attribute{BasicAttribute: &shared.BasicAttribute{FieldName: "quantity", SourceName: "quantity"}, Parent: table}
	total := Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:    "total",
			SourceName:   "total",
			DefaultValue: "price * quantity",
		},
		Parent: table,
	}
	table.Attributes = []Attribute{price, quantity, total}
	tbuf := &TableBuffer{
		Table:    table,
		rowCache: map[string]interface{}{"price": 1.50, "quantity": 3},
	}
	session.TableBuffers = map[string]*TableBuffer{table.Name: tbuf}
	row := map[string]interface{}{"price": 1.50, "quantity": 3}

	values, paths, err := session.readColumn(row, "/", &total, false, false, false)

	require.NoError(t, err)
	require.Equal(t, []string{""}, paths)
	require.Len(t, values, 1)
	assert.Equal(t, "4.5", values[0])
}

func TestReadColumnExplicitValueOverridesDefaultExpression(t *testing.T) {
	session := &Session{}
	table := &Table{BasicTable: &shared.BasicTable{Name: "order_line"}}
	total := Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:    "total",
			SourceName:   "total",
			DefaultValue: "price * quantity",
		},
		Parent: table,
	}
	table.Attributes = []Attribute{total}
	tbuf := &TableBuffer{
		Table:    table,
		rowCache: map[string]interface{}{"total": 9.99},
	}
	session.TableBuffers = map[string]*TableBuffer{table.Name: tbuf}
	row := map[string]interface{}{"total": 9.99}

	values, _, err := session.readColumn(row, "/", &total, false, false, false)

	require.NoError(t, err)
	require.Len(t, values, 1)
	assert.Equal(t, "9.99", values[0])
}

func TestExpandChildRowsRequiresChildBufferForNestedArray(t *testing.T) {
	session := &Session{TableBuffers: map[string]*TableBuffer{}}
	table := &Table{BasicTable: &shared.BasicTable{Name: "orders"}}
	tbuf := &TableBuffer{
		Table: table,
		rowCache: map[string]interface{}{
			"lineitems": []interface{}{
				map[string]interface{}{"line_number": 1},
			},
		},
	}
	attr := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "lineitems",
			SourceName:      "lineitems",
			MappingStrategy: "ChildRelation",
			ChildTable:      "lineitem",
		},
		Parent: table,
	}

	err := session.expandChildRows(putRowRequest{row: tbuf.rowCache}, tbuf, attr)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "child table lineitem invalid or not opened")
}

func TestBuildPutRowChildPayloadDoesNotMutateParentRow(t *testing.T) {
	parent := map[string]interface{}{
		"o_orderkey": 1001,
		"lineitems": []interface{}{
			map[string]interface{}{"l_linenumber": 1},
		},
	}
	child := map[string]interface{}{"l_linenumber": 1}

	payload, err := buildPutRowChildPayload(parent, "lineitems", child)

	require.NoError(t, err)
	assert.Equal(t, 1001, payload["o_orderkey"])
	assert.Equal(t, child, payload["lineitems"])
	assert.IsType(t, []interface{}{}, parent["lineitems"])
	payload["o_orderkey"] = 2002
	assert.Equal(t, 1001, parent["o_orderkey"])
}

func TestBuildPutRowChildPayloadRequiresMapParent(t *testing.T) {
	_, err := buildPutRowChildPayload(struct{}{}, "lineitems", map[string]interface{}{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "child expansion requires map row")
}
