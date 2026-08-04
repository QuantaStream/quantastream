package core

import (
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestPutRowResultIncludesStageTimings(t *testing.T) {
	req := putRowRequest{
		tableName: "customers",
		timings: &putRowStageTimings{
			sourceElapsed:         time.Millisecond,
			identityElapsed:       2 * time.Millisecond,
			alternateKeysElapsed:  3 * time.Millisecond,
			childExpansionElapsed: 4 * time.Millisecond,
			relationElapsed:       5 * time.Millisecond,
			attributeElapsed:      6 * time.Millisecond,
		},
	}
	tbuf := &TableBuffer{CurrentColumnID: 42}
	identity := putRowIdentity{updateExisting: true}

	result := req.putRowResult(tbuf, identity, 21*time.Millisecond)

	assert.Equal(t, PutRowResult{
		TableName:             "customers",
		ColumnID:              42,
		ExistingRow:           true,
		SourceElapsed:         time.Millisecond,
		IdentityElapsed:       2 * time.Millisecond,
		AlternateKeysElapsed:  3 * time.Millisecond,
		ChildExpansionElapsed: 4 * time.Millisecond,
		RelationElapsed:       5 * time.Millisecond,
		AttributeElapsed:      6 * time.Millisecond,
		TotalElapsed:          21 * time.Millisecond,
	}, result)
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

func TestMapAlternateKeysSkipsTablesWithoutSecondaryKeys(t *testing.T) {
	session := &Session{}
	tbuf := &TableBuffer{
		Table: &Table{BasicTable: &shared.BasicTable{Name: "customers"}},
	}

	err := session.mapAlternateKeys(putRowRequest{}, tbuf)

	require.NoError(t, err)
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
	tbuf := &TableBuffer{Table: table, PKAttributes: []*Attribute{pk}}

	updateExisting, err := session.resolvePrimaryKeyColumnID(tbuf, "1001", 99, false)

	require.NoError(t, err)
	assert.False(t, updateExisting)
	assert.Equal(t, uint64(99), tbuf.CurrentColumnID)
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

	updateExisting, err := session.resolvePrimaryKeyColumnID(tbuf, "1001", 99, true)

	require.NoError(t, err)
	assert.False(t, updateExisting)
	assert.Equal(t, uint64(7), tbuf.CurrentColumnID)
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
