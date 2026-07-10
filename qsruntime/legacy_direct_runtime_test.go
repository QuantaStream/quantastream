package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
)

func TestLegacyQuantaSourceFactoryReportsMissingTableCache(t *testing.T) {
	runtime, diagnostics, err := LegacyQuantaSourceFactory{}.
		NewDirectRuntime(context.Background(), NewDirectRuntimeConfig("", "", 0, 0))
	if err != nil {
		t.Fatalf("new direct runtime: %v", err)
	}
	if runtime != nil {
		t.Fatalf("runtime = %#v, want nil", runtime)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing table cache diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestLegacyQuantaSourceFactoryInitializesZeroValueTableCache(t *testing.T) {
	cache := &core.TableCacheStruct{}
	diagnostics := (LegacyQuantaSourceFactory{TableCache: cache}).ensureTableCache()
	if diagnostics.BlocksNative() {
		t.Fatalf("ensure table cache diagnostics: %v", diagnostics)
	}
	if cache.TableCache == nil {
		t.Fatalf("table cache map was not initialized")
	}
}

func TestLegacyQuantaSourceSessionProviderReportsMissingSource(t *testing.T) {
	session, diagnostics, err := LegacyQuantaSourceSessionProvider{}.
		BorrowDirectSession(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("borrow direct session: %v", err)
	}
	if session != nil {
		t.Fatalf("session = %#v, want nil", session)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing source diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestLegacyQuantaSessionHandleReportsMissingBitmapIndex(t *testing.T) {
	_, diagnostics, err := LegacyQuantaSessionHandle{}.
		QueryBitmap(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("query bitmap: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing bitmap index diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestLegacyQuantaSessionHandleReportsMissingReleaseState(t *testing.T) {
	diagnostics := LegacyQuantaSessionHandle{}.Release(context.Background())
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing release state diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestLegacyDirectInsertRowMapOmitsNullValuesForDefaultsAndKeepsLogicalRownum(t *testing.T) {
	rowMap, rownum, diagnostics, ok := legacyDirectInsertRowMap(
		nil,
		[]qsbridge.FieldRef{
			{Name: "cust_id"},
			{Name: "createdAtTimestamp"},
			{Name: "rownum"},
		},
		qsbridge.MutationRow{Values: []qsbridge.Expr{
			qsbridge.Literal(qsbridge.ValueString, "200"),
			qsbridge.Literal(qsbridge.ValueNull, nil),
			qsbridge.Literal(qsbridge.ValueInt, int64(42)),
		}},
	)
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("legacyDirectInsertRowMap diagnostics = %v", diagnostics)
	}
	if rownum != 0 {
		t.Fatalf("rownum = %d, want physical rownum unset", rownum)
	}
	if _, ok := rowMap["createdAtTimestamp"]; ok {
		t.Fatalf("createdAtTimestamp was present in row map: %#v", rowMap)
	}
	if got := rowMap["cust_id"]; got != "200" {
		t.Fatalf("cust_id = %#v, want 200", got)
	}
	if got := rowMap["rownum"]; got != int64(42) {
		t.Fatalf("rownum = %#v, want logical field value 42", got)
	}
}

func TestLegacyDirectInsertRowMapPreservesExplicitNullDefaultTimeAsEmptyValue(t *testing.T) {
	table := &core.Table{
		AttributeNameMap: map[string]*core.Attribute{
			"createdAtTimestamp": {
				BasicAttribute: &shared.BasicAttribute{
					FieldName:    "createdAtTimestamp",
					Type:         "DateTime",
					DefaultValue: "now()",
				},
			},
		},
	}
	rowMap, _, diagnostics, ok := legacyDirectInsertRowMap(
		table,
		[]qsbridge.FieldRef{{Name: "createdAtTimestamp"}},
		qsbridge.MutationRow{Values: []qsbridge.Expr{qsbridge.Literal(qsbridge.ValueNull, nil)}},
	)
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("legacyDirectInsertRowMap diagnostics = %v", diagnostics)
	}
	if got, ok := rowMap["createdAtTimestamp"]; !ok || got != legacyDirectExplicitEmptyTimeSentinel {
		t.Fatalf("createdAtTimestamp = %#v ok=%v, want explicit sentinel", got, ok)
	}
}

func TestLegacyDirectInsertRowMapOmitsBlankStringWhenCatalogDefaultExists(t *testing.T) {
	table := &core.Table{
		AttributeNameMap: map[string]*core.Attribute{
			"createdAtTimestamp": {BasicAttribute: &shared.BasicAttribute{FieldName: "createdAtTimestamp", DefaultValue: "now()"}},
			"phoneType":          {BasicAttribute: &shared.BasicAttribute{FieldName: "phoneType"}},
		},
	}
	rowMap, _, diagnostics, ok := legacyDirectInsertRowMap(
		table,
		[]qsbridge.FieldRef{
			{Name: "createdAtTimestamp"},
			{Name: "phoneType"},
		},
		qsbridge.MutationRow{Values: []qsbridge.Expr{
			qsbridge.Literal(qsbridge.ValueString, ""),
			qsbridge.Literal(qsbridge.ValueString, ""),
		}},
	)
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("legacyDirectInsertRowMap diagnostics = %v", diagnostics)
	}
	if _, ok := rowMap["createdAtTimestamp"]; ok {
		t.Fatalf("createdAtTimestamp was present in row map: %#v", rowMap)
	}
	if got, ok := rowMap["phoneType"]; !ok || got != "" {
		t.Fatalf("phoneType = %#v ok=%v, want preserved blank string", got, ok)
	}
}

func TestLegacyDirectInsertRowMapSplitsSetValuedString(t *testing.T) {
	customers := qsbridge.TableInstance{ID: "customers_qa_1", Table: "customers_qa", Alias: "c"}
	columns := []qsbridge.FieldRef{{
		Table: customers,
		Name:  "phoneType",
		Type:  qsbridge.DataTypeString,
		Encoding: qsbridge.EncodingProfile{
			Kind:         qsbridge.EncodingStringEnum,
			Multiplicity: qsbridge.MultiplicitySet,
		},
	}}
	row := qsbridge.MutationRow{Values: []qsbridge.Expr{qsbridge.Literal(qsbridge.ValueString, "cell; home;business")}}

	rowMap, _, diagnostics, ok := legacyDirectInsertRowMap(nil, columns, row)
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, ok=%v", diagnostics, ok)
	}
	values, ok := rowMap["phoneType"].([]string)
	if !ok {
		t.Fatalf("phoneType = %#v, want []string", rowMap["phoneType"])
	}
	if len(values) != 3 || values[0] != "cell" || values[1] != "home" || values[2] != "business" {
		t.Fatalf("values = %#v, want split phone labels", values)
	}
}

func TestLegacyDirectMutationLiteralValuePreservesScalarSemicolonString(t *testing.T) {
	field := qsbridge.FieldRef{Name: "comment", Type: qsbridge.DataTypeString}
	value, diagnostics, ok := legacyDirectMutationLiteralValue(field, qsbridge.Literal(qsbridge.ValueString, "a;b"))
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, ok=%v", diagnostics, ok)
	}
	if value != "a;b" {
		t.Fatalf("value = %#v, want scalar string", value)
	}
}

func TestLegacyDirectUpdateValueMapSplitsSetValuedString(t *testing.T) {
	customers := qsbridge.TableInstance{ID: "customers_qa_1", Table: "customers_qa", Alias: "c"}
	assignments := []qsbridge.MutationAssignment{{
		Field: qsbridge.FieldRef{
			Table: customers,
			Name:  "phoneType",
			Type:  qsbridge.DataTypeString,
			Encoding: qsbridge.EncodingProfile{
				Kind:         qsbridge.EncodingStringEnum,
				Multiplicity: qsbridge.MultiplicitySet,
			},
		},
		Value: qsbridge.Literal(qsbridge.ValueString, "cell;home"),
	}}

	valueMap, diagnostics, ok := legacyDirectUpdateValueMap(assignments)
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, ok=%v", diagnostics, ok)
	}
	values, ok := valueMap["phoneType"].Value.([]string)
	if !ok {
		t.Fatalf("value = %#v, want []string", valueMap["phoneType"].Value)
	}
	if len(values) != 2 || values[0] != "cell" || values[1] != "home" {
		t.Fatalf("values = %#v, want cell/home", values)
	}
}

func TestLegacyDirectUpdateValueMapUsesPhysicalNames(t *testing.T) {
	orders := qsbridge.TableInstance{ID: "orders_1", Table: "orders", Alias: "o"}
	assignments := []qsbridge.MutationAssignment{{
		Field: qsbridge.FieldRef{Table: orders, Name: "total_price", PhysicalName: "o_totalprice", Type: qsbridge.DataTypeFloat},
		Value: qsbridge.Literal(qsbridge.ValueFloat, 42.5),
	}}

	valueMap, diagnostics, ok := legacyDirectUpdateValueMap(assignments)
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, ok=%v", diagnostics, ok)
	}
	column, ok := valueMap["o_totalprice"]
	if !ok {
		t.Fatalf("value map keys = %#v, want o_totalprice", valueMap)
	}
	if got := column.Value; got != 42.5 {
		t.Fatalf("value = %#v, want 42.5", got)
	}
}

func TestLegacyDirectExecuteDeleteReportsMissingBitmapIndex(t *testing.T) {
	_, diagnostics, err := (LegacyQuantaSessionHandle{}).ExecuteMutation(context.Background(), ExecutionRequest{
		Mutation: qsbridge.MutationShape{Kind: qsbridge.MutationDelete},
	})
	if err != nil {
		t.Fatalf("execute mutation: %v", err)
	}
	if !diagnostics.BlocksNative() || diagnostics.Codes()[0] != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostics = %#v, want missing bitmap index", diagnostics)
	}
}

func TestLegacyDirectMutationBitmapCopiesRownums(t *testing.T) {
	bitmap := legacyDirectMutationBitmap([]qsbridge.QuantaRownum{3, 5, 5})
	if bitmap.GetCardinality() != 2 || !bitmap.Contains(3) || !bitmap.Contains(5) {
		t.Fatalf("bitmap cardinality=%d contains3=%v contains5=%v", bitmap.GetCardinality(), bitmap.Contains(3), bitmap.Contains(5))
	}
}

func TestLegacyQuantaSessionHandleFindsCachedRootTableFromSessionBuffer(t *testing.T) {
	table := &core.Table{BasicTable: &shared.BasicTable{Name: "customers_qa"}}
	handle := LegacyQuantaSessionHandle{
		TableName: "customers_qa",
		Session: &core.Session{TableBuffers: map[string]*core.TableBuffer{
			"customers_qa": {Table: table},
		}},
	}

	got := handle.cachedRootTable(NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if got != table {
		t.Fatalf("table = %#v, want session table buffer", got)
	}
}
func TestLegacyDirectFullTableScanSeedPrefersPrimaryKeyExistence(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"customers_qa"}
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "customers_qa", PrimaryKey: "cust_id"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "cust_id"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "rownum"}},
		},
	}

	seeded := legacyDirectExecutionWithFullTableScanSeed(request, table)

	if len(seeded.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one existence seed", seeded.Query.Fragments)
	}
	fragment := seeded.Query.Fragments[0]
	if fragment.Index != "customers_qa" || fragment.Field != "cust_id" {
		t.Fatalf("fragment target = %s.%s, want customers_qa.cust_id", fragment.Index, fragment.Field)
	}
	if !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want not-null existence seed", fragment)
	}
}

func TestLegacyDirectFullTableScanSeedUsesPrimaryKeyForNonPhysicalTimestampTable(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"customers_qa"}
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "customers_qa", PrimaryKey: "cust_id", TimeQuantumField: "birthdate"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "cust_id", Type: "String", MappingStrategy: "StringHashBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "birthdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "rownum", Type: "Int", MappingStrategy: "IntBSI"}},
		},
	}

	seeded := legacyDirectExecutionWithFullTableScanSeed(request, table)

	if len(seeded.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one existence seed", seeded.Query.Fragments)
	}
	fragment := seeded.Query.Fragments[0]
	if fragment.Index != "customers_qa" || fragment.Field != "cust_id" {
		t.Fatalf("fragment target = %s.%s, want customers_qa.cust_id", fragment.Index, fragment.Field)
	}
	if !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want not-null existence seed", fragment)
	}
	if len(seeded.Query.ProjectionFields) != 0 {
		t.Fatalf("projection fields = %#v, want no shard time projection metadata", seeded.Query.ProjectionFields)
	}
}

func TestLegacyDirectFullTableScanSeedFallsBackToRownumWithoutPrimaryKey(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"customers_qa"}
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "customers_qa"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "rownum", Type: "Int", MappingStrategy: "IntBSI"}},
		},
	}

	seeded := legacyDirectExecutionWithFullTableScanSeed(request, table)

	if len(seeded.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one existence seed", seeded.Query.Fragments)
	}
	fragment := seeded.Query.Fragments[0]
	if fragment.Index != "customers_qa" || fragment.Field != "rownum" {
		t.Fatalf("fragment target = %s.%s, want customers_qa.rownum", fragment.Index, fragment.Field)
	}
	if !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want not-null existence seed", fragment)
	}
}

func TestLegacyDirectFullTableScanSeedUsesTimeRangeForTimeShardedTable(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"lineitem"}
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem", TimeQuantumType: "YMD", TimeQuantumField: "l_shipdate"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
		},
	}

	seeded := legacyDirectExecutionWithFullTableScanSeed(request, table)

	if len(seeded.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one time range seed", seeded.Query.Fragments)
	}
	fragment := seeded.Query.Fragments[0]
	if fragment.Index != "lineitem" || fragment.Field != "l_shipdate" {
		t.Fatalf("fragment target = %s.%s, want lineitem.l_shipdate", fragment.Index, fragment.Field)
	}
	if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
		t.Fatalf("fragment = %#v, want two-sided time range seed", fragment)
	}
	begin, end := legacyDirectRelationshipFullTimeRangeEncoded(table, "l_shipdate")
	if fragment.Begin.Int64() != begin || fragment.End.Int64() != end {
		t.Fatalf("range = %d..%d, want %d..%d", fragment.Begin.Int64(), fragment.End.Int64(), begin, end)
	}
	if fragment.NullCheck || fragment.Negate {
		t.Fatalf("fragment = %#v, did not expect null-check seed", fragment)
	}
	if len(seeded.Query.ProjectionFields) != 1 || seeded.Query.ProjectionFields[0].Field != "l_shipdate" || seeded.Query.ProjectionFields[0].Type != qsbridge.DataTypeTime {
		t.Fatalf("projection fields = %#v, want shard time projection metadata", seeded.Query.ProjectionFields)
	}
}
