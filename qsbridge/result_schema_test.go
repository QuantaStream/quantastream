package qsbridge

import "testing"

func TestResultColumnProtocolColumnMapsMySQLTypes(t *testing.T) {
	profile := NewProtocolProfile(ProtocolMySQL, "mysql")
	tests := []struct {
		name     string
		dataType DataType
		wantType string
		wantWire string
	}{
		{name: "bool", dataType: DataTypeBool, wantType: "BOOL", wantWire: "MYSQL_TYPE_TINY"},
		{name: "int", dataType: DataTypeInt, wantType: "BIGINT", wantWire: "MYSQL_TYPE_LONGLONG"},
		{name: "float", dataType: DataTypeFloat, wantType: "DOUBLE", wantWire: "MYSQL_TYPE_DOUBLE"},
		{name: "string", dataType: DataTypeString, wantType: "VARCHAR", wantWire: "MYSQL_TYPE_VAR_STRING"},
		{name: "time", dataType: DataTypeTime, wantType: "DATETIME", wantWire: "MYSQL_TYPE_DATETIME"},
		{name: "unknown", dataType: DataTypeUnknown, wantType: "TEXT", wantWire: "MYSQL_TYPE_STRING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			column := ResultColumn{Name: "col", Type: tt.dataType, Nullable: true, Source: "orders.col"}
			got := column.ProtocolColumn(profile)
			if got.TypeName != tt.wantType || got.WireType != tt.wantWire {
				t.Fatalf("type/wire = %q/%q, want %q/%q", got.TypeName, got.WireType, tt.wantType, tt.wantWire)
			}
			if got.Name != "col" || got.Source != "orders.col" || !got.Nullable || got.LogicalType != tt.dataType {
				t.Fatalf("column metadata = %#v, want copied logical metadata", got)
			}
			if !protocolColumnFlagsContain(got.Flags, ProtocolColumnNullable, ProtocolColumnDirectSource) {
				t.Fatalf("flags = %#v, want nullable direct source", got.Flags)
			}
		})
	}
}

func TestResultColumnProtocolColumnMapsGRPCTypes(t *testing.T) {
	column := ResultColumn{Name: "created_at", Type: DataTypeTime}

	got := column.ProtocolColumn(NewProtocolProfile(ProtocolGRPC, "grpc"))
	if got.TypeName != "timestamp" || got.WireType != "google.protobuf.Timestamp" {
		t.Fatalf("type/wire = %q/%q, want timestamp protobuf", got.TypeName, got.WireType)
	}
}

func TestExecutionRequestProtocolResultSchemaCopiesColumns(t *testing.T) {
	request := ExecutionRequest{
		ResultColumns: []ResultColumn{
			{Name: "order_id", Type: DataTypeInt, Source: "orders.o_orderkey"},
			{Name: "city", Type: DataTypeString, Nullable: true},
		},
	}
	profile := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)

	schema := request.ProtocolResultSchema(profile)
	if schema.Profile.Kind != ProtocolMySQL || !schema.Profile.Supports(ProtocolCapabilityPreparedStatements) {
		t.Fatalf("profile = %#v, want copied mysql profile", schema.Profile)
	}
	if len(schema.Columns) != 2 || schema.Columns[0].WireType != "MYSQL_TYPE_LONGLONG" || schema.Columns[1].TypeName != "VARCHAR" {
		t.Fatalf("columns = %#v, want mapped mysql columns", schema.Columns)
	}
	request.ResultColumns[0].Name = "mutated"
	schema.Profile.Capabilities[0] = ProtocolCapabilityBatchExecution
	second := request.ProtocolResultSchema(profile)
	if schema.Columns[0].Name != "order_id" {
		t.Fatalf("schema leaked request column mutation: %#v", schema.Columns)
	}
	if second.Profile.Capabilities[0] != ProtocolCapabilityPreparedStatements {
		t.Fatalf("profile leaked mutation: %#v", second.Profile.Capabilities)
	}
}

func TestExecutionResultProtocolResultSchemaUsesResultColumns(t *testing.T) {
	result := ExecutionResult{
		Columns: []ResultColumn{{Name: "active", Type: DataTypeBool}},
	}

	schema := result.ProtocolResultSchema(NewProtocolProfile(ProtocolHTTP, "http"))
	if len(schema.Columns) != 1 || schema.Columns[0].TypeName != "boolean" || schema.Columns[0].WireType != "json_boolean" {
		t.Fatalf("schema = %#v, want HTTP boolean metadata", schema)
	}
	if !protocolColumnFlagsContain(schema.Columns[0].Flags, ProtocolColumnDerived, ProtocolColumnBoolean) {
		t.Fatalf("flags = %#v, want derived boolean metadata", schema.Columns[0].Flags)
	}
}

func TestNewProtocolResultSchemaUsesGenericFallbackForUnknownProtocol(t *testing.T) {
	schema := NewProtocolResultSchema(ProtocolProfile{}, []ResultColumn{{Name: "value", Type: DataTypeFloat}})
	if len(schema.Columns) != 1 || schema.Columns[0].TypeName != "float" || schema.Columns[0].WireType != "float" {
		t.Fatalf("schema = %#v, want generic float metadata", schema)
	}
	if !protocolColumnFlagsContain(schema.Columns[0].Flags, ProtocolColumnDerived, ProtocolColumnNumeric) {
		t.Fatalf("flags = %#v, want derived numeric metadata", schema.Columns[0].Flags)
	}
}

func TestNewProtocolResultSchemaClassifiesColumnFlags(t *testing.T) {
	schema := NewProtocolResultSchema(NewProtocolProfile(ProtocolMySQL, "mysql"), []ResultColumn{
		{Name: "shipdate", Type: DataTypeTime, Source: "lineitem.l_shipdate"},
		{Name: "comment", Type: DataTypeString, Nullable: true},
	})
	if !protocolColumnFlagsContain(schema.Columns[0].Flags, ProtocolColumnDirectSource, ProtocolColumnTemporal) {
		t.Fatalf("time flags = %#v, want direct temporal metadata", schema.Columns[0].Flags)
	}
	if !protocolColumnFlagsContain(schema.Columns[1].Flags, ProtocolColumnNullable, ProtocolColumnDerived, ProtocolColumnString) {
		t.Fatalf("string flags = %#v, want nullable derived string metadata", schema.Columns[1].Flags)
	}
}

func protocolColumnFlagsContain(flags []ProtocolColumnFlag, want ...ProtocolColumnFlag) bool {
	for _, item := range want {
		found := false
		for _, flag := range flags {
			if flag == item {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
