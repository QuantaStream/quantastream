package qsbridge

// ProtocolColumnFlag records adapter-facing column traits.
type ProtocolColumnFlag string

const (
	// ProtocolColumnNullable means the column may return NULL values.
	ProtocolColumnNullable ProtocolColumnFlag = "nullable"
	// ProtocolColumnDirectSource means the column maps directly to one source field.
	ProtocolColumnDirectSource ProtocolColumnFlag = "direct_source"
	// ProtocolColumnDerived means the column is computed, aggregated, or otherwise expression-derived.
	ProtocolColumnDerived ProtocolColumnFlag = "derived"
	// ProtocolColumnNumeric means the logical type is numeric.
	ProtocolColumnNumeric ProtocolColumnFlag = "numeric"
	// ProtocolColumnString means the logical type is string-like.
	ProtocolColumnString ProtocolColumnFlag = "string"
	// ProtocolColumnTemporal means the logical type is time-like.
	ProtocolColumnTemporal ProtocolColumnFlag = "temporal"
	// ProtocolColumnBoolean means the logical type is boolean.
	ProtocolColumnBoolean ProtocolColumnFlag = "boolean"
)

// ProtocolColumn describes one result column as a protocol adapter can expose it.
//
// It preserves the logical qsbridge type while adding conservative protocol
// type labels. Adapters still own concrete wire encodings and value
// serialization.
type ProtocolColumn struct {
	Name        string
	Source      string
	Nullable    bool
	LogicalType DataType
	TypeName    string
	WireType    string
	Flags       []ProtocolColumnFlag
}

// ProtocolResultSchema is a protocol-facing description of a row result shape.
type ProtocolResultSchema struct {
	Profile ProtocolProfile
	Columns []ProtocolColumn
}

// ProtocolResultSchema describes result columns for the given protocol profile.
func (r ExecutionRequest) ProtocolResultSchema(profile ProtocolProfile) ProtocolResultSchema {
	return NewProtocolResultSchema(profile, r.ResultColumns)
}

// ProtocolResultSchema describes result columns for the given protocol profile.
func (r ExecutionResult) ProtocolResultSchema(profile ProtocolProfile) ProtocolResultSchema {
	return NewProtocolResultSchema(profile, r.Columns)
}

// NewProtocolResultSchema maps logical result columns into protocol metadata.
func NewProtocolResultSchema(profile ProtocolProfile, columns []ResultColumn) ProtocolResultSchema {
	schema := ProtocolResultSchema{
		Profile: profile.Clone(),
		Columns: make([]ProtocolColumn, 0, len(columns)),
	}
	for _, column := range columns {
		schema.Columns = append(schema.Columns, column.ProtocolColumn(profile))
	}
	return schema
}

// ProtocolColumn maps one logical result column into protocol metadata.
func (c ResultColumn) ProtocolColumn(profile ProtocolProfile) ProtocolColumn {
	typeName, wireType := protocolTypeNames(profile.Kind, c.Type)
	return ProtocolColumn{
		Name:        c.Name,
		Source:      c.Source,
		Nullable:    c.Nullable,
		LogicalType: c.Type,
		TypeName:    typeName,
		WireType:    wireType,
		Flags:       protocolColumnFlags(c),
	}
}

func protocolColumnFlags(column ResultColumn) []ProtocolColumnFlag {
	flags := make([]ProtocolColumnFlag, 0, 4)
	if column.Nullable {
		flags = append(flags, ProtocolColumnNullable)
	}
	if column.Source != "" {
		flags = append(flags, ProtocolColumnDirectSource)
	} else {
		flags = append(flags, ProtocolColumnDerived)
	}
	switch column.Type {
	case DataTypeBool:
		flags = append(flags, ProtocolColumnBoolean)
	case DataTypeInt, DataTypeFloat:
		flags = append(flags, ProtocolColumnNumeric)
	case DataTypeString:
		flags = append(flags, ProtocolColumnString)
	case DataTypeTime:
		flags = append(flags, ProtocolColumnTemporal)
	}
	return flags
}

func protocolTypeNames(protocol ProtocolKind, dataType DataType) (string, string) {
	switch protocol {
	case ProtocolMySQL:
		return mysqlProtocolTypeNames(dataType)
	case ProtocolGRPC:
		return grpcProtocolTypeNames(dataType)
	case ProtocolGo:
		return goProtocolTypeNames(dataType)
	case ProtocolHTTP:
		return httpProtocolTypeNames(dataType)
	default:
		return genericProtocolTypeNames(dataType)
	}
}

func mysqlProtocolTypeNames(dataType DataType) (string, string) {
	switch dataType {
	case DataTypeBool:
		return "BOOL", "MYSQL_TYPE_TINY"
	case DataTypeInt:
		return "BIGINT", "MYSQL_TYPE_LONGLONG"
	case DataTypeFloat:
		return "DOUBLE", "MYSQL_TYPE_DOUBLE"
	case DataTypeString:
		return "VARCHAR", "MYSQL_TYPE_VAR_STRING"
	case DataTypeTime:
		return "DATETIME", "MYSQL_TYPE_DATETIME"
	default:
		return "TEXT", "MYSQL_TYPE_STRING"
	}
}

func grpcProtocolTypeNames(dataType DataType) (string, string) {
	switch dataType {
	case DataTypeBool:
		return "bool", "google.protobuf.BoolValue"
	case DataTypeInt:
		return "int64", "google.protobuf.Int64Value"
	case DataTypeFloat:
		return "double", "google.protobuf.DoubleValue"
	case DataTypeString:
		return "string", "google.protobuf.StringValue"
	case DataTypeTime:
		return "timestamp", "google.protobuf.Timestamp"
	default:
		return "value", "google.protobuf.Value"
	}
}

func goProtocolTypeNames(dataType DataType) (string, string) {
	switch dataType {
	case DataTypeBool:
		return "bool", "bool"
	case DataTypeInt:
		return "int64", "int64"
	case DataTypeFloat:
		return "float64", "float64"
	case DataTypeString:
		return "string", "string"
	case DataTypeTime:
		return "time.Time", "time.Time"
	default:
		return "any", "any"
	}
}

func httpProtocolTypeNames(dataType DataType) (string, string) {
	switch dataType {
	case DataTypeBool:
		return "boolean", "json_boolean"
	case DataTypeInt:
		return "integer", "json_number"
	case DataTypeFloat:
		return "number", "json_number"
	case DataTypeString:
		return "string", "json_string"
	case DataTypeTime:
		return "string", "json_rfc3339"
	default:
		return "value", "json_value"
	}
}

func genericProtocolTypeNames(dataType DataType) (string, string) {
	if dataType == "" {
		return "unknown", "unknown"
	}
	return string(dataType), string(dataType)
}
