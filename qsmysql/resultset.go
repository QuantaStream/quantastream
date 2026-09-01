package qsmysql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// ColumnType identifies one MySQL column type byte.
type ColumnType byte

const (
	// ColumnTypeTiny is MYSQL_TYPE_TINY.
	ColumnTypeTiny ColumnType = 0x01
	// ColumnTypeDouble is MYSQL_TYPE_DOUBLE.
	ColumnTypeDouble ColumnType = 0x05
	// ColumnTypeLongLong is MYSQL_TYPE_LONGLONG.
	ColumnTypeLongLong ColumnType = 0x08
	// ColumnTypeDateTime is MYSQL_TYPE_DATETIME.
	ColumnTypeDateTime ColumnType = 0x0c
	// ColumnTypeVarString is MYSQL_TYPE_VAR_STRING.
	ColumnTypeVarString ColumnType = 0xfd
	// ColumnTypeString is MYSQL_TYPE_STRING.
	ColumnTypeString ColumnType = 0xfe
)

const (
	columnFlagNotNull uint16 = 0x0001
	nullTextValue            = 0xfb
	eofPacketHeader          = 0xfe
)

// ColumnDefinition is a MySQL text-protocol column definition payload model.
type ColumnDefinition struct {
	Catalog       string
	Schema        string
	Table         string
	OriginalTable string
	Name          string
	OriginalName  string
	CharacterSet  CharacterSet
	ColumnLength  uint32
	Type          ColumnType
	Flags         uint16
	Decimals      byte
}

// ResultSetOptions tunes MySQL result-set metadata for client-specific front doors.
type ResultSetOptions struct {
	DefaultSchema string
}

// NewColumnDefinition maps generic qsbridge protocol metadata to a MySQL column definition.
func NewColumnDefinition(column qsbridge.ProtocolColumn) ColumnDefinition {
	return NewColumnDefinitionWithOptions(column, ResultSetOptions{})
}

// NewColumnDefinitionWithOptions maps generic qsbridge protocol metadata to a
// MySQL column definition using optional connection context.
func NewColumnDefinitionWithOptions(column qsbridge.ProtocolColumn, options ResultSetOptions) ColumnDefinition {
	schema, table, originalName := sourceParts(column.Source)
	if schema == "" && table != "" {
		schema = strings.TrimSpace(options.DefaultSchema)
	}
	definition := ColumnDefinition{
		Catalog:       "def",
		Schema:        schema,
		Table:         table,
		OriginalTable: table,
		Name:          column.Name,
		OriginalName:  originalName,
		CharacterSet:  CharacterSetUTF8MB4GeneralCI,
		Type:          columnType(column),
		ColumnLength:  columnLength(column),
		Decimals:      columnDecimals(column),
	}
	if definition.OriginalName == "" {
		definition.OriginalName = column.Name
	}
	if !column.Nullable {
		definition.Flags |= columnFlagNotNull
	}
	return definition
}

// Payload encodes the column definition as a MySQL text-protocol payload.
func (c ColumnDefinition) Payload() []byte {
	payload := make([]byte, 0, 64+len(c.Name)+len(c.OriginalName)+len(c.Table)+len(c.OriginalTable))
	payload = appendLengthEncodedString(payload, c.Catalog)
	payload = appendLengthEncodedString(payload, c.Schema)
	payload = appendLengthEncodedString(payload, c.Table)
	payload = appendLengthEncodedString(payload, c.OriginalTable)
	payload = appendLengthEncodedString(payload, c.Name)
	payload = appendLengthEncodedString(payload, c.OriginalName)
	payload = append(payload, 0x0c)
	payload = appendUint16LE(payload, uint16(c.CharacterSet))
	payload = appendUint32LE(payload, c.ColumnLength)
	payload = append(payload, byte(c.Type))
	payload = appendUint16LE(payload, c.Flags)
	payload = append(payload, c.Decimals)
	payload = append(payload, 0x00, 0x00)
	return payload
}

// FieldListPayload encodes the COM_FIELD_LIST variant of a MySQL 4.1 column
// definition. That command requires a trailing length-encoded default value;
// QuantaStream does not expose column defaults through this legacy command.
func (c ColumnDefinition) FieldListPayload() []byte {
	payload := c.Payload()
	return append(payload, nullTextValue)
}

// TextResultSetPackets encodes a protocol-neutral query result into MySQL text-result packets.
func TextResultSetPackets(result qsbridge.ExecutionResult) ([]Packet, error) {
	return TextResultSetPacketsWithOptions(result, ResultSetOptions{})
}

// TextResultSetPacketsWithOptions encodes a protocol-neutral query result into
// MySQL text-result packets using optional connection metadata.
func TextResultSetPacketsWithOptions(result qsbridge.ExecutionResult, options ResultSetOptions) ([]Packet, error) {
	if len(result.Columns) == 0 {
		return nil, fmt.Errorf("mysql text resultset requires at least one column")
	}
	schema := result.ProtocolResultSchema(qsbridge.NewProtocolProfile(qsbridge.ProtocolMySQL, "mysql-wire"))
	packets := make([]Packet, 0, 1+len(schema.Columns)+2+int(result.RowsReturned))
	sequence := byte(1)
	packets = append(packets, Packet{SequenceID: sequence, Payload: encodeLengthEncodedInteger(uint64(len(schema.Columns)))})
	sequence++
	for _, column := range schema.Columns {
		packets = append(packets, Packet{SequenceID: sequence, Payload: NewColumnDefinitionWithOptions(column, options).Payload()})
		sequence++
	}
	packets = append(packets, Packet{SequenceID: sequence, Payload: eofPayload(0)})
	sequence++
	for _, chunk := range result.Chunks {
		for _, row := range chunk.Rows {
			payload, err := textRowPayload(row, len(schema.Columns))
			if err != nil {
				return nil, err
			}
			packets = append(packets, Packet{SequenceID: sequence, Payload: payload})
			sequence++
		}
	}
	packets = append(packets, Packet{SequenceID: sequence, Payload: eofPayload(0)})
	return packets, nil
}

// FieldListPacketsWithOptions encodes COM_FIELD_LIST column metadata packets.
// Unlike a normal result set, COM_FIELD_LIST sends column definitions directly
// followed by EOF, without a leading column-count packet or row data.
func FieldListPacketsWithOptions(result qsbridge.ExecutionResult, options ResultSetOptions, pattern string) ([]Packet, error) {
	if len(result.Columns) == 0 {
		return nil, fmt.Errorf("mysql field list requires at least one column")
	}
	schema := result.ProtocolResultSchema(qsbridge.NewProtocolProfile(qsbridge.ProtocolMySQL, "mysql-wire"))
	packets := make([]Packet, 0, len(schema.Columns)+1)
	sequence := byte(1)
	for _, column := range schema.Columns {
		if !fieldListPatternMatches(pattern, column.Name) {
			continue
		}
		packets = append(packets, Packet{SequenceID: sequence, Payload: NewColumnDefinitionWithOptions(column, options).FieldListPayload()})
		sequence++
	}
	packets = append(packets, Packet{SequenceID: sequence, Payload: eofPayload(0)})
	return packets, nil
}

func fieldListPatternMatches(pattern string, value string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == "%" || pattern == "*" {
		return true
	}
	return fieldListPatternMatchRunes([]rune(strings.ToLower(pattern)), []rune(strings.ToLower(value)), 0, 0)
}

func fieldListPatternMatchRunes(pattern []rune, value []rune, p int, v int) bool {
	if p == len(pattern) {
		return v == len(value)
	}
	switch pattern[p] {
	case '%', '*':
		for i := v; i <= len(value); i++ {
			if fieldListPatternMatchRunes(pattern, value, p+1, i) {
				return true
			}
		}
		return false
	case '_', '?':
		return v < len(value) && fieldListPatternMatchRunes(pattern, value, p+1, v+1)
	default:
		return v < len(value) && pattern[p] == value[v] && fieldListPatternMatchRunes(pattern, value, p+1, v+1)
	}
}

func textRowPayload(row qsbridge.ResultRow, columnCount int) ([]byte, error) {
	if len(row) != columnCount {
		return nil, fmt.Errorf("mysql text row has %d cells for %d columns", len(row), columnCount)
	}
	payload := make([]byte, 0, len(row)*8)
	for _, cell := range row {
		if cell.Kind == qsbridge.ValueNull || cell.Value == nil {
			payload = append(payload, nullTextValue)
			continue
		}
		value, err := textCellValue(cell)
		if err != nil {
			return nil, err
		}
		payload = appendLengthEncodedString(payload, value)
	}
	return payload, nil
}

func textCellValue(cell qsbridge.ResultCell) (string, error) {
	switch cell.Kind {
	case qsbridge.ValueBool:
		value, ok := cell.Value.(bool)
		if !ok {
			return "", fmt.Errorf("mysql text bool cell has %T value", cell.Value)
		}
		if value {
			return "1", nil
		}
		return "0", nil
	case qsbridge.ValueInt:
		return integerTextValue(cell.Value)
	case qsbridge.ValueFloat:
		return floatTextValue(cell.Value)
	case qsbridge.ValueTime:
		if value, ok := cell.Value.(time.Time); ok {
			return value.Format("2006-01-02 15:04:05"), nil
		}
		return fmt.Sprint(cell.Value), nil
	case qsbridge.ValueString, qsbridge.ValueUnknown:
		return fmt.Sprint(cell.Value), nil
	default:
		return fmt.Sprint(cell.Value), nil
	}
}

func integerTextValue(value any) (string, error) {
	switch typed := value.(type) {
	case int:
		return strconv.FormatInt(int64(typed), 10), nil
	case int8:
		return strconv.FormatInt(int64(typed), 10), nil
	case int16:
		return strconv.FormatInt(int64(typed), 10), nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case uint:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint64:
		return strconv.FormatUint(typed, 10), nil
	default:
		return "", fmt.Errorf("mysql text int cell has %T value", value)
	}
}

func floatTextValue(value any) (string, error) {
	switch typed := value.(type) {
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("mysql text float cell has %T value", value)
	}
}

func sourceParts(source string) (string, string, string) {
	if source == "" {
		return "", "", ""
	}
	parts := strings.Split(source, ".")
	if len(parts) == 1 {
		return "", "", parts[0]
	}
	if len(parts) == 2 {
		return "", parts[0], parts[1]
	}
	return parts[len(parts)-3], parts[len(parts)-2], parts[len(parts)-1]
}

func columnType(column qsbridge.ProtocolColumn) ColumnType {
	switch column.LogicalType {
	case qsbridge.DataTypeBool:
		return ColumnTypeTiny
	case qsbridge.DataTypeInt:
		return ColumnTypeLongLong
	case qsbridge.DataTypeFloat:
		return ColumnTypeDouble
	case qsbridge.DataTypeTime:
		return ColumnTypeDateTime
	case qsbridge.DataTypeString:
		return ColumnTypeVarString
	default:
		switch column.WireType {
		case "MYSQL_TYPE_TINY":
			return ColumnTypeTiny
		case "MYSQL_TYPE_LONGLONG":
			return ColumnTypeLongLong
		case "MYSQL_TYPE_DOUBLE":
			return ColumnTypeDouble
		case "MYSQL_TYPE_DATETIME":
			return ColumnTypeDateTime
		case "MYSQL_TYPE_VAR_STRING":
			return ColumnTypeVarString
		default:
			return ColumnTypeString
		}
	}
}

func columnLength(column qsbridge.ProtocolColumn) uint32 {
	switch column.LogicalType {
	case qsbridge.DataTypeBool:
		return 1
	case qsbridge.DataTypeInt:
		return 20
	case qsbridge.DataTypeFloat:
		return 22
	case qsbridge.DataTypeTime:
		return 19
	case qsbridge.DataTypeString:
		return 1024
	default:
		return 1024
	}
}

func columnDecimals(column qsbridge.ProtocolColumn) byte {
	if column.LogicalType == qsbridge.DataTypeFloat {
		return 31
	}
	return 0
}

func eofPayload(warnings uint16) []byte {
	payload := []byte{eofPacketHeader}
	payload = appendUint16LE(payload, warnings)
	payload = appendUint16LE(payload, uint16(StatusAutocommit))
	return payload
}

func appendLengthEncodedString(out []byte, value string) []byte {
	out = appendLengthEncodedInteger(out, uint64(len(value)))
	return append(out, []byte(value)...)
}

func encodeLengthEncodedInteger(value uint64) []byte {
	return appendLengthEncodedInteger(nil, value)
}

func appendLengthEncodedInteger(out []byte, value uint64) []byte {
	switch {
	case value < 251:
		return append(out, byte(value))
	case value <= 0xffff:
		return append(out, 0xfc, byte(value), byte(value>>8))
	case value <= 0xffffff:
		return append(out, 0xfd, byte(value), byte(value>>8), byte(value>>16))
	default:
		out = append(out, 0xfe)
		return appendUint64LE(out, value)
	}
}

func appendUint64LE(out []byte, value uint64) []byte {
	return append(out,
		byte(value),
		byte(value>>8),
		byte(value>>16),
		byte(value>>24),
		byte(value>>32),
		byte(value>>40),
		byte(value>>48),
		byte(value>>56),
	)
}
