package qsmysql

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

const (
	// ColumnTypeDecimal is MYSQL_TYPE_DECIMAL.
	ColumnTypeDecimal ColumnType = 0x00
	// ColumnTypeShort is MYSQL_TYPE_SHORT.
	ColumnTypeShort ColumnType = 0x02
	// ColumnTypeLong is MYSQL_TYPE_LONG.
	ColumnTypeLong ColumnType = 0x03
	// ColumnTypeInt24 is MYSQL_TYPE_INT24.
	ColumnTypeInt24 ColumnType = 0x09
	// ColumnTypeFloat is MYSQL_TYPE_FLOAT.
	ColumnTypeFloat ColumnType = 0x04
	// ColumnTypeNull is MYSQL_TYPE_NULL.
	ColumnTypeNull ColumnType = 0x06
	// ColumnTypeTimestamp is MYSQL_TYPE_TIMESTAMP.
	ColumnTypeTimestamp ColumnType = 0x07
	// ColumnTypeDate is MYSQL_TYPE_DATE.
	ColumnTypeDate ColumnType = 0x0a
	// ColumnTypeTime is MYSQL_TYPE_TIME.
	ColumnTypeTime ColumnType = 0x0b
	// ColumnTypeNewDate is MYSQL_TYPE_NEWDATE.
	ColumnTypeNewDate ColumnType = 0x0e
	// ColumnTypeVarchar is MYSQL_TYPE_VARCHAR.
	ColumnTypeVarchar ColumnType = 0x0f
	// ColumnTypeBit is MYSQL_TYPE_BIT.
	ColumnTypeBit ColumnType = 0x10
	// ColumnTypeNewDecimal is MYSQL_TYPE_NEWDECIMAL.
	ColumnTypeNewDecimal ColumnType = 0xf6
	// ColumnTypeTinyBlob is MYSQL_TYPE_TINY_BLOB.
	ColumnTypeTinyBlob ColumnType = 0xf9
	// ColumnTypeMediumBlob is MYSQL_TYPE_MEDIUM_BLOB.
	ColumnTypeMediumBlob ColumnType = 0xfa
	// ColumnTypeLongBlob is MYSQL_TYPE_LONG_BLOB.
	ColumnTypeLongBlob ColumnType = 0xfb
	// ColumnTypeBlob is MYSQL_TYPE_BLOB.
	ColumnTypeBlob ColumnType = 0xfc
)

// PreparedExecuteCommand is the decoded fixed header for COM_STMT_EXECUTE.
type PreparedExecuteCommand struct {
	StatementID      uint32
	Flags            byte
	IterationCount   uint32
	ParameterPayload []byte
}

// PreparedLongDataCommand is a decoded COM_STMT_SEND_LONG_DATA packet.
type PreparedLongDataCommand struct {
	StatementID uint32
	ParameterID uint16
	Data        []byte
}

// ParameterIndex returns the one-based prepared parameter index.
func (c PreparedLongDataCommand) ParameterIndex() int {
	return int(c.ParameterID) + 1
}

// PreparedParameterType describes one MySQL binary prepared-statement parameter type.
type PreparedParameterType struct {
	Type     ColumnType
	Unsigned bool
}

// PreparedExecuteDecodeOptions supplies session state for COM_STMT_EXECUTE decoding.
type PreparedExecuteDecodeOptions struct {
	CachedTypes []PreparedParameterType
	LongData    map[int][]byte
}

// DecodePreparedExecuteCommand decodes the fixed COM_STMT_EXECUTE envelope.
func DecodePreparedExecuteCommand(payload []byte) (PreparedExecuteCommand, error) {
	if len(payload) < 10 {
		return PreparedExecuteCommand{}, fmt.Errorf("COM_STMT_EXECUTE payload too short: %d bytes", len(payload))
	}
	if CommandByte(payload[0]) != CommandStmtExecute {
		return PreparedExecuteCommand{}, fmt.Errorf("COM_STMT_EXECUTE payload has command byte 0x%02x", payload[0])
	}
	return PreparedExecuteCommand{
		StatementID:      readUint32LE(payload[1:5]),
		Flags:            payload[5],
		IterationCount:   readUint32LE(payload[6:10]),
		ParameterPayload: append([]byte(nil), payload[10:]...),
	}, nil
}

// DecodePreparedLongDataCommand decodes a COM_STMT_SEND_LONG_DATA packet.
func DecodePreparedLongDataCommand(payload []byte) (PreparedLongDataCommand, error) {
	if len(payload) < 7 {
		return PreparedLongDataCommand{}, fmt.Errorf("COM_STMT_SEND_LONG_DATA payload too short: %d bytes", len(payload))
	}
	if CommandByte(payload[0]) != CommandStmtSendLongData {
		return PreparedLongDataCommand{}, fmt.Errorf("COM_STMT_SEND_LONG_DATA payload has command byte 0x%02x", payload[0])
	}
	return PreparedLongDataCommand{
		StatementID: readUint32LE(payload[1:5]),
		ParameterID: readUint16LE(payload[5:7]),
		Data:        append([]byte(nil), payload[7:]...),
	}, nil
}

// DecodePreparedExecuteParameters converts a COM_STMT_EXECUTE parameter payload
// into qsbridge values using the prepared-plan parameter metadata.
func DecodePreparedExecuteParameters(execute PreparedExecuteCommand, refs []qsbridge.ParameterRef) ([]qsbridge.ParameterValue, error) {
	values, _, err := DecodePreparedExecuteParametersWithOptions(execute, refs, PreparedExecuteDecodeOptions{})
	return values, err
}

// DecodePreparedExecuteParametersWithOptions converts a COM_STMT_EXECUTE
// parameter payload using cached wire metadata and previously sent long-data
// chunks when the client omits inline values.
func DecodePreparedExecuteParametersWithOptions(execute PreparedExecuteCommand, refs []qsbridge.ParameterRef, options PreparedExecuteDecodeOptions) ([]qsbridge.ParameterValue, []PreparedParameterType, error) {
	parameterCount := len(refs)
	if parameterCount == 0 {
		return nil, nil, nil
	}
	payload := execute.ParameterPayload
	nullBitmapLength := (parameterCount + 7) / 8
	if len(payload) < nullBitmapLength+1 {
		return nil, nil, fmt.Errorf("COM_STMT_EXECUTE parameter payload truncated: got %d bytes, want at least %d", len(payload), nullBitmapLength+1)
	}
	nullBitmap := payload[:nullBitmapLength]
	offset := nullBitmapLength
	newParameterBoundFlag := payload[offset]
	offset++

	types := make([]PreparedParameterType, parameterCount)
	if newParameterBoundFlag != 0 {
		typeBytes := parameterCount * 2
		if len(payload)-offset < typeBytes {
			return nil, nil, fmt.Errorf("COM_STMT_EXECUTE parameter types truncated: got %d bytes, want %d", len(payload)-offset, typeBytes)
		}
		for i := 0; i < parameterCount; i++ {
			types[i] = PreparedParameterType{
				Type:     ColumnType(payload[offset]),
				Unsigned: payload[offset+1]&0x80 != 0,
			}
			offset += 2
		}
	} else if len(options.CachedTypes) == parameterCount {
		copy(types, options.CachedTypes)
	} else {
		for i, ref := range refs {
			types[i] = preparedParameterTypeFromRef(ref)
		}
	}

	values := make([]qsbridge.ParameterValue, 0, parameterCount)
	for i, ref := range refs {
		index := ref.Index
		if index == 0 {
			index = i + 1
		}
		if nullBitmap[i/8]&(1<<uint(i%8)) != 0 {
			values = append(values, qsbridge.IndexedParameterValue(index, qsbridge.ValueNull, nil))
			continue
		}
		if data, ok := options.LongData[index]; ok {
			values = append(values, qsbridge.IndexedParameterValue(index, qsbridge.ValueString, string(data)))
			continue
		}
		kind, value, next, err := decodePreparedBinaryValue(payload, offset, types[i])
		if err != nil {
			return nil, nil, fmt.Errorf("decode COM_STMT_EXECUTE parameter %d: %w", i+1, err)
		}
		offset = next
		values = append(values, qsbridge.IndexedParameterValue(index, kind, value))
	}
	if offset != len(payload) {
		return nil, nil, fmt.Errorf("COM_STMT_EXECUTE parameter payload has %d trailing bytes", len(payload)-offset)
	}
	return values, clonePreparedParameterTypes(types), nil
}

// PreparedStatementResponse encodes a COM_STMT_PREPARE_OK response and the
// parameter/result metadata packets expected by MySQL clients.
func PreparedStatementResponse(description qsbridge.PreparedPlanDescription) (CommandResponse, error) {
	if len(description.Parameters) > 0xffff {
		return CommandResponse{}, fmt.Errorf("prepared statement has too many parameters: %d", len(description.Parameters))
	}
	if len(description.ResultColumns) > 0xffff {
		return CommandResponse{}, fmt.Errorf("prepared statement has too many result columns: %d", len(description.ResultColumns))
	}
	statementID := uint32(description.Handle.ID)
	payload := []byte{okPacketHeader}
	payload = appendUint32LE(payload, statementID)
	payload = appendUint16LE(payload, uint16(len(description.ResultColumns)))
	payload = appendUint16LE(payload, uint16(len(description.Parameters)))
	payload = append(payload, 0x00)
	payload = appendUint16LE(payload, description.Statement.Warnings)

	packets := []Packet{{SequenceID: 1, Payload: payload}}
	sequence := byte(2)
	if len(description.Parameters) > 0 {
		for _, parameter := range description.Parameters {
			packets = append(packets, Packet{SequenceID: sequence, Payload: parameterColumnDefinition(parameter).Payload()})
			sequence++
		}
		packets = append(packets, Packet{SequenceID: sequence, Payload: eofPayload(description.Statement.Warnings)})
		sequence++
	}
	if len(description.ResultColumns) > 0 {
		schema := qsbridge.NewProtocolResultSchema(qsbridge.NewProtocolProfile(qsbridge.ProtocolMySQL, "mysql-wire"), description.ResultColumns)
		for _, column := range schema.Columns {
			packets = append(packets, Packet{SequenceID: sequence, Payload: NewColumnDefinition(column).Payload()})
			sequence++
		}
		packets = append(packets, Packet{SequenceID: sequence, Payload: eofPayload(description.Statement.Warnings)})
	}
	return CommandResponse{Kind: CommandResponsePrepared, Packets: packets}, nil
}

func preparedParameterTypeFromRef(ref qsbridge.ParameterRef) PreparedParameterType {
	switch ref.Type {
	case qsbridge.DataTypeBool:
		return PreparedParameterType{Type: ColumnTypeTiny}
	case qsbridge.DataTypeInt:
		return PreparedParameterType{Type: ColumnTypeLongLong}
	case qsbridge.DataTypeFloat:
		return PreparedParameterType{Type: ColumnTypeDouble}
	case qsbridge.DataTypeTime:
		return PreparedParameterType{Type: ColumnTypeDateTime}
	case qsbridge.DataTypeString:
		return PreparedParameterType{Type: ColumnTypeVarString}
	default:
		return PreparedParameterType{Type: ColumnTypeVarString}
	}
}

func clonePreparedParameterTypes(types []PreparedParameterType) []PreparedParameterType {
	if len(types) == 0 {
		return nil
	}
	return append([]PreparedParameterType(nil), types...)
}

func parameterColumnDefinition(ref qsbridge.ParameterRef) ColumnDefinition {
	name := ref.Name
	if name == "" {
		name = "?" + strconv.Itoa(ref.Index)
	}
	definition := NewColumnDefinition(qsbridge.ProtocolColumn{
		Name:        name,
		Nullable:    ref.Nullable,
		LogicalType: ref.Type,
	})
	definition.OriginalName = name
	return definition
}

func decodePreparedBinaryValue(payload []byte, offset int, parameterType PreparedParameterType) (qsbridge.ValueKind, any, int, error) {
	switch parameterType.Type {
	case ColumnTypeNull:
		return qsbridge.ValueNull, nil, offset, nil
	case ColumnTypeTiny:
		if offset+1 > len(payload) {
			return "", nil, offset, fmt.Errorf("tiny integer truncated")
		}
		if parameterType.Unsigned {
			return qsbridge.ValueInt, uint64(payload[offset]), offset + 1, nil
		}
		return qsbridge.ValueInt, int64(int8(payload[offset])), offset + 1, nil
	case ColumnTypeShort:
		if offset+2 > len(payload) {
			return "", nil, offset, fmt.Errorf("short integer truncated")
		}
		raw := readUint16LE(payload[offset : offset+2])
		if parameterType.Unsigned {
			return qsbridge.ValueInt, uint64(raw), offset + 2, nil
		}
		return qsbridge.ValueInt, int64(int16(raw)), offset + 2, nil
	case ColumnTypeLong, ColumnTypeInt24:
		if offset+4 > len(payload) {
			return "", nil, offset, fmt.Errorf("integer truncated")
		}
		raw := readUint32LE(payload[offset : offset+4])
		if parameterType.Unsigned {
			return qsbridge.ValueInt, uint64(raw), offset + 4, nil
		}
		return qsbridge.ValueInt, int64(int32(raw)), offset + 4, nil
	case ColumnTypeLongLong:
		if offset+8 > len(payload) {
			return "", nil, offset, fmt.Errorf("long integer truncated")
		}
		raw := readUint64LE(payload[offset : offset+8])
		if parameterType.Unsigned {
			return qsbridge.ValueInt, raw, offset + 8, nil
		}
		return qsbridge.ValueInt, int64(raw), offset + 8, nil
	case ColumnTypeFloat:
		if offset+4 > len(payload) {
			return "", nil, offset, fmt.Errorf("float truncated")
		}
		raw := readUint32LE(payload[offset : offset+4])
		return qsbridge.ValueFloat, float64(math.Float32frombits(raw)), offset + 4, nil
	case ColumnTypeDouble:
		if offset+8 > len(payload) {
			return "", nil, offset, fmt.Errorf("double truncated")
		}
		raw := readUint64LE(payload[offset : offset+8])
		return qsbridge.ValueFloat, math.Float64frombits(raw), offset + 8, nil
	case ColumnTypeDate, ColumnTypeNewDate, ColumnTypeDateTime, ColumnTypeTimestamp:
		value, next, err := readBinaryDateTime(payload, offset)
		if err != nil {
			return "", nil, offset, err
		}
		return qsbridge.ValueTime, value, next, nil
	case ColumnTypeTime:
		value, next, err := readBinaryTimeString(payload, offset)
		if err != nil {
			return "", nil, offset, err
		}
		return qsbridge.ValueString, value, next, nil
	case ColumnTypeString, ColumnTypeVarString, ColumnTypeVarchar,
		ColumnTypeBlob, ColumnTypeTinyBlob, ColumnTypeMediumBlob, ColumnTypeLongBlob,
		ColumnTypeDecimal, ColumnTypeNewDecimal, ColumnTypeBit:
		raw, next, ok := readLengthEncodedBytes(payload, offset)
		if !ok {
			return "", nil, offset, fmt.Errorf("length-encoded value truncated")
		}
		return qsbridge.ValueString, string(raw), next, nil
	default:
		return "", nil, offset, fmt.Errorf("unsupported mysql prepared parameter type 0x%02x", byte(parameterType.Type))
	}
}

func readBinaryDateTime(payload []byte, offset int) (time.Time, int, error) {
	if offset >= len(payload) {
		return time.Time{}, offset, fmt.Errorf("datetime length truncated")
	}
	length := int(payload[offset])
	offset++
	if length == 0 {
		return time.Time{}, offset, nil
	}
	if length != 4 && length != 7 && length != 11 {
		return time.Time{}, offset, fmt.Errorf("unsupported datetime length %d", length)
	}
	if offset+length > len(payload) {
		return time.Time{}, offset, fmt.Errorf("datetime payload truncated")
	}
	year := int(readUint16LE(payload[offset : offset+2]))
	month := time.Month(payload[offset+2])
	day := int(payload[offset+3])
	hour, minute, second, nanosecond := 0, 0, 0, 0
	if length >= 7 {
		hour = int(payload[offset+4])
		minute = int(payload[offset+5])
		second = int(payload[offset+6])
	}
	if length == 11 {
		microseconds := readUint32LE(payload[offset+7 : offset+11])
		nanosecond = int(microseconds) * 1000
	}
	return time.Date(year, month, day, hour, minute, second, nanosecond, time.UTC), offset + length, nil
}

func readBinaryTimeString(payload []byte, offset int) (string, int, error) {
	if offset >= len(payload) {
		return "", offset, fmt.Errorf("time length truncated")
	}
	length := int(payload[offset])
	offset++
	if length == 0 {
		return "00:00:00", offset, nil
	}
	if length != 8 && length != 12 {
		return "", offset, fmt.Errorf("unsupported time length %d", length)
	}
	if offset+length > len(payload) {
		return "", offset, fmt.Errorf("time payload truncated")
	}
	sign := ""
	if payload[offset] != 0 {
		sign = "-"
	}
	days := readUint32LE(payload[offset+1 : offset+5])
	hours := payload[offset+5]
	minutes := payload[offset+6]
	seconds := payload[offset+7]
	next := offset + length
	totalHours := uint64(days)*24 + uint64(hours)
	if length == 12 {
		microseconds := readUint32LE(payload[offset+8 : offset+12])
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, totalHours, minutes, seconds, microseconds), next, nil
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, totalHours, minutes, seconds), next, nil
}

func readLengthEncodedBytes(data []byte, offset int) ([]byte, int, bool) {
	length, next, ok := readLengthEncodedInteger(data, offset)
	if !ok || length > uint64(len(data)-next) {
		return nil, offset, false
	}
	return append([]byte(nil), data[next:next+int(length)]...), next + int(length), true
}

func readLengthEncodedInteger(data []byte, offset int) (uint64, int, bool) {
	if offset >= len(data) {
		return 0, offset, false
	}
	first := data[offset]
	switch first {
	case 0xfc:
		if offset+3 > len(data) {
			return 0, offset, false
		}
		return uint64(readUint16LE(data[offset+1 : offset+3])), offset + 3, true
	case 0xfd:
		if offset+4 > len(data) {
			return 0, offset, false
		}
		value := uint64(data[offset+1]) | uint64(data[offset+2])<<8 | uint64(data[offset+3])<<16
		return value, offset + 4, true
	case 0xfe:
		if offset+9 > len(data) {
			return 0, offset, false
		}
		return readUint64LE(data[offset+1 : offset+9]), offset + 9, true
	default:
		if first >= 0xfb {
			return 0, offset, false
		}
		return uint64(first), offset + 1, true
	}
}

func readUint16LE(data []byte) uint16 {
	return uint16(data[0]) | uint16(data[1])<<8
}

func readUint64LE(data []byte) uint64 {
	return uint64(data[0]) |
		uint64(data[1])<<8 |
		uint64(data[2])<<16 |
		uint64(data[3])<<24 |
		uint64(data[4])<<32 |
		uint64(data[5])<<40 |
		uint64(data[6])<<48 |
		uint64(data[7])<<56
}
