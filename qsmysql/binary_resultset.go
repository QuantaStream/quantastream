package qsmysql

import (
	"fmt"
	"math"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// BinaryResultSetPackets encodes a protocol-neutral query result into MySQL
// binary-result packets for COM_STMT_EXECUTE responses.
func BinaryResultSetPackets(result qsbridge.ExecutionResult) ([]Packet, error) {
	if len(result.Columns) == 0 {
		return nil, fmt.Errorf("mysql binary resultset requires at least one column")
	}
	schema := result.ProtocolResultSchema(qsbridge.NewProtocolProfile(qsbridge.ProtocolMySQL, "mysql-wire"))
	packets := make([]Packet, 0, 1+len(schema.Columns)+2+int(result.RowsReturned))
	sequence := byte(1)
	packets = append(packets, Packet{SequenceID: sequence, Payload: encodeLengthEncodedInteger(uint64(len(schema.Columns)))})
	sequence++
	for _, column := range schema.Columns {
		packets = append(packets, Packet{SequenceID: sequence, Payload: NewColumnDefinition(column).Payload()})
		sequence++
	}
	packets = append(packets, Packet{SequenceID: sequence, Payload: eofPayload(0)})
	sequence++
	for _, chunk := range result.Chunks {
		for _, row := range chunk.Rows {
			payload, err := binaryRowPayload(row, schema.Columns)
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

func binaryRowPayload(row qsbridge.ResultRow, columns []qsbridge.ProtocolColumn) ([]byte, error) {
	if len(row) != len(columns) {
		return nil, fmt.Errorf("mysql binary row has %d cells for %d columns", len(row), len(columns))
	}
	nullBitmap := make([]byte, (len(columns)+9)/8)
	payload := []byte{0x00}
	payload = append(payload, nullBitmap...)
	for i, cell := range row {
		if cell.Kind == qsbridge.ValueNull || cell.Value == nil {
			setBinaryResultNullBit(payload[1:], i)
			continue
		}
		var err error
		payload, err = appendBinaryCellValue(payload, cell, columns[i])
		if err != nil {
			return nil, fmt.Errorf("mysql binary column %d: %w", i+1, err)
		}
	}
	return payload, nil
}

func setBinaryResultNullBit(bitmap []byte, column int) {
	bit := column + 2
	bitmap[bit/8] |= 1 << uint(bit%8)
}

func appendBinaryCellValue(out []byte, cell qsbridge.ResultCell, column qsbridge.ProtocolColumn) ([]byte, error) {
	switch column.LogicalType {
	case qsbridge.DataTypeBool:
		if value, ok := boolCellValue(cell); ok {
			if value {
				return append(out, 1), nil
			}
			return append(out, 0), nil
		}
	case qsbridge.DataTypeInt:
		if value, ok := int64CellValue(cell); ok {
			return appendUint64LE(out, uint64(value)), nil
		}
	case qsbridge.DataTypeFloat:
		if value, ok := float64CellValue(cell); ok {
			return appendUint64LE(out, math.Float64bits(value)), nil
		}
	case qsbridge.DataTypeTime:
		if value, ok := timeCellValue(cell); ok {
			return appendBinaryDateTime(out, value), nil
		}
	case qsbridge.DataTypeString, qsbridge.DataTypeUnknown:
		return appendLengthEncodedString(out, fmt.Sprint(cell.Value)), nil
	}
	return nil, fmt.Errorf("cannot encode %s cell %T as %s", cell.Kind, cell.Value, column.LogicalType)
}

func appendBinaryDateTime(out []byte, value time.Time) []byte {
	if value.IsZero() {
		return append(out, 0)
	}
	value = value.UTC()
	out = append(out, 7)
	out = appendUint16LE(out, uint16(value.Year()))
	out = append(out, byte(value.Month()), byte(value.Day()), byte(value.Hour()), byte(value.Minute()), byte(value.Second()))
	return out
}

func boolCellValue(cell qsbridge.ResultCell) (bool, bool) {
	switch value := cell.Value.(type) {
	case bool:
		return value, true
	case int:
		return value != 0, true
	case int8:
		return value != 0, true
	case int16:
		return value != 0, true
	case int32:
		return value != 0, true
	case int64:
		return value != 0, true
	case uint:
		return value != 0, true
	case uint8:
		return value != 0, true
	case uint16:
		return value != 0, true
	case uint32:
		return value != 0, true
	case uint64:
		return value != 0, true
	default:
		return false, false
	}
}

func int64CellValue(cell qsbridge.ResultCell) (int64, bool) {
	switch value := cell.Value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		return int64(value), true
	default:
		return 0, false
	}
}

func float64CellValue(cell qsbridge.ResultCell) (float64, bool) {
	switch value := cell.Value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return value, true
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	default:
		return 0, false
	}
}

func timeCellValue(cell qsbridge.ResultCell) (time.Time, bool) {
	if value, ok := cell.Value.(time.Time); ok {
		return value, true
	}
	return time.Time{}, false
}
