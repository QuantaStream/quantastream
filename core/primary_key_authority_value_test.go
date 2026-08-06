package core

import (
	"encoding/binary"
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

func TestEncodeCompoundPrimaryKeyAuthorityValuePacksNumericFields(t *testing.T) {
	orderKey := primaryKeyAuthorityValueTestAttribute("l_orderkey", "Integer", "IntBSI")
	lineNumber := primaryKeyAuthorityValueTestAttribute("l_linenumber", "Integer", "IntBSI")

	value, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "lineitem",
		PrimaryKey: "l_orderkey+l_linenumber",
		Attributes: []*Attribute{orderKey, lineNumber},
		Values:     []interface{}{int64(1001), int64(2)},
	})

	require.NoError(t, err)
	var encoded [16]byte
	binary.BigEndian.PutUint64(encoded[0:8], 1001)
	binary.BigEndian.PutUint64(encoded[8:16], 2)
	require.Equal(t, new(big.Int).SetBytes(encoded[:]), value)
}

func TestEncodeCompoundPrimaryKeyAuthorityValueCoercesIntegerStrings(t *testing.T) {
	partKey := primaryKeyAuthorityValueTestAttribute("ps_partkey", "Integer", "ParentRelation")
	suppKey := primaryKeyAuthorityValueTestAttribute("ps_suppkey", "Integer", "ParentRelation")

	typed, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "partsupp",
		PrimaryKey: "ps_partkey+ps_suppkey",
		Attributes: []*Attribute{partKey, suppKey},
		Values:     []interface{}{int64(1001), int64(2)},
	})
	require.NoError(t, err)
	fromStrings, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "partsupp",
		PrimaryKey: "ps_partkey+ps_suppkey",
		Attributes: []*Attribute{partKey, suppKey},
		Values:     []interface{}{"1001", "2"},
	})
	require.NoError(t, err)

	require.Equal(t, typed, fromStrings)
}

func TestEncodeCompoundPrimaryKeyAuthorityValueAvoidsDecimalConcatenationCollision(t *testing.T) {
	left := primaryKeyAuthorityValueTestAttribute("left_id", "Integer", "IntBSI")
	right := primaryKeyAuthorityValueTestAttribute("right_id", "Integer", "IntBSI")

	first, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "sample",
		PrimaryKey: "left_id+right_id",
		Attributes: []*Attribute{left, right},
		Values:     []interface{}{int64(1), int64(23)},
	})
	require.NoError(t, err)
	second, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "sample",
		PrimaryKey: "left_id+right_id",
		Attributes: []*Attribute{left, right},
		Values:     []interface{}{int64(12), int64(3)},
	})
	require.NoError(t, err)

	require.NotEqual(t, first, second)
}

func TestEncodeCompoundPrimaryKeyAuthorityValuePreservesFieldOrder(t *testing.T) {
	orderKey := primaryKeyAuthorityValueTestAttribute("l_orderkey", "Integer", "IntBSI")
	lineNumber := primaryKeyAuthorityValueTestAttribute("l_linenumber", "Integer", "IntBSI")

	orderThenLine, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "lineitem",
		PrimaryKey: "l_orderkey+l_linenumber",
		Attributes: []*Attribute{orderKey, lineNumber},
		Values:     []interface{}{int64(1001), int64(2)},
	})
	require.NoError(t, err)
	lineThenOrder, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "lineitem",
		PrimaryKey: "l_linenumber+l_orderkey",
		Attributes: []*Attribute{lineNumber, orderKey},
		Values:     []interface{}{int64(2), int64(1001)},
	})
	require.NoError(t, err)

	require.NotEqual(t, orderThenLine, lineThenOrder)
}

func TestEncodeCompoundPrimaryKeyAuthorityValueSupportsBigIntAndTimestamps(t *testing.T) {
	eventTime := primaryKeyAuthorityValueTestAttribute("event_time", "DateTime", "TimestampBSI")
	eventID := primaryKeyAuthorityValueTestAttribute("event_id", "Integer", "IntBSI")
	ts := time.Unix(100, 25).UTC()

	value, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "events",
		PrimaryKey: "event_time+event_id",
		Attributes: []*Attribute{eventTime, eventID},
		Values:     []interface{}{ts, big.NewInt(9001)},
	})

	require.NoError(t, err)
	var encoded [16]byte
	binary.BigEndian.PutUint64(encoded[0:8], uint64(ts.UnixNano()))
	binary.BigEndian.PutUint64(encoded[8:16], 9001)
	require.Equal(t, new(big.Int).SetBytes(encoded[:]), value)
}

func TestEncodeCompoundPrimaryKeyAuthorityValueRejectsUnsupportedValues(t *testing.T) {
	left := primaryKeyAuthorityValueTestAttribute("left_value", "String", "StringLexBSI")
	right := primaryKeyAuthorityValueTestAttribute("right_value", "String", "StringLexBSI")

	_, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "sample",
		PrimaryKey: "left_value+right_value",
		Attributes: []*Attribute{left, right},
		Values:     []interface{}{"1", "2"},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "left_value")
	require.Contains(t, err.Error(), "unsupported value type string")
}

func TestEncodeCompoundPrimaryKeyAuthorityValueRejectsInvalidIntegerStrings(t *testing.T) {
	left := primaryKeyAuthorityValueTestAttribute("left_id", "Integer", "IntBSI")
	right := primaryKeyAuthorityValueTestAttribute("right_id", "Integer", "IntBSI")

	_, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "sample",
		PrimaryKey: "left_id+right_id",
		Attributes: []*Attribute{left, right},
		Values:     []interface{}{"not-an-int", "2"},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "left_id")
	require.Contains(t, err.Error(), "parse integer string")
}

func primaryKeyAuthorityValueTestAttribute(fieldName string, fieldType string, mapping string) *Attribute {
	return &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       fieldName,
			SourceName:      fieldName,
			Type:            fieldType,
			MappingStrategy: mapping,
		},
	}
}
