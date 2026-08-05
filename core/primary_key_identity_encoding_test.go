package core

import (
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

func TestEncodePrimaryKeyIdentityIsDeterministic(t *testing.T) {
	req := PrimaryKeyIdentityEncodingRequest{
		TableName:      "lineitem",
		PrimaryKey:     "l_orderkey,l_linenumber",
		Attributes:     []*Attribute{primaryKeyIdentityTestAttribute("l_orderkey", "Integer"), primaryKeyIdentityTestAttribute("l_linenumber", "Integer")},
		Values:         []interface{}{int64(1001), int64(2)},
		ShardTimestamp: time.Unix(123, 456).UTC(),
	}

	first, err := EncodePrimaryKeyIdentity(req)
	require.NoError(t, err)
	second, err := EncodePrimaryKeyIdentity(req)
	require.NoError(t, err)

	require.NotEmpty(t, first)
	require.Equal(t, first, second)
	require.Equal(t, primaryKeyIdentityEncodingVersion, first[0])
}

func TestEncodePrimaryKeyIdentityKeepsTypedValuesDistinct(t *testing.T) {
	attr := primaryKeyIdentityTestAttribute("order_id", "Integer")
	intKey, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "orders",
		PrimaryKey: "order_id",
		Attributes: []*Attribute{attr},
		Values:     []interface{}{int64(42)},
	})
	require.NoError(t, err)

	stringKey, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "orders",
		PrimaryKey: "order_id",
		Attributes: []*Attribute{attr},
		Values:     []interface{}{"42"},
	})
	require.NoError(t, err)

	require.NotEqual(t, intKey, stringKey)
}

func TestEncodePrimaryKeyIdentityIsDelimiterSafe(t *testing.T) {
	leftAttrs := []*Attribute{primaryKeyIdentityTestAttribute("a", "String"), primaryKeyIdentityTestAttribute("b", "String")}
	left, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "compound",
		PrimaryKey: "a,b",
		Attributes: leftAttrs,
		Values:     []interface{}{"x\x00+y", "z"},
	})
	require.NoError(t, err)

	right, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "compound",
		PrimaryKey: "a,b",
		Attributes: leftAttrs,
		Values:     []interface{}{"x", "\x00+yz"},
	})
	require.NoError(t, err)

	require.NotEqual(t, left, right)
}

func TestEncodePrimaryKeyIdentityIncludesKeyShape(t *testing.T) {
	values := []interface{}{int64(1001), int64(2)}
	orderLine, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "lineitem",
		PrimaryKey: "l_orderkey,l_linenumber",
		Attributes: []*Attribute{primaryKeyIdentityTestAttribute("l_orderkey", "Integer"), primaryKeyIdentityTestAttribute("l_linenumber", "Integer")},
		Values:     values,
	})
	require.NoError(t, err)

	lineOrder, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "lineitem",
		PrimaryKey: "l_linenumber,l_orderkey",
		Attributes: []*Attribute{primaryKeyIdentityTestAttribute("l_linenumber", "Integer"), primaryKeyIdentityTestAttribute("l_orderkey", "Integer")},
		Values:     values,
	})
	require.NoError(t, err)

	require.NotEqual(t, orderLine, lineOrder)
}

func TestEncodePrimaryKeyIdentityIncludesShardTimestamp(t *testing.T) {
	attr := primaryKeyIdentityTestAttribute("l_orderkey", "Integer")
	first, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:      "lineitem",
		PrimaryKey:     "l_orderkey",
		Attributes:     []*Attribute{attr},
		Values:         []interface{}{int64(1001)},
		ShardTimestamp: time.Unix(1, 0).UTC(),
	})
	require.NoError(t, err)

	second, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:      "lineitem",
		PrimaryKey:     "l_orderkey",
		Attributes:     []*Attribute{attr},
		Values:         []interface{}{int64(1001)},
		ShardTimestamp: time.Unix(2, 0).UTC(),
	})
	require.NoError(t, err)

	require.NotEqual(t, first, second)
}

func TestEncodePrimaryKeyIdentityRejectsInvalidInput(t *testing.T) {
	attr := primaryKeyIdentityTestAttribute("order_id", "Integer")
	_, err := EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "orders",
		PrimaryKey: "order_id",
		Attributes: []*Attribute{attr},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires typed values")

	_, err = EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "orders",
		PrimaryKey: "order_id",
		Attributes: []*Attribute{attr},
		Values:     []interface{}{int64(42), int64(43)},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires 2 attributes")

	_, err = EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:  "orders",
		PrimaryKey: "order_id",
		Attributes: []*Attribute{attr},
		Values:     []interface{}{float64(42)},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported value type")
}

func primaryKeyIdentityTestAttribute(fieldName string, fieldType string) *Attribute {
	return &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       fieldName,
			SourceName:      fieldName,
			Type:            fieldType,
			MappingStrategy: fieldType + "BSI",
		},
	}
}
