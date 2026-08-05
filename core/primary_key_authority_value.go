package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"
)

const primaryKeyAuthorityValueDefaultIntegerBytes = 8

// PrimaryKeyAuthorityValueEncodingRequest carries typed primary-key values to
// the physical BSI authority value encoder.
type PrimaryKeyAuthorityValueEncodingRequest struct {
	TableName  string
	PrimaryKey string
	Attributes []*Attribute
	Values     []interface{}
}

// EncodeCompoundPrimaryKeyAuthorityValue packs a compound typed primary key
// into an exact BSI-comparable integer. The default encoding uses fixed-width
// big-endian components, starting with 8-byte integer/timestamp values.
func EncodeCompoundPrimaryKeyAuthorityValue(req PrimaryKeyAuthorityValueEncodingRequest) (*big.Int, error) {
	if req.TableName == "" {
		return nil, fmt.Errorf("compound primary-key authority value encoding requires table name")
	}
	if req.PrimaryKey == "" {
		return nil, fmt.Errorf("compound primary-key authority value encoding requires primary key name")
	}
	if len(req.Values) < 2 {
		return nil, fmt.Errorf("compound primary-key authority value encoding requires at least two typed values")
	}
	if len(req.Attributes) != len(req.Values) {
		return nil, fmt.Errorf("compound primary-key authority value encoding requires %d attributes, got %d",
			len(req.Values), len(req.Attributes))
	}

	var buf bytes.Buffer
	for i, value := range req.Values {
		if err := appendCompoundPrimaryKeyAuthorityValueComponent(&buf, req.Attributes[i], value); err != nil {
			return nil, err
		}
	}
	return new(big.Int).SetBytes(buf.Bytes()), nil
}

func optionalCompoundPrimaryKeyAuthorityValue(
	tableName string,
	primaryKey string,
	attrs []*Attribute,
	values []interface{},
) *big.Int {
	if len(values) < 2 {
		return nil
	}
	value, err := EncodeCompoundPrimaryKeyAuthorityValue(PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  tableName,
		PrimaryKey: primaryKey,
		Attributes: attrs,
		Values:     values,
	})
	if err != nil {
		return nil
	}
	return value
}

func appendCompoundPrimaryKeyAuthorityValueComponent(buf *bytes.Buffer, attr *Attribute, value interface{}) error {
	if value == nil {
		return compoundPrimaryKeyAuthorityValueComponentError(attr, "nil values are not supported")
	}
	var scratch [primaryKeyAuthorityValueDefaultIntegerBytes]byte
	switch typed := value.(type) {
	case int:
		binary.BigEndian.PutUint64(scratch[:], uint64(int64(typed)))
	case int8:
		binary.BigEndian.PutUint64(scratch[:], uint64(int64(typed)))
	case int16:
		binary.BigEndian.PutUint64(scratch[:], uint64(int64(typed)))
	case int32:
		binary.BigEndian.PutUint64(scratch[:], uint64(int64(typed)))
	case int64:
		binary.BigEndian.PutUint64(scratch[:], uint64(typed))
	case uint:
		binary.BigEndian.PutUint64(scratch[:], uint64(typed))
	case uint8:
		binary.BigEndian.PutUint64(scratch[:], uint64(typed))
	case uint16:
		binary.BigEndian.PutUint64(scratch[:], uint64(typed))
	case uint32:
		binary.BigEndian.PutUint64(scratch[:], uint64(typed))
	case uint64:
		binary.BigEndian.PutUint64(scratch[:], typed)
	case *big.Int:
		if typed == nil {
			return compoundPrimaryKeyAuthorityValueComponentError(attr, "nil big.Int values are not supported")
		}
		if typed.Sign() < 0 || typed.BitLen() > 64 {
			return compoundPrimaryKeyAuthorityValueComponentError(attr, "big.Int values must fit in unsigned 64-bit default encoding")
		}
		binary.BigEndian.PutUint64(scratch[:], typed.Uint64())
	case time.Time:
		binary.BigEndian.PutUint64(scratch[:], uint64(typed.UTC().UnixNano()))
	default:
		return compoundPrimaryKeyAuthorityValueComponentError(attr, fmt.Sprintf("unsupported value type %T", value))
	}
	buf.Write(scratch[:])
	return nil
}

func compoundPrimaryKeyAuthorityValueComponentError(attr *Attribute, detail string) error {
	fieldName := "(unknown)"
	if attr != nil && attr.BasicAttribute != nil && attr.FieldName != "" {
		fieldName = attr.FieldName
	}
	return fmt.Errorf("compound primary-key authority value encoding for %s: %s", fieldName, detail)
}
