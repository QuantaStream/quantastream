package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

const (
	// PrimaryKeyIdentityEncodingVersion is the durable encoding version used
	// for BSI-backed primary-key authority identities.
	PrimaryKeyIdentityEncodingVersion = 1

	primaryKeyIdentityEncodingVersion byte = byte(PrimaryKeyIdentityEncodingVersion)
)

const (
	primaryKeyIdentityValueBool byte = iota + 1
	primaryKeyIdentityValueInt
	primaryKeyIdentityValueUint
	primaryKeyIdentityValueString
	primaryKeyIdentityValueBytes
	primaryKeyIdentityValueTime
)

// PrimaryKeyIdentityEncodingRequest carries the typed identity material needed
// to produce a deterministic primary-key authority key.
type PrimaryKeyIdentityEncodingRequest struct {
	TableName      string
	PrimaryKey     string
	Attributes     []*Attribute
	Values         []interface{}
	ShardTimestamp time.Time
}

// EncodeBSIPrimaryKeyLookupIdentity encodes a BSI lookup request into the
// durable typed primary-key identity form.
func EncodeBSIPrimaryKeyLookupIdentity(req BSIPrimaryKeyLookupRequest) ([]byte, error) {
	return EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:      req.TableName,
		PrimaryKey:     req.PrimaryKey,
		Attributes:     req.Attributes,
		Values:         req.Values,
		ShardTimestamp: req.ShardTimestamp,
	})
}

// EncodeBSIPrimaryKeyStageIdentity encodes a BSI stage request into the durable
// typed primary-key identity form.
func EncodeBSIPrimaryKeyStageIdentity(req BSIPrimaryKeyStageRequest) ([]byte, error) {
	return EncodePrimaryKeyIdentity(PrimaryKeyIdentityEncodingRequest{
		TableName:      req.TableName,
		PrimaryKey:     req.PrimaryKey,
		Attributes:     req.Attributes,
		Values:         req.Values,
		ShardTimestamp: req.ShardTimestamp,
	})
}

// EncodePrimaryKeyIdentity returns a versioned, length-prefixed encoding for a
// typed primary key. It is intentionally independent of rendered SQL/string
// forms so compound keys can contain delimiters, null bytes, or presentation
// strings without colliding.
func EncodePrimaryKeyIdentity(req PrimaryKeyIdentityEncodingRequest) ([]byte, error) {
	if req.TableName == "" {
		return nil, fmt.Errorf("primary-key identity encoding requires table name")
	}
	if req.PrimaryKey == "" {
		return nil, fmt.Errorf("primary-key identity encoding requires primary key name")
	}
	if len(req.Values) == 0 {
		return nil, fmt.Errorf("primary-key identity encoding requires typed values")
	}
	if len(req.Attributes) != len(req.Values) {
		return nil, fmt.Errorf("primary-key identity encoding requires %d attributes, got %d",
			len(req.Values), len(req.Attributes))
	}

	var buf bytes.Buffer
	buf.WriteByte(primaryKeyIdentityEncodingVersion)
	appendPrimaryKeyIdentityString(&buf, req.TableName)
	appendPrimaryKeyIdentityString(&buf, req.PrimaryKey)
	appendPrimaryKeyIdentityInt(&buf, req.ShardTimestamp.UTC().UnixNano())
	appendPrimaryKeyIdentityUint(&buf, uint64(len(req.Values)))
	for i, value := range req.Values {
		attr := req.Attributes[i]
		appendPrimaryKeyIdentityAttribute(&buf, attr, i)
		if err := appendPrimaryKeyIdentityValue(&buf, value); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func appendPrimaryKeyIdentityAttribute(buf *bytes.Buffer, attr *Attribute, index int) {
	if attr == nil || attr.BasicAttribute == nil {
		appendPrimaryKeyIdentityString(buf, fmt.Sprintf("value%d", index))
		appendPrimaryKeyIdentityString(buf, "")
		appendPrimaryKeyIdentityString(buf, "")
		return
	}
	appendPrimaryKeyIdentityString(buf, attr.FieldName)
	appendPrimaryKeyIdentityString(buf, attr.Type)
	appendPrimaryKeyIdentityString(buf, attr.MappingStrategy)
}

func appendPrimaryKeyIdentityValue(buf *bytes.Buffer, value interface{}) error {
	switch typed := value.(type) {
	case bool:
		buf.WriteByte(primaryKeyIdentityValueBool)
		if typed {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case int:
		buf.WriteByte(primaryKeyIdentityValueInt)
		appendPrimaryKeyIdentityInt(buf, int64(typed))
	case int8:
		buf.WriteByte(primaryKeyIdentityValueInt)
		appendPrimaryKeyIdentityInt(buf, int64(typed))
	case int16:
		buf.WriteByte(primaryKeyIdentityValueInt)
		appendPrimaryKeyIdentityInt(buf, int64(typed))
	case int32:
		buf.WriteByte(primaryKeyIdentityValueInt)
		appendPrimaryKeyIdentityInt(buf, int64(typed))
	case int64:
		buf.WriteByte(primaryKeyIdentityValueInt)
		appendPrimaryKeyIdentityInt(buf, typed)
	case uint:
		buf.WriteByte(primaryKeyIdentityValueUint)
		appendPrimaryKeyIdentityUint(buf, uint64(typed))
	case uint8:
		buf.WriteByte(primaryKeyIdentityValueUint)
		appendPrimaryKeyIdentityUint(buf, uint64(typed))
	case uint16:
		buf.WriteByte(primaryKeyIdentityValueUint)
		appendPrimaryKeyIdentityUint(buf, uint64(typed))
	case uint32:
		buf.WriteByte(primaryKeyIdentityValueUint)
		appendPrimaryKeyIdentityUint(buf, uint64(typed))
	case uint64:
		buf.WriteByte(primaryKeyIdentityValueUint)
		appendPrimaryKeyIdentityUint(buf, typed)
	case string:
		buf.WriteByte(primaryKeyIdentityValueString)
		appendPrimaryKeyIdentityString(buf, typed)
	case []byte:
		buf.WriteByte(primaryKeyIdentityValueBytes)
		appendPrimaryKeyIdentityBytes(buf, typed)
	case time.Time:
		buf.WriteByte(primaryKeyIdentityValueTime)
		appendPrimaryKeyIdentityInt(buf, typed.UTC().UnixNano())
	default:
		return fmt.Errorf("primary-key identity encoding unsupported value type %T", value)
	}
	return nil
}

func appendPrimaryKeyIdentityString(buf *bytes.Buffer, value string) {
	appendPrimaryKeyIdentityBytes(buf, []byte(value))
}

func appendPrimaryKeyIdentityBytes(buf *bytes.Buffer, value []byte) {
	appendPrimaryKeyIdentityUint(buf, uint64(len(value)))
	buf.Write(value)
}

func appendPrimaryKeyIdentityInt(buf *bytes.Buffer, value int64) {
	encoded := uint64(value<<1) ^ uint64(value>>63)
	appendPrimaryKeyIdentityUint(buf, encoded)
}

func appendPrimaryKeyIdentityUint(buf *bytes.Buffer, value uint64) {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], value)
	buf.Write(scratch[:n])
}
