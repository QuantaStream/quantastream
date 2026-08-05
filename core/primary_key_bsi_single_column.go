package core

import (
	"fmt"
	"time"
)

// SingleColumnBSIPrimaryKeyReader reads the existing catalog-designated PK BSI
// for one typed key value and returns matching rownums.
type SingleColumnBSIPrimaryKeyReader interface {
	LookupSingleColumnBSIPrimaryKey(SingleColumnBSIPrimaryKeyReadRequest) (SingleColumnBSIPrimaryKeyReadResult, error)
}

// SingleColumnBSIPrimaryKeyReadRequest carries the narrow lookup shape for a
// single-column, BSI-backed primary key.
type SingleColumnBSIPrimaryKeyReadRequest struct {
	TableName          string
	FieldName          string
	MappingStrategy    string
	Attribute          *Attribute
	Value              interface{}
	ShardTimestamp     time.Time
	RequiresShardScope bool
}

// SingleColumnBSIPrimaryKeyReadResult returns the BSI rownums for the key.
type SingleColumnBSIPrimaryKeyReadResult struct {
	ColumnIDs []uint64
}

// SingleColumnBSIPrimaryKeyBackend adapts an existing PK BSI reader to the
// generic BSIPrimaryKeyBackend contract.
type SingleColumnBSIPrimaryKeyBackend struct {
	Table  *Table
	Reader SingleColumnBSIPrimaryKeyReader
}

// NewSingleColumnBSIPrimaryKeyBackend builds a backend over an existing
// catalog-designated PK BSI.
func NewSingleColumnBSIPrimaryKeyBackend(table *Table, reader SingleColumnBSIPrimaryKeyReader) SingleColumnBSIPrimaryKeyBackend {
	return SingleColumnBSIPrimaryKeyBackend{Table: table, Reader: reader}
}

// LookupPrimaryKey returns the matching rownums from the table's existing PK
// BSI. It intentionally supports only the single-column BSI authority mode.
func (b SingleColumnBSIPrimaryKeyBackend) LookupPrimaryKey(req BSIPrimaryKeyLookupRequest) (BSIPrimaryKeyLookupResult, error) {
	eligibility := ObserveBSIPrimaryKeyAuthorityEligibility(b.Table)
	if !eligibility.Eligible || eligibility.Mode != BSIPrimaryKeyAuthorityModeSingleColumnBSI {
		return BSIPrimaryKeyLookupResult{}, fmt.Errorf("single-column BSI primary-key authority unsupported for table %s: %s", eligibility.TableName, eligibility.Reason)
	}
	if b.Reader == nil {
		return BSIPrimaryKeyLookupResult{}, fmt.Errorf("single-column BSI primary-key reader is nil")
	}
	if len(req.Values) != 1 {
		return BSIPrimaryKeyLookupResult{}, fmt.Errorf("single-column BSI primary-key lookup requires 1 value, got %d", len(req.Values))
	}
	attr, err := b.Table.GetAttribute(eligibility.FieldName)
	if err != nil {
		return BSIPrimaryKeyLookupResult{}, err
	}
	read, err := b.Reader.LookupSingleColumnBSIPrimaryKey(SingleColumnBSIPrimaryKeyReadRequest{
		TableName:          eligibility.TableName,
		FieldName:          eligibility.FieldName,
		MappingStrategy:    eligibility.MappingStrategy,
		Attribute:          attr,
		Value:              req.Values[0],
		ShardTimestamp:     req.ShardTimestamp,
		RequiresShardScope: eligibility.RequiresShardScope,
	})
	if err != nil {
		return BSIPrimaryKeyLookupResult{}, err
	}
	matches := append([]uint64(nil), read.ColumnIDs...)
	result := BSIPrimaryKeyLookupResult{MatchedColumnIDs: matches}
	if len(matches) == 1 {
		result.ColumnID = matches[0]
		result.Found = true
	}
	return result, nil
}

// StagePrimaryKey is intentionally a no-op for the single-column BSI authority
// backend. The existing primary-key field mutation is staged by PutRow's normal
// field mapping path after rownum resolution.
func (b SingleColumnBSIPrimaryKeyBackend) StagePrimaryKey(BSIPrimaryKeyStageRequest) error {
	eligibility := ObserveBSIPrimaryKeyAuthorityEligibility(b.Table)
	if !eligibility.Eligible || eligibility.Mode != BSIPrimaryKeyAuthorityModeSingleColumnBSI {
		return fmt.Errorf("single-column BSI primary-key authority unsupported for table %s: %s", eligibility.TableName, eligibility.Reason)
	}
	return nil
}
