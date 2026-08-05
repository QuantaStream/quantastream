package core

import (
	"fmt"
	"time"
)

// BSIPrimaryKeyBackend is the storage boundary needed by a BSI-backed primary
// key resolver. Implementations own the actual bitmap/vector representation.
type BSIPrimaryKeyBackend interface {
	LookupPrimaryKey(BSIPrimaryKeyLookupRequest) (BSIPrimaryKeyLookupResult, error)
	StagePrimaryKey(BSIPrimaryKeyStageRequest) error
}

// BSIPrimaryKeyLookupRequest carries typed primary-key values to a BSI lookup.
type BSIPrimaryKeyLookupRequest struct {
	TableName      string
	PrimaryKey     string
	Attributes     []*Attribute
	Values         []interface{}
	RenderedValue  string
	ShardTimestamp time.Time
	PrimaryKeyMode PrimaryKeyMode
}

// BSIPrimaryKeyLookupResult describes a BSI primary-key lookup result.
type BSIPrimaryKeyLookupResult struct {
	ColumnID uint64
	Found    bool
}

// BSIPrimaryKeyStageRequest stages a typed primary-key mapping for a rownum.
type BSIPrimaryKeyStageRequest struct {
	TableName      string
	PrimaryKey     string
	Attributes     []*Attribute
	Values         []interface{}
	RenderedValue  string
	ShardTimestamp time.Time
	ColumnID       uint64
}

// BSIPrimaryKeyResolver resolves primary keys through a typed BSI backend. It is
// the native primary-key authority path; callers still inject the backend
// explicitly while durable storage and recovery semantics mature.
type BSIPrimaryKeyResolver struct {
	Backend BSIPrimaryKeyBackend
}

// NewBSIPrimaryKeyResolver returns a BSI-backed primary-key resolver.
func NewBSIPrimaryKeyResolver(backend BSIPrimaryKeyBackend) BSIPrimaryKeyResolver {
	return BSIPrimaryKeyResolver{Backend: backend}
}

// ResolvePrimaryKeyColumnID resolves or allocates a rownum through the BSI
// primary-key backend.
func (r BSIPrimaryKeyResolver) ResolvePrimaryKeyColumnID(req PrimaryKeyResolveRequest) (PrimaryKeyResolveResult, error) {
	totalStart := time.Now()
	profile := PrimaryKeyResolveProfile{ResolveCount: 1}
	finish := func(columnID uint64, existingRow bool) PrimaryKeyResolveResult {
		profile.TotalElapsed = time.Since(totalStart)
		return PrimaryKeyResolveResult{
			ColumnID:    columnID,
			ExistingRow: existingRow,
			Profile:     profile,
		}
	}

	if r.Backend == nil {
		return finish(0, false), fmt.Errorf("BSI primary key resolver backend is nil")
	}
	session := req.Session
	tbuf := req.TableBuffer
	if session == nil {
		return finish(0, false), fmt.Errorf("primary key resolver session is nil")
	}
	if tbuf == nil {
		return finish(0, false), fmt.Errorf("primary key resolver table buffer is nil")
	}
	if !tbuf.ShouldLookupPrimaryKey() {
		return r.resolveDirectColumnID(req, tbuf, &profile, finish)
	}
	if err := validateBSIPrimaryKeyValues(req, tbuf); err != nil {
		return finish(0, false), err
	}

	profile.LookupRequiredCount++
	if req.PrimaryKeyMode.assumeNew() {
		profile.AssumeNewCount++
		profile.SkippedBSILookupCount++
	} else {
		lookupStart := time.Now()
		profile.BSILookupCount++
		lookup, err := r.Backend.LookupPrimaryKey(newBSIPrimaryKeyLookupRequest(req, tbuf))
		profile.BSILookupElapsed += time.Since(lookupStart)
		if err != nil {
			return finish(0, false), fmt.Errorf("BSI primary key lookup error - %w", err)
		}
		if lookup.Found {
			profile.BSIHitCount++
			tbuf.CurrentColumnID = lookup.ColumnID
			return finish(lookup.ColumnID, true), nil
		}
	}

	if err := allocatePrimaryKeyColumnID(req, tbuf, &profile); err != nil {
		return finish(0, false), err
	}
	stageStart := time.Now()
	profile.BSIStageWriteCount++
	stageReq := newBSIPrimaryKeyStageRequest(req, tbuf)
	if err := r.Backend.StagePrimaryKey(stageReq); err != nil {
		profile.BSIStageWriteElapsed += time.Since(stageStart)
		return finish(0, false), fmt.Errorf("BSI primary key stage error - %w", err)
	}
	profile.BSIStageWriteElapsed += time.Since(stageStart)
	return finish(tbuf.CurrentColumnID, false), nil
}

func validateBSIPrimaryKeyValues(req PrimaryKeyResolveRequest, tbuf *TableBuffer) error {
	if len(req.PrimaryKeyValues) != len(tbuf.PKAttributes) {
		return fmt.Errorf("BSI primary key resolver requires %d typed primary-key values, got %d",
			len(tbuf.PKAttributes), len(req.PrimaryKeyValues))
	}
	for i, value := range req.PrimaryKeyValues {
		if value == nil {
			fieldName := fmt.Sprintf("value%d", i)
			if i < len(tbuf.PKAttributes) && tbuf.PKAttributes[i] != nil {
				fieldName = tbuf.PKAttributes[i].FieldName
			}
			return fmt.Errorf("BSI primary key resolver requires non-nil typed primary-key value for %s", fieldName)
		}
	}
	return nil
}

func (r BSIPrimaryKeyResolver) resolveDirectColumnID(
	req PrimaryKeyResolveRequest,
	tbuf *TableBuffer,
	profile *PrimaryKeyResolveProfile,
	finish func(uint64, bool) PrimaryKeyResolveResult,
) (PrimaryKeyResolveResult, error) {
	if req.DirectColumnID {
		profile.DirectColumnIDCount++
		return finish(tbuf.CurrentColumnID, false), nil
	}
	if err := allocatePrimaryKeyColumnID(req, tbuf, profile); err != nil {
		return finish(0, false), err
	}
	return finish(tbuf.CurrentColumnID, false), nil
}

func allocatePrimaryKeyColumnID(req PrimaryKeyResolveRequest, tbuf *TableBuffer, profile *PrimaryKeyResolveProfile) error {
	if req.ProvidedColumnID != 0 {
		profile.ProvidedColumnIDCount++
		tbuf.CurrentColumnID = req.ProvidedColumnID
		return nil
	}
	allocationStart := time.Now()
	profile.RownumAllocationCount++
	if err := tbuf.NextColumnID(req.Session.BitIndex); err != nil {
		profile.RownumAllocationElapsed += time.Since(allocationStart)
		return err
	}
	profile.RownumAllocationElapsed += time.Since(allocationStart)
	return nil
}

func newBSIPrimaryKeyLookupRequest(req PrimaryKeyResolveRequest, tbuf *TableBuffer) BSIPrimaryKeyLookupRequest {
	return BSIPrimaryKeyLookupRequest{
		TableName:      tbuf.Table.Name,
		PrimaryKey:     tbuf.Table.PrimaryKey,
		Attributes:     append([]*Attribute(nil), tbuf.PKAttributes...),
		Values:         append([]interface{}(nil), req.PrimaryKeyValues...),
		RenderedValue:  req.LookupValue,
		ShardTimestamp: tbuf.CurrentTimestamp,
		PrimaryKeyMode: req.PrimaryKeyMode.normalize(),
	}
}

func newBSIPrimaryKeyStageRequest(req PrimaryKeyResolveRequest, tbuf *TableBuffer) BSIPrimaryKeyStageRequest {
	return BSIPrimaryKeyStageRequest{
		TableName:      tbuf.Table.Name,
		PrimaryKey:     tbuf.Table.PrimaryKey,
		Attributes:     append([]*Attribute(nil), tbuf.PKAttributes...),
		Values:         append([]interface{}(nil), req.PrimaryKeyValues...),
		RenderedValue:  req.LookupValue,
		ShardTimestamp: tbuf.CurrentTimestamp,
		ColumnID:       tbuf.CurrentColumnID,
	}
}
