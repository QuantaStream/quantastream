package core

import (
	"fmt"
	"math/big"
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
	TableName  string
	PrimaryKey string
	Attributes []*Attribute
	Values     []interface{}
	// AuthorityValue is an optional exact BSI-comparable value for physical
	// authority artifacts. It is currently produced for supported compound
	// numeric/timestamp keys and is distinct from the versioned Identity bytes.
	AuthorityValue *big.Int
	// Identity is the resolver-produced, typed, versioned authority key.
	// Backends should use it for diagnostics, batch-local identity checks, and
	// fallback authority paths when AuthorityValue is not present.
	Identity []byte
	// RenderedValue is retained for diagnostics and temporary KV-transition
	// paths. It is not the BSI authority identity.
	RenderedValue  string
	ShardTimestamp time.Time
	PrimaryKeyMode PrimaryKeyMode
}

// BSIPrimaryKeyLookupResult describes a BSI primary-key lookup result.
type BSIPrimaryKeyLookupResult struct {
	ColumnID         uint64
	Found            bool
	MatchedColumnIDs []uint64
	Profile          BSIPrimaryKeyLookupProfile
}

// BSIPrimaryKeyLookupProfile is backend-provided timing detail for the BSI
// lookup work hidden behind BSIPrimaryKeyBackend.
type BSIPrimaryKeyLookupProfile struct {
	ProjectionElapsed      time.Duration
	CompareElapsed         time.Duration
	MatchExtractionElapsed time.Duration
}

// BSIPrimaryKeyStageRequest stages a typed primary-key mapping for a rownum.
type BSIPrimaryKeyStageRequest struct {
	TableName  string
	PrimaryKey string
	Attributes []*Attribute
	Values     []interface{}
	// AuthorityValue is an optional exact BSI-comparable value for physical
	// authority artifacts. It is currently produced for supported compound
	// numeric/timestamp keys and is distinct from the versioned Identity bytes.
	AuthorityValue *big.Int
	// Identity is the resolver-produced, typed, versioned authority key.
	// Backends should use it for diagnostics, batch-local identity checks, and
	// fallback authority paths when AuthorityValue is not present.
	Identity []byte
	// RenderedValue is retained for diagnostics and temporary KV-transition
	// paths. It is not the BSI authority identity.
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
	lookupReq, identityEncodeElapsed, authorityEncodeElapsed, err := newBSIPrimaryKeyLookupRequest(req, tbuf)
	profile.BSIIdentityEncodeElapsed += identityEncodeElapsed
	profile.BSIAuthorityEncodeElapsed += authorityEncodeElapsed
	if err != nil {
		return finish(0, false), fmt.Errorf("BSI primary key identity error - %w", err)
	}
	if session.BatchBuffer != nil {
		localLookupStart := time.Now()
		profile.LocalCacheLookupCount++
		if columnID, ok := session.BatchBuffer.LookupLocalCIDForPrimaryKeyIdentity(lookupReq.Identity); ok {
			profile.LocalCacheLookupElapsed += time.Since(localLookupStart)
			if req.ProvidedColumnID != 0 && req.ProvidedColumnID != columnID {
				return finish(0, false), fmt.Errorf("BSI primary key local batch conflict for %s: existing column ID %d, provided column ID %d",
					req.LookupValue, columnID, req.ProvidedColumnID)
			}
			profile.LocalCacheHitCount++
			tbuf.CurrentColumnID = columnID
			return finish(columnID, true), nil
		}
		profile.LocalCacheLookupElapsed += time.Since(localLookupStart)
	}
	if req.PrimaryKeyMode.assumeNew() {
		profile.AssumeNewCount++
		profile.SkippedBSILookupCount++
	} else {
		lookupStart := time.Now()
		profile.BSILookupCount++
		lookup, err := r.Backend.LookupPrimaryKey(lookupReq)
		profile.BSILookupElapsed += time.Since(lookupStart)
		profile.BSIProjectionElapsed += lookup.Profile.ProjectionElapsed
		profile.BSICompareElapsed += lookup.Profile.CompareElapsed
		profile.BSIMatchExtractionElapsed += lookup.Profile.MatchExtractionElapsed
		if err != nil {
			return finish(0, false), fmt.Errorf("BSI primary key lookup error - %w", err)
		}
		matchedColumnIDs := lookup.matchedColumnIDs()
		if len(matchedColumnIDs) > 1 {
			return finish(0, false), fmt.Errorf("BSI primary key authority conflict for %s.%s value %s: matched %d rownums",
				lookupReq.TableName, lookupReq.PrimaryKey, lookupReq.RenderedValue, len(matchedColumnIDs))
		}
		if len(matchedColumnIDs) == 1 {
			profile.BSIHitCount++
			tbuf.CurrentColumnID = matchedColumnIDs[0]
			return finish(matchedColumnIDs[0], true), nil
		}
	}

	if err := allocatePrimaryKeyColumnID(req, tbuf, &profile); err != nil {
		return finish(0, false), err
	}
	stageStart := time.Now()
	profile.BSIStageWriteCount++
	stageReq, identityEncodeElapsed, authorityEncodeElapsed, err := newBSIPrimaryKeyStageRequest(req, tbuf)
	profile.BSIIdentityEncodeElapsed += identityEncodeElapsed
	profile.BSIAuthorityEncodeElapsed += authorityEncodeElapsed
	if err != nil {
		profile.BSIStageWriteElapsed += time.Since(stageStart)
		return finish(0, false), fmt.Errorf("BSI primary key identity error - %w", err)
	}
	if err := r.Backend.StagePrimaryKey(stageReq); err != nil {
		profile.BSIStageWriteElapsed += time.Since(stageStart)
		return finish(0, false), fmt.Errorf("BSI primary key stage error - %w", err)
	}
	profile.BSIStageWriteElapsed += time.Since(stageStart)
	if session.BatchBuffer != nil {
		batchCacheWriteStart := time.Now()
		profile.BatchCacheWriteCount++
		session.BatchBuffer.SetPrimaryKeyIdentity(stageReq.Identity, tbuf.CurrentColumnID)
		profile.BatchCacheWriteElapsed += time.Since(batchCacheWriteStart)
	}
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

func (r BSIPrimaryKeyLookupResult) matchedColumnIDs() []uint64 {
	if len(r.MatchedColumnIDs) > 0 {
		return append([]uint64(nil), r.MatchedColumnIDs...)
	}
	if r.Found {
		return []uint64{r.ColumnID}
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

func newBSIPrimaryKeyLookupRequest(req PrimaryKeyResolveRequest, tbuf *TableBuffer) (BSIPrimaryKeyLookupRequest, time.Duration, time.Duration, error) {
	lookupReq := BSIPrimaryKeyLookupRequest{
		TableName:      tbuf.Table.Name,
		PrimaryKey:     tbuf.Table.PrimaryKey,
		Attributes:     append([]*Attribute(nil), tbuf.PKAttributes...),
		Values:         append([]interface{}(nil), req.PrimaryKeyValues...),
		RenderedValue:  req.LookupValue,
		ShardTimestamp: tbuf.CurrentTimestamp,
		PrimaryKeyMode: req.PrimaryKeyMode.normalize(),
	}
	identityEncodeStart := time.Now()
	identity, err := EncodeBSIPrimaryKeyLookupIdentity(lookupReq)
	identityEncodeElapsed := time.Since(identityEncodeStart)
	if err != nil {
		return BSIPrimaryKeyLookupRequest{}, identityEncodeElapsed, 0, err
	}
	lookupReq.Identity = append([]byte(nil), identity...)
	authorityEncodeStart := time.Now()
	lookupReq.AuthorityValue = optionalCompoundPrimaryKeyAuthorityValue(
		lookupReq.TableName,
		lookupReq.PrimaryKey,
		lookupReq.Attributes,
		lookupReq.Values,
	)
	authorityEncodeElapsed := time.Since(authorityEncodeStart)
	return lookupReq, identityEncodeElapsed, authorityEncodeElapsed, nil
}

func newBSIPrimaryKeyStageRequest(req PrimaryKeyResolveRequest, tbuf *TableBuffer) (BSIPrimaryKeyStageRequest, time.Duration, time.Duration, error) {
	stageReq := BSIPrimaryKeyStageRequest{
		TableName:      tbuf.Table.Name,
		PrimaryKey:     tbuf.Table.PrimaryKey,
		Attributes:     append([]*Attribute(nil), tbuf.PKAttributes...),
		Values:         append([]interface{}(nil), req.PrimaryKeyValues...),
		RenderedValue:  req.LookupValue,
		ShardTimestamp: tbuf.CurrentTimestamp,
		ColumnID:       tbuf.CurrentColumnID,
	}
	identityEncodeStart := time.Now()
	identity, err := EncodeBSIPrimaryKeyStageIdentity(stageReq)
	identityEncodeElapsed := time.Since(identityEncodeStart)
	if err != nil {
		return BSIPrimaryKeyStageRequest{}, identityEncodeElapsed, 0, err
	}
	stageReq.Identity = append([]byte(nil), identity...)
	authorityEncodeStart := time.Now()
	stageReq.AuthorityValue = optionalCompoundPrimaryKeyAuthorityValue(
		stageReq.TableName,
		stageReq.PrimaryKey,
		stageReq.Attributes,
		stageReq.Values,
	)
	authorityEncodeElapsed := time.Since(authorityEncodeStart)
	return stageReq, identityEncodeElapsed, authorityEncodeElapsed, nil
}
