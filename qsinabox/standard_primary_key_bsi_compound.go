package qsinabox

import (
	"fmt"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// StandardCompoundBSIPrimaryKeyBackend stores compact compound primary-key
// authority values in the hidden catalog system BSI for inabox-standard.
type StandardCompoundBSIPrimaryKeyBackend struct {
	Table   *core.Table
	Reader  StandardSingleColumnBSIPrimaryKeyReader
	Session *core.Session
}

var _ core.BSIPrimaryKeyBackend = StandardCompoundBSIPrimaryKeyBackend{}
var _ core.BSIPrimaryKeyDomainStateBackend = StandardCompoundBSIPrimaryKeyBackend{}

// LookupPrimaryKey resolves the compact compound authority value to matching
// rownums.
func (b StandardCompoundBSIPrimaryKeyBackend) LookupPrimaryKey(req core.BSIPrimaryKeyLookupRequest) (core.BSIPrimaryKeyLookupResult, error) {
	if req.AuthorityValue == nil {
		return core.BSIPrimaryKeyLookupResult{}, fmt.Errorf("compound BSI primary-key lookup requires authority value")
	}
	var profile core.BSIPrimaryKeyLookupProfile
	fromTime, toTime := b.lookupWindowNanos(req.ShardTimestamp)
	projectionStart := time.Now()
	bsi, cacheLookup, cacheHit, err := b.Reader.projectCachedPrimaryKeyBSI(req.TableName, shared.CompoundPrimaryKeyAuthorityFieldName, fromTime, toTime)
	profile.ProjectionElapsed = time.Since(projectionStart)
	if cacheLookup {
		profile.ProjectionCacheLookupCount++
	}
	if cacheHit {
		profile.ProjectionCacheHitCount++
	}
	if err != nil {
		return core.BSIPrimaryKeyLookupResult{}, err
	}
	if bsi == nil {
		return core.BSIPrimaryKeyLookupResult{Profile: profile}, nil
	}
	compareStart := time.Now()
	matches := bsi.CompareBigValue(0, roaring64.EQ, req.AuthorityValue, nil, nil)
	profile.CompareElapsed = time.Since(compareStart)
	extractionStart := time.Now()
	matchedColumnIDs := standardSingleColumnBSIPrimaryKeyColumnIDs(matches)
	profile.MatchExtractionElapsed = time.Since(extractionStart)
	return core.BSIPrimaryKeyLookupResult{
		MatchedColumnIDs: matchedColumnIDs,
		Profile:          profile,
	}, nil
}

// PrimaryKeyDomainState reports whether the hidden compound authority BSI
// contains any committed values for the lookup domain.
func (b StandardCompoundBSIPrimaryKeyBackend) PrimaryKeyDomainState(req core.BSIPrimaryKeyLookupRequest) (core.PrimaryKeyDomainState, error) {
	fromTime, toTime := b.lookupWindowNanos(req.ShardTimestamp)
	if cardinality, ok, err := b.Reader.primaryKeyBSIDomainCardinality(req.TableName,
		shared.CompoundPrimaryKeyAuthorityFieldName, fromTime, toTime); err != nil {
		return core.PrimaryKeyDomainUnknown, err
	} else if ok {
		if cardinality == 0 {
			return core.PrimaryKeyDomainEmpty, nil
		}
		return core.PrimaryKeyDomainNonEmpty, nil
	}
	bsi, _, _, err := b.Reader.projectCachedPrimaryKeyBSI(req.TableName, shared.CompoundPrimaryKeyAuthorityFieldName, fromTime, toTime)
	if err != nil {
		return core.PrimaryKeyDomainUnknown, err
	}
	if bsi == nil || bsi.GetCardinality() == 0 {
		return core.PrimaryKeyDomainEmpty, nil
	}
	return core.PrimaryKeyDomainNonEmpty, nil
}

// StagePrimaryKey stages the compact compound authority value in the hidden BSI.
func (b StandardCompoundBSIPrimaryKeyBackend) StagePrimaryKey(req core.BSIPrimaryKeyStageRequest) error {
	if req.AuthorityValue == nil {
		return fmt.Errorf("compound BSI primary-key stage requires authority value")
	}
	if req.ColumnID == 0 {
		return fmt.Errorf("compound BSI primary-key stage requires column ID")
	}
	if b.Session == nil || b.Session.BatchBuffer == nil {
		return fmt.Errorf("compound BSI primary-key stage requires session batch buffer")
	}
	if err := b.Session.BatchBuffer.SetValue(
		req.TableName,
		shared.CompoundPrimaryKeyAuthorityFieldName,
		req.ColumnID,
		req.AuthorityValue,
		req.ShardTimestamp,
	); err != nil {
		return err
	}
	fromTime, toTime := b.lookupWindowNanos(req.ShardTimestamp)
	b.Reader.stageCachedPrimaryKeyBSI(req.TableName, shared.CompoundPrimaryKeyAuthorityFieldName, fromTime, toTime,
		req.ColumnID, req.AuthorityValue)
	return nil
}

func (b StandardCompoundBSIPrimaryKeyBackend) lookupWindowNanos(shardTimestamp time.Time) (int64, int64) {
	if b.Table != nil && b.Table.TimeQuantumType != "" && !shardTimestamp.IsZero() {
		nanos := shardTimestamp.UTC().UnixNano()
		return nanos, nanos
	}
	return standardProjectionWindowNanos(b.Reader.TableCache, b.tableName(), 0, 0)
}

func (b StandardCompoundBSIPrimaryKeyBackend) tableName() string {
	if b.Table != nil {
		return b.Table.Name
	}
	return ""
}
