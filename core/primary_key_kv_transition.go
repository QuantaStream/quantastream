package core

import (
	"fmt"
	"time"

	u "github.com/araddon/gou"
)

// KVPrimaryKeyResolver preserves the older KV-backed primary-key lookup and
// rownum assignment behavior only for explicit transition tests and shadow
// comparison runs.
//
// Deprecated: primary-key authority should be BSI-backed. Do not use this in
// production or default session/router paths.
type KVPrimaryKeyResolver struct{}

func (KVPrimaryKeyResolver) ResolvePrimaryKeyColumnID(req PrimaryKeyResolveRequest) (PrimaryKeyResolveResult, error) {
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

	session := req.Session
	tbuf := req.TableBuffer
	if session == nil {
		return finish(0, false), fmt.Errorf("primary key resolver session is nil")
	}
	if tbuf == nil {
		return finish(0, false), fmt.Errorf("primary key resolver table buffer is nil")
	}
	if tbuf.ShouldLookupPrimaryKey() {
		profile.LookupRequiredCount++
		localKey := indexPath(tbuf, tbuf.PKAttributes[0].FieldName, tbuf.Table.PrimaryKey+".PK")
		if req.PrimaryKeyMode.assumeNew() {
			profile.AssumeNewCount++
			profile.SkippedLocalCacheLookupCount++
			profile.SkippedKVLookupCount++
		} else {
			// Can't use batch operation here unfortunately, but at least we have local batch cache.
			localLookupStart := time.Now()
			profile.LocalCacheLookupCount++
			if lColID, ok := session.BatchBuffer.LookupLocalCIDForString(localKey, req.LookupValue); ok {
				profile.LocalCacheLookupElapsed += time.Since(localLookupStart)
				profile.LocalCacheHitCount++
				tbuf.CurrentColumnID = lColID
				u.Warnf("PK %s found in cache.  PK mapping error for %s?", req.LookupValue, tbuf.Table.Name)
				return finish(tbuf.CurrentColumnID, false), nil
			}
			profile.LocalCacheLookupElapsed += time.Since(localLookupStart)
			kvLookupStart := time.Now()
			profile.KVLookupCount++
			colID, found, err := session.lookupColumnID(tbuf, req.LookupValue, "")
			profile.KVLookupElapsed += time.Since(kvLookupStart)
			if err != nil {
				return finish(0, false), fmt.Errorf("Dedup lookup error - %v", err)
			}
			if found {
				profile.KVHitCount++
				tbuf.CurrentColumnID = colID
				return finish(colID, true), nil
			}
		}
		if req.ProvidedColumnID == 0 {
			allocationStart := time.Now()
			profile.RownumAllocationCount++
			if err := tbuf.NextColumnID(session.BitIndex); err != nil {
				profile.RownumAllocationElapsed += time.Since(allocationStart)
				return finish(0, false), err
			}
			profile.RownumAllocationElapsed += time.Since(allocationStart)
		} else {
			profile.ProvidedColumnIDCount++
			tbuf.CurrentColumnID = req.ProvidedColumnID
		}
		batchCacheWriteStart := time.Now()
		profile.BatchCacheWriteCount++
		session.BatchBuffer.SetPartitionedString(localKey, req.LookupValue, tbuf.CurrentColumnID)
		profile.BatchCacheWriteElapsed += time.Since(batchCacheWriteStart)
		return finish(tbuf.CurrentColumnID, false), nil
	}

	if req.DirectColumnID {
		profile.DirectColumnIDCount++
		return finish(tbuf.CurrentColumnID, false), nil
	}
	if req.ProvidedColumnID == 0 {
		allocationStart := time.Now()
		profile.RownumAllocationCount++
		if err := tbuf.NextColumnID(session.BitIndex); err != nil {
			profile.RownumAllocationElapsed += time.Since(allocationStart)
			return finish(0, false), err
		}
		profile.RownumAllocationElapsed += time.Since(allocationStart)
	} else {
		profile.ProvidedColumnIDCount++
		tbuf.CurrentColumnID = req.ProvidedColumnID
	}
	return finish(tbuf.CurrentColumnID, false), nil
}
