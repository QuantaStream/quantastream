package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShadowPrimaryKeyResolverReturnsAuthorityResultAndObservesMatch(t *testing.T) {
	tbuf, _ := newBSIPrimaryKeyTestBuffer()
	authority := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{ColumnID: 11, ExistingRow: false},
	}
	shadow := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{ColumnID: 11, ExistingRow: false},
	}
	var comparison PrimaryKeyShadowComparison
	resolver := NewShadowPrimaryKeyResolver(authority, shadow, func(observed PrimaryKeyShadowComparison) {
		comparison = observed
	})

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001",
		PrimaryKeyValues: []interface{}{int64(1001)},
	})

	require.NoError(t, err)
	require.Equal(t, uint64(11), result.ColumnID)
	require.Equal(t, uint64(11), tbuf.CurrentColumnID)
	require.True(t, shadow.called)
	require.Equal(t, uint64(11), shadow.request.ProvidedColumnID)
	require.NotSame(t, tbuf, shadow.request.TableBuffer)
	require.True(t, comparison.Match)
	require.Equal(t, PrimaryKeyShadowMatchReason, comparison.Reason)
	require.Equal(t, "orders", comparison.TableName)
	require.Equal(t, "o_orderkey", comparison.PrimaryKey)
	require.Equal(t, "1001", comparison.LookupValue)
}

func TestShadowPrimaryKeyResolverObservesMismatchWithoutChangingAuthorityResult(t *testing.T) {
	tbuf, _ := newBSIPrimaryKeyTestBuffer()
	authority := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{ColumnID: 11, ExistingRow: false},
	}
	shadow := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{ColumnID: 12, ExistingRow: false},
	}
	var comparison PrimaryKeyShadowComparison
	resolver := NewShadowPrimaryKeyResolver(authority, shadow, func(observed PrimaryKeyShadowComparison) {
		comparison = observed
	})

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001",
		PrimaryKeyValues: []interface{}{int64(1001)},
	})

	require.NoError(t, err)
	require.Equal(t, uint64(11), result.ColumnID)
	require.Equal(t, uint64(11), tbuf.CurrentColumnID)
	require.False(t, comparison.Match)
	require.Equal(t, PrimaryKeyShadowColumnIDReason, comparison.Reason)
	require.Equal(t, uint64(12), comparison.ShadowResult.ColumnID)
}

func TestShadowPrimaryKeyResolverStagesShadowMissWithAuthorityColumnID(t *testing.T) {
	tbuf, _ := newBSIPrimaryKeyTestBuffer()
	authority := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{ColumnID: 55, ExistingRow: false},
	}
	backend := &recordingBSIPrimaryKeyBackend{}
	shadow := NewBSIPrimaryKeyResolver(backend)
	var comparison PrimaryKeyShadowComparison
	resolver := NewShadowPrimaryKeyResolver(authority, shadow, func(observed PrimaryKeyShadowComparison) {
		comparison = observed
	})

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001",
		PrimaryKeyValues: []interface{}{int64(1001)},
	})

	require.NoError(t, err)
	require.Equal(t, uint64(55), result.ColumnID)
	require.Len(t, backend.lookupRequests, 1)
	require.Len(t, backend.stageRequests, 1)
	require.NotEmpty(t, backend.lookupRequests[0].Identity)
	require.NotEmpty(t, backend.stageRequests[0].Identity)
	require.Equal(t, backend.lookupRequests[0].Identity, backend.stageRequests[0].Identity)
	require.Equal(t, uint64(55), backend.stageRequests[0].ColumnID)
	require.True(t, comparison.Match)
	require.Equal(t, PrimaryKeyShadowMatchReason, comparison.Reason)
}

func TestShadowPrimaryKeyResolverSkipsShadowWhenAuthorityErrors(t *testing.T) {
	tbuf, _ := newBSIPrimaryKeyTestBuffer()
	authorityErr := errors.New("authority failed")
	authority := &recordingPrimaryKeyResolver{err: authorityErr}
	shadow := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{ColumnID: 12, ExistingRow: false},
	}
	var comparison PrimaryKeyShadowComparison
	resolver := NewShadowPrimaryKeyResolver(authority, shadow, func(observed PrimaryKeyShadowComparison) {
		comparison = observed
	})

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001",
		PrimaryKeyValues: []interface{}{int64(1001)},
	})

	require.ErrorIs(t, err, authorityErr)
	require.Zero(t, result.ColumnID)
	require.False(t, shadow.called)
	require.Equal(t, PrimaryKeyShadowAuthorityErrorReason, comparison.Reason)
	require.Equal(t, "authority failed", comparison.AuthorityError)
}

func TestShadowPrimaryKeyResolverSkipsShadowWithoutAuthorityColumnID(t *testing.T) {
	tbuf, _ := newBSIPrimaryKeyTestBuffer()
	authority := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{ColumnID: 0, ExistingRow: false},
	}
	shadow := &recordingPrimaryKeyResolver{
		result: PrimaryKeyResolveResult{ColumnID: 12, ExistingRow: false},
	}
	var comparison PrimaryKeyShadowComparison
	resolver := NewShadowPrimaryKeyResolver(authority, shadow, func(observed PrimaryKeyShadowComparison) {
		comparison = observed
	})

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001",
		PrimaryKeyValues: []interface{}{int64(1001)},
	})

	require.NoError(t, err)
	require.Zero(t, result.ColumnID)
	require.False(t, shadow.called)
	require.Equal(t, PrimaryKeyShadowNoAuthorityColumnIDReason, comparison.Reason)
}
