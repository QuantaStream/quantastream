package core

import (
	"fmt"
)

// PrimaryKeyDomainState describes whether a primary-key authority domain can
// contain committed keys.
type PrimaryKeyDomainState string

const (
	// PrimaryKeyDomainUnknown is the fail-safe state when authority metadata is
	// missing, stale, dirty, partial, unsupported, or otherwise untrusted.
	PrimaryKeyDomainUnknown PrimaryKeyDomainState = "unknown"
	// PrimaryKeyDomainEmpty means the exact table/shard authority domain is
	// known to contain no committed primary-key values.
	PrimaryKeyDomainEmpty PrimaryKeyDomainState = "empty"
	// PrimaryKeyDomainNonEmpty means the authority domain may contain committed
	// primary-key values and must be queried for idempotent writes.
	PrimaryKeyDomainNonEmpty PrimaryKeyDomainState = "non_empty"
)

// BSIPrimaryKeyDomainStateBackend is the optional backend capability that
// allows the resolver to skip durable lookup for proven-empty authority domains.
type BSIPrimaryKeyDomainStateBackend interface {
	PrimaryKeyDomainState(BSIPrimaryKeyLookupRequest) (PrimaryKeyDomainState, error)
}

func bsiPrimaryKeyDomainKey(req BSIPrimaryKeyLookupRequest, tbuf *TableBuffer) string {
	shardNanos := int64(0)
	if tbuf != nil && tbuf.Table != nil && tbuf.Table.TimeQuantumType != "" && !req.ShardTimestamp.IsZero() {
		shardNanos = req.ShardTimestamp.UTC().UnixNano()
	}
	return fmt.Sprintf("%s|%s|%d", req.TableName, req.PrimaryKey, shardNanos)
}

func (s *Session) bsiPrimaryKeyDomainSkipAllowed(domainKey string) bool {
	state, ok := s.cachedBSIPrimaryKeyDomainState(domainKey)
	return ok && state == PrimaryKeyDomainEmpty
}

func (s *Session) markBSIPrimaryKeyDomainSkipAllowed(domainKey string) {
	s.markBSIPrimaryKeyDomainState(domainKey, PrimaryKeyDomainEmpty)
}

func (s *Session) cachedBSIPrimaryKeyDomainState(domainKey string) (PrimaryKeyDomainState, bool) {
	if s == nil || domainKey == "" {
		return PrimaryKeyDomainUnknown, false
	}
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	state, ok := s.primaryKeyDomainStates[domainKey]
	return normalizePrimaryKeyDomainState(state), ok
}

func (s *Session) markBSIPrimaryKeyDomainState(domainKey string, state PrimaryKeyDomainState) {
	if s == nil || domainKey == "" {
		return
	}
	state = normalizePrimaryKeyDomainState(state)
	if state == PrimaryKeyDomainUnknown {
		return
	}
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if s.primaryKeyDomainStates == nil {
		s.primaryKeyDomainStates = map[string]PrimaryKeyDomainState{}
	}
	s.primaryKeyDomainStates[domainKey] = state
}

func normalizePrimaryKeyDomainState(state PrimaryKeyDomainState) PrimaryKeyDomainState {
	switch state {
	case PrimaryKeyDomainEmpty, PrimaryKeyDomainNonEmpty:
		return state
	default:
		return PrimaryKeyDomainUnknown
	}
}
