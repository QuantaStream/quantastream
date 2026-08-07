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
	if s == nil || domainKey == "" {
		return false
	}
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.primaryKeyEmptyDomainSkips[domainKey]
}

func (s *Session) markBSIPrimaryKeyDomainSkipAllowed(domainKey string) {
	if s == nil || domainKey == "" {
		return
	}
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if s.primaryKeyEmptyDomainSkips == nil {
		s.primaryKeyEmptyDomainSkips = map[string]bool{}
	}
	s.primaryKeyEmptyDomainSkips[domainKey] = true
}

func normalizePrimaryKeyDomainState(state PrimaryKeyDomainState) PrimaryKeyDomainState {
	switch state {
	case PrimaryKeyDomainEmpty, PrimaryKeyDomainNonEmpty:
		return state
	default:
		return PrimaryKeyDomainUnknown
	}
}
