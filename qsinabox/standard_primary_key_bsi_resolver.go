package qsinabox

import (
	"github.com/QuantaStream/quantastream/core"
)

// StandardBSIPrimaryKeyResolver promotes eligible single-column PKs to the
// native BSI authority path while preserving KV behavior for unsupported
// catalog shapes.
type StandardBSIPrimaryKeyResolver struct {
	Reader   core.SingleColumnBSIPrimaryKeyReader
	Fallback core.PrimaryKeyResolver
}

var _ core.PrimaryKeyResolver = StandardBSIPrimaryKeyResolver{}

// NewStandardBSIPrimaryKeyResolver returns the standard-mode resolver wrapper.
func NewStandardBSIPrimaryKeyResolver(reader core.SingleColumnBSIPrimaryKeyReader) StandardBSIPrimaryKeyResolver {
	return StandardBSIPrimaryKeyResolver{
		Reader:   reader,
		Fallback: core.KVPrimaryKeyResolver{},
	}
}

// NewStandardBSIPrimaryKeyResolverFactory builds a session resolver factory for
// router or session-provider owners that have opted into BSI PK authority.
func NewStandardBSIPrimaryKeyResolverFactory(reader core.SingleColumnBSIPrimaryKeyReader) core.SessionPrimaryKeyResolverFactory {
	return func(*core.Session) core.PrimaryKeyResolver {
		return NewStandardBSIPrimaryKeyResolver(reader)
	}
}

// ResolvePrimaryKeyColumnID delegates eligible single-column BSI tables to the
// native resolver and falls back for compound, direct-rownum, or non-BSI keys.
func (r StandardBSIPrimaryKeyResolver) ResolvePrimaryKeyColumnID(req core.PrimaryKeyResolveRequest) (core.PrimaryKeyResolveResult, error) {
	if r.Reader == nil || req.TableBuffer == nil || req.TableBuffer.Table == nil {
		return r.fallback().ResolvePrimaryKeyColumnID(req)
	}
	eligibility := core.ObserveBSIPrimaryKeyAuthorityEligibility(req.TableBuffer.Table)
	if !eligibility.Eligible || eligibility.Mode != core.BSIPrimaryKeyAuthorityModeSingleColumnBSI {
		return r.fallback().ResolvePrimaryKeyColumnID(req)
	}
	backend := core.NewSingleColumnBSIPrimaryKeyBackend(req.TableBuffer.Table, r.Reader)
	return core.NewBSIPrimaryKeyResolver(backend).ResolvePrimaryKeyColumnID(req)
}

func (r StandardBSIPrimaryKeyResolver) fallback() core.PrimaryKeyResolver {
	if r.Fallback != nil {
		return r.Fallback
	}
	return core.KVPrimaryKeyResolver{}
}
