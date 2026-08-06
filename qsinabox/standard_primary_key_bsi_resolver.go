package qsinabox

import (
	"github.com/QuantaStream/quantastream/core"
)

// StandardBSIPrimaryKeyResolver promotes eligible PKs to the native BSI
// authority path while preserving KV behavior for unsupported catalog shapes.
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
		return NewStandardBSIPrimaryKeyResolver(standardPrimaryKeyReaderWithProjectionCache(reader))
	}
}

// NewStandardSessionBSIPrimaryKeyResolverFactory builds a resolver factory for
// loader-owned sessions. The concrete reader uses each opened session's bitmap
// connection, so this works for native gRPC loader sessions without sharing
// in-process server pointers.
func NewStandardSessionBSIPrimaryKeyResolverFactory(tableCache *core.TableCacheStruct) core.SessionPrimaryKeyResolverFactory {
	return func(session *core.Session) core.PrimaryKeyResolver {
		if session == nil {
			return core.KVPrimaryKeyResolver{}
		}
		return NewStandardBSIPrimaryKeyResolver(StandardSingleColumnBSIPrimaryKeyReader{
			TableCache:      tableCache,
			BitIndex:        session.BitIndex,
			ProjectionCache: NewStandardBSIProjectionCache(),
		})
	}
}

// ResolvePrimaryKeyColumnID delegates eligible BSI authority shapes to the
// native resolver and falls back for direct-rownum, unsupported, or not-yet
// encodable keys.
func (r StandardBSIPrimaryKeyResolver) ResolvePrimaryKeyColumnID(req core.PrimaryKeyResolveRequest) (core.PrimaryKeyResolveResult, error) {
	if r.Reader == nil || req.TableBuffer == nil || req.TableBuffer.Table == nil {
		return r.resolveFallback(req, "resolver_context_missing")
	}
	eligibility := core.ObserveBSIPrimaryKeyAuthorityEligibility(req.TableBuffer.Table)
	if eligibility.Eligible && eligibility.Mode == core.BSIPrimaryKeyAuthorityModeSingleColumnBSI {
		backend := core.NewSingleColumnBSIPrimaryKeyBackend(req.TableBuffer.Table, r.Reader)
		return core.NewBSIPrimaryKeyResolver(backend).ResolvePrimaryKeyColumnID(req)
	}
	if eligibility.Mode == core.BSIPrimaryKeyAuthorityModeCompoundEncodedBSI {
		reader, ok := standardCompoundBSIPrimaryKeyReader(r.Reader)
		if !ok {
			return r.resolveFallback(req, "compound_reader_missing")
		}
		if req.Session == nil {
			return r.resolveFallback(req, "compound_session_missing")
		}
		if req.Session.BatchBuffer == nil {
			return r.resolveFallback(req, "compound_batch_buffer_missing")
		}
		if !standardCompoundBSIPrimaryKeyEncodable(req) {
			return r.resolveFallback(req, "compound_not_encodable")
		}
		backend := StandardCompoundBSIPrimaryKeyBackend{
			Table:   req.TableBuffer.Table,
			Reader:  reader,
			Session: req.Session,
		}
		return core.NewBSIPrimaryKeyResolver(backend).ResolvePrimaryKeyColumnID(req)
	}
	if !eligibility.Eligible || eligibility.Mode != core.BSIPrimaryKeyAuthorityModeSingleColumnBSI {
		return r.resolveFallback(req, standardBSIPrimaryKeyFallbackReason(eligibility))
	}
	return r.resolveFallback(req, "unhandled_bsi_authority_shape")
}

func (r StandardBSIPrimaryKeyResolver) fallback() core.PrimaryKeyResolver {
	if r.Fallback != nil {
		return r.Fallback
	}
	return core.KVPrimaryKeyResolver{}
}

func (r StandardBSIPrimaryKeyResolver) resolveFallback(req core.PrimaryKeyResolveRequest, reason string) (core.PrimaryKeyResolveResult, error) {
	result, err := r.fallback().ResolvePrimaryKeyColumnID(req)
	result.Profile.RecordBSIFallback(reason)
	return result, err
}

func standardBSIPrimaryKeyFallbackReason(eligibility core.BSIPrimaryKeyAuthorityEligibility) string {
	switch {
	case eligibility.Mode == core.BSIPrimaryKeyAuthorityModeUnsupported && eligibility.Reason == "primary key is missing":
		return "primary_key_missing"
	case eligibility.Mode == core.BSIPrimaryKeyAuthorityModeUnsupported && eligibility.Reason == "primary key field is missing from catalog":
		return "primary_key_field_missing"
	case eligibility.Mode == core.BSIPrimaryKeyAuthorityModeUnsupported && eligibility.Reason == "primary key field is not BSI-backed":
		return "primary_key_field_not_bsi_backed"
	case eligibility.Mode == core.BSIPrimaryKeyAuthorityModeDirectColumnID:
		return "direct_column_id"
	case eligibility.Mode != "":
		return "unsupported_" + eligibility.Mode
	default:
		return "unsupported_unknown"
	}
}

func standardCompoundBSIPrimaryKeyReader(reader core.SingleColumnBSIPrimaryKeyReader) (StandardSingleColumnBSIPrimaryKeyReader, bool) {
	switch typed := reader.(type) {
	case StandardSingleColumnBSIPrimaryKeyReader:
		return typed, true
	case *StandardSingleColumnBSIPrimaryKeyReader:
		if typed != nil {
			return *typed, true
		}
	}
	return StandardSingleColumnBSIPrimaryKeyReader{}, false
}

func standardPrimaryKeyReaderWithProjectionCache(reader core.SingleColumnBSIPrimaryKeyReader) core.SingleColumnBSIPrimaryKeyReader {
	switch typed := reader.(type) {
	case StandardSingleColumnBSIPrimaryKeyReader:
		if typed.ProjectionCache == nil {
			typed.ProjectionCache = NewStandardBSIProjectionCache()
		}
		return typed
	case *StandardSingleColumnBSIPrimaryKeyReader:
		if typed == nil {
			return reader
		}
		copy := *typed
		if copy.ProjectionCache == nil {
			copy.ProjectionCache = NewStandardBSIProjectionCache()
		}
		return copy
	default:
		return reader
	}
}

func standardCompoundBSIPrimaryKeyEncodable(req core.PrimaryKeyResolveRequest) bool {
	if req.TableBuffer == nil || req.TableBuffer.Table == nil {
		return false
	}
	_, err := core.EncodeCompoundPrimaryKeyAuthorityValue(core.PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  req.TableBuffer.Table.Name,
		PrimaryKey: req.TableBuffer.Table.PrimaryKey,
		Attributes: append([]*core.Attribute(nil), req.TableBuffer.PKAttributes...),
		Values:     append([]interface{}(nil), req.PrimaryKeyValues...),
	})
	return err == nil
}
