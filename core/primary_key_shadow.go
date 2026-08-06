package core

import "fmt"

const (
	// PrimaryKeyShadowMatchReason means the shadow resolver matched the authority result.
	PrimaryKeyShadowMatchReason = "match"
	// PrimaryKeyShadowAuthorityErrorReason means the authority resolver failed before shadow evaluation.
	PrimaryKeyShadowAuthorityErrorReason = "authority_error"
	// PrimaryKeyShadowShadowErrorReason means the shadow resolver returned an error.
	PrimaryKeyShadowShadowErrorReason = "shadow_error"
	// PrimaryKeyShadowColumnIDReason means authority and shadow selected different rownums.
	PrimaryKeyShadowColumnIDReason = "column_id_mismatch"
	// PrimaryKeyShadowExistingRowReason means authority and shadow disagreed on existing-row state.
	PrimaryKeyShadowExistingRowReason = "existing_row_mismatch"
	// PrimaryKeyShadowNoAuthorityColumnIDReason means authority succeeded without a rownum to validate.
	PrimaryKeyShadowNoAuthorityColumnIDReason = "no_authority_column_id"
)

// PrimaryKeyShadowComparison captures the non-authoritative result of comparing
// a shadow primary-key resolver against the production resolver.
type PrimaryKeyShadowComparison struct {
	TableName       string
	PrimaryKey      string
	LookupValue     string
	AuthorityResult PrimaryKeyResolveResult
	ShadowResult    PrimaryKeyResolveResult
	AuthorityError  string
	ShadowError     string
	Match           bool
	Reason          string
}

// PrimaryKeyShadowObserver receives shadow comparison events.
type PrimaryKeyShadowObserver func(PrimaryKeyShadowComparison)

// ShadowPrimaryKeyResolver lets a new resolver run beside the authoritative
// resolver while the authoritative result continues to drive writes.
type ShadowPrimaryKeyResolver struct {
	Authority PrimaryKeyResolver
	Shadow    PrimaryKeyResolver
	Observe   PrimaryKeyShadowObserver
}

// NewShadowPrimaryKeyResolver returns a primary-key resolver that compares a
// shadow resolver with an authoritative resolver without changing write
// semantics.
func NewShadowPrimaryKeyResolver(
	authority PrimaryKeyResolver,
	shadow PrimaryKeyResolver,
	observe PrimaryKeyShadowObserver,
) ShadowPrimaryKeyResolver {
	return ShadowPrimaryKeyResolver{
		Authority: authority,
		Shadow:    shadow,
		Observe:   observe,
	}
}

// ResolvePrimaryKeyColumnID returns the authoritative resolver result and, when
// possible, evaluates the shadow resolver on a cloned table buffer.
func (r ShadowPrimaryKeyResolver) ResolvePrimaryKeyColumnID(
	req PrimaryKeyResolveRequest,
) (PrimaryKeyResolveResult, error) {
	authority := r.Authority
	if authority == nil {
		err := fmt.Errorf("primary key shadow resolver requires explicit authority resolver")
		comparison := PrimaryKeyShadowComparison{
			TableName:      primaryKeyShadowTableName(req),
			PrimaryKey:     primaryKeyShadowPrimaryKey(req),
			LookupValue:    req.LookupValue,
			AuthorityError: err.Error(),
			Reason:         PrimaryKeyShadowAuthorityErrorReason,
		}
		r.observePrimaryKeyShadow(comparison)
		return PrimaryKeyResolveResult{}, err
	}

	authorityResult, authorityErr := authority.ResolvePrimaryKeyColumnID(req)
	comparison := PrimaryKeyShadowComparison{
		TableName:       primaryKeyShadowTableName(req),
		PrimaryKey:      primaryKeyShadowPrimaryKey(req),
		LookupValue:     req.LookupValue,
		AuthorityResult: authorityResult,
		AuthorityError:  primaryKeyShadowErrorString(authorityErr),
	}
	if authorityErr != nil {
		comparison.Reason = PrimaryKeyShadowAuthorityErrorReason
		r.observePrimaryKeyShadow(comparison)
		return authorityResult, authorityErr
	}
	if r.Shadow == nil {
		return authorityResult, nil
	}
	if authorityResult.ColumnID == 0 {
		comparison.Reason = PrimaryKeyShadowNoAuthorityColumnIDReason
		r.observePrimaryKeyShadow(comparison)
		return authorityResult, nil
	}

	shadowReq := clonePrimaryKeyShadowRequest(req)
	shadowReq.ProvidedColumnID = authorityResult.ColumnID
	shadowResult, shadowErr := r.Shadow.ResolvePrimaryKeyColumnID(shadowReq)
	comparison.ShadowResult = shadowResult
	comparison.ShadowError = primaryKeyShadowErrorString(shadowErr)
	comparison.Match, comparison.Reason = comparePrimaryKeyShadow(authorityResult, shadowResult, shadowErr)
	r.observePrimaryKeyShadow(comparison)
	return authorityResult, nil
}

func (r ShadowPrimaryKeyResolver) observePrimaryKeyShadow(comparison PrimaryKeyShadowComparison) {
	if r.Observe != nil {
		r.Observe(comparison)
	}
}

func clonePrimaryKeyShadowRequest(req PrimaryKeyResolveRequest) PrimaryKeyResolveRequest {
	clone := req
	clone.PrimaryKeyValues = append([]interface{}(nil), req.PrimaryKeyValues...)
	clone.TableBuffer = clonePrimaryKeyShadowTableBuffer(req.TableBuffer)
	return clone
}

func clonePrimaryKeyShadowTableBuffer(tbuf *TableBuffer) *TableBuffer {
	if tbuf == nil {
		return nil
	}
	clone := *tbuf
	clone.CurrentPKValue = append([]interface{}(nil), tbuf.CurrentPKValue...)
	clone.PKAttributes = append([]*Attribute(nil), tbuf.PKAttributes...)
	clone.PKMap = make(map[string]*Attribute, len(tbuf.PKMap))
	for key, value := range tbuf.PKMap {
		clone.PKMap[key] = value
	}
	clone.SKMap = make(map[string][]*Attribute, len(tbuf.SKMap))
	for key, value := range tbuf.SKMap {
		clone.SKMap[key] = append([]*Attribute(nil), value...)
	}
	clone.rowCache = make(map[string]interface{}, len(tbuf.rowCache))
	for key, value := range tbuf.rowCache {
		clone.rowCache[key] = value
	}
	clone.sequencerCache = nil
	return &clone
}

func comparePrimaryKeyShadow(
	authorityResult PrimaryKeyResolveResult,
	shadowResult PrimaryKeyResolveResult,
	shadowErr error,
) (bool, string) {
	if shadowErr != nil {
		return false, PrimaryKeyShadowShadowErrorReason
	}
	if authorityResult.ColumnID != shadowResult.ColumnID {
		return false, PrimaryKeyShadowColumnIDReason
	}
	if authorityResult.ExistingRow != shadowResult.ExistingRow {
		return false, PrimaryKeyShadowExistingRowReason
	}
	return true, PrimaryKeyShadowMatchReason
}

func primaryKeyShadowErrorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func primaryKeyShadowTableName(req PrimaryKeyResolveRequest) string {
	if req.TableBuffer == nil || req.TableBuffer.Table == nil {
		return ""
	}
	return req.TableBuffer.Table.Name
}

func primaryKeyShadowPrimaryKey(req PrimaryKeyResolveRequest) string {
	if req.TableBuffer == nil || req.TableBuffer.Table == nil {
		return ""
	}
	return req.TableBuffer.Table.PrimaryKey
}

func (c PrimaryKeyShadowComparison) String() string {
	if c.Match {
		return fmt.Sprintf("primary-key shadow match table=%s primary_key=%s value=%s column_id=%d",
			c.TableName, c.PrimaryKey, c.LookupValue, c.AuthorityResult.ColumnID)
	}
	return fmt.Sprintf("primary-key shadow mismatch table=%s primary_key=%s value=%s reason=%s authority_column_id=%d shadow_column_id=%d authority_error=%q shadow_error=%q",
		c.TableName, c.PrimaryKey, c.LookupValue, c.Reason, c.AuthorityResult.ColumnID, c.ShadowResult.ColumnID,
		c.AuthorityError, c.ShadowError)
}
