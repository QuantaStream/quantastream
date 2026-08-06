package qsinabox

import (
	"fmt"

	"github.com/QuantaStream/quantastream/core"
)

type standardBSIPrimaryKeyAuthorityPolicy struct {
	Observation    core.BSIPrimaryKeyAuthorityManifestObservation
	Warning        string
	BlockMutations bool
}

func observeStandardBSIPrimaryKeyAuthorityPolicy(config StandardConfig) standardBSIPrimaryKeyAuthorityPolicy {
	return standardBSIPrimaryKeyAuthorityPolicyForObservation(ObserveStandardBSIPrimaryKeyAuthorityManifest(config))
}

func standardBSIPrimaryKeyAuthorityPolicyForObservation(observation core.BSIPrimaryKeyAuthorityManifestObservation) standardBSIPrimaryKeyAuthorityPolicy {
	policy := standardBSIPrimaryKeyAuthorityPolicy{
		Observation: observation,
	}
	switch observation.Status {
	case core.BSIPrimaryKeyAuthorityManifestStatusMissing:
		policy.Warning = "BSI primary-key authority manifest is missing; writes use native BSI authority without persisted startup validation"
	case core.BSIPrimaryKeyAuthorityManifestStatusStale, core.BSIPrimaryKeyAuthorityManifestStatusInvalid:
		policy.BlockMutations = true
		detail := observation.Detail
		if detail == "" {
			detail = "no detail"
		}
		policy.Warning = fmt.Sprintf("BSI primary-key authority manifest is %s; mutations fail closed until the manifest is repaired: %s", observation.Status, detail)
	}
	return policy
}
