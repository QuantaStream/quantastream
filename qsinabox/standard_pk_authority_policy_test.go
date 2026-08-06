package qsinabox

import (
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/core"
)

func TestStandardBSIPrimaryKeyAuthorityPolicyAllowsMissingManifest(t *testing.T) {
	policy := standardBSIPrimaryKeyAuthorityPolicyForObservation(core.BSIPrimaryKeyAuthorityManifestObservation{
		Status: core.BSIPrimaryKeyAuthorityManifestStatusMissing,
	})

	if policy.BlockMutations {
		t.Fatalf("BlockMutations = true, want missing manifest to allow BSI writes with a warning")
	}
	if !strings.Contains(policy.Warning, "manifest is missing") {
		t.Fatalf("Warning = %q, want missing manifest warning", policy.Warning)
	}
}

func TestStandardBSIPrimaryKeyAuthorityPolicyBlocksUntrustedManifest(t *testing.T) {
	for _, status := range []string{
		core.BSIPrimaryKeyAuthorityManifestStatusInvalid,
		core.BSIPrimaryKeyAuthorityManifestStatusStale,
	} {
		t.Run(status, func(t *testing.T) {
			policy := standardBSIPrimaryKeyAuthorityPolicyForObservation(core.BSIPrimaryKeyAuthorityManifestObservation{
				Status: status,
				Detail: "unit-test",
			})

			if !policy.BlockMutations {
				t.Fatalf("BlockMutations = false, want %s manifest to fail closed", status)
			}
			if !strings.Contains(policy.Warning, "mutations fail closed") || !strings.Contains(policy.Warning, "unit-test") {
				t.Fatalf("Warning = %q, want fail-closed warning with detail", policy.Warning)
			}
		})
	}
}

func TestStandardBSIPrimaryKeyAuthorityPolicyTrustsOKManifest(t *testing.T) {
	policy := standardBSIPrimaryKeyAuthorityPolicyForObservation(core.BSIPrimaryKeyAuthorityManifestObservation{
		Status: core.BSIPrimaryKeyAuthorityManifestStatusOK,
	})

	if policy.BlockMutations {
		t.Fatalf("BlockMutations = true, want OK manifest to allow writes")
	}
	if policy.Warning != "" {
		t.Fatalf("Warning = %q, want no warning for OK manifest", policy.Warning)
	}
}
