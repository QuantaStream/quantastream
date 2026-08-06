package core

import (
	"strings"
	"testing"
)

func TestBSIPrimaryKeyAuthorityArtifactLoadRequestValidate(t *testing.T) {
	request := BSIPrimaryKeyAuthorityArtifactLoadRequest{
		Entry: BSIPrimaryKeyAuthorityManifestEntry{
			TableName:      "lineitem",
			AuthorityMode:  BSIPrimaryKeyAuthorityModeCompoundEncodedBSI,
			AuthorityField: "__qs_pk_authority",
		},
		Artifact: BSIPrimaryKeyAuthorityManifestArtifact{
			Kind: BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI,
			Path: "bitmap/lineitem/__qs_pk_authority",
		},
		PhysicalPath: "/tmp/authority",
	}

	if err := request.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestBSIPrimaryKeyAuthorityArtifactLoadRequestValidateRejectsUnsupportedKind(t *testing.T) {
	request := BSIPrimaryKeyAuthorityArtifactLoadRequest{
		Entry: BSIPrimaryKeyAuthorityManifestEntry{
			TableName:      "lineitem",
			AuthorityMode:  BSIPrimaryKeyAuthorityModeCompoundEncodedBSI,
			AuthorityField: "__qs_pk_authority",
		},
		Artifact: BSIPrimaryKeyAuthorityManifestArtifact{
			Kind: "kv",
			Path: "bitmap/lineitem/__qs_pk_authority",
		},
		PhysicalPath: "/tmp/authority",
	}

	err := request.Validate()
	if err == nil {
		t.Fatalf("Validate returned nil error, want unsupported kind rejection")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("Validate error = %v, want unsupported kind detail", err)
	}
}
