package qsinabox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/core"
)

func TestStandardBSIPrimaryKeyAuthorityArtifactLoaderLoadsPresentArtifactRoot(t *testing.T) {
	dataDir := t.TempDir()
	artifactDir := filepath.Join(dataDir, "bitmap", "sample", "id", "bsi", "default")
	if err := os.MkdirAll(artifactDir, 0755); err != nil {
		t.Fatalf("mkdir artifact dir: %v", err)
	}
	for _, name := range []string{"EBM", "1"} {
		if err := os.WriteFile(filepath.Join(artifactDir, name), []byte("unit-test"), 0644); err != nil {
			t.Fatalf("write artifact file %s: %v", name, err)
		}
	}

	loader := StandardBSIPrimaryKeyAuthorityArtifactLoader{
		Config: StandardConfig{DataDir: dataDir},
	}
	result, err := loader.LoadBSIPrimaryKeyAuthorityArtifact(core.BSIPrimaryKeyAuthorityArtifactLoadRequest{
		Entry: core.BSIPrimaryKeyAuthorityManifestEntry{
			TableName:      "sample",
			AuthorityMode:  core.BSIPrimaryKeyAuthorityModeSingleColumnBSI,
			AuthorityField: "id",
			KeyCount:       11,
		},
		Artifact: core.BSIPrimaryKeyAuthorityManifestArtifact{
			Kind:     core.BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI,
			Path:     "bitmap/sample/id/bsi",
			KeyCount: 11,
		},
	})
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityArtifact returned error: %v", err)
	}
	if result.TableName != "sample" || result.AuthorityField != "id" || result.ArtifactPath != "bitmap/sample/id/bsi" {
		t.Fatalf("result identity = %+v", result)
	}
	if result.FileCount != 2 {
		t.Fatalf("file count = %d, want 2", result.FileCount)
	}
	if result.KeyCount != 11 {
		t.Fatalf("key count = %d, want 11", result.KeyCount)
	}
	if result.PhysicalPath == "" || !strings.HasPrefix(result.PhysicalPath, filepath.Clean(dataDir)) {
		t.Fatalf("physical path = %q, want under data dir %q", result.PhysicalPath, dataDir)
	}
	if result.LoadedAt.IsZero() {
		t.Fatalf("LoadedAt is zero")
	}
}

func TestStandardBSIPrimaryKeyAuthorityArtifactLoaderRejectsMissingArtifactRoot(t *testing.T) {
	loader := StandardBSIPrimaryKeyAuthorityArtifactLoader{
		Config: StandardConfig{DataDir: t.TempDir()},
	}

	_, err := loader.LoadBSIPrimaryKeyAuthorityArtifact(core.BSIPrimaryKeyAuthorityArtifactLoadRequest{
		Entry: core.BSIPrimaryKeyAuthorityManifestEntry{
			TableName:      "sample",
			AuthorityMode:  core.BSIPrimaryKeyAuthorityModeSingleColumnBSI,
			AuthorityField: "id",
		},
		Artifact: core.BSIPrimaryKeyAuthorityManifestArtifact{
			Kind: core.BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI,
			Path: "bitmap/sample/id/bsi",
		},
	})
	if err == nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityArtifact returned nil error, want missing artifact rejection")
	}
	if !strings.Contains(err.Error(), "artifact path missing") {
		t.Fatalf("error = %v, want missing artifact detail", err)
	}
}
