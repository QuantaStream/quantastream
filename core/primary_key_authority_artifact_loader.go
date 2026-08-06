package core

import (
	"fmt"
	"strings"
	"time"
)

// BSIPrimaryKeyAuthorityArtifactLoader loads a physical primary-key authority
// artifact after manifest/catalog validation has selected it as a candidate.
type BSIPrimaryKeyAuthorityArtifactLoader interface {
	LoadBSIPrimaryKeyAuthorityArtifact(BSIPrimaryKeyAuthorityArtifactLoadRequest) (BSIPrimaryKeyAuthorityArtifactLoadResult, error)
}

// BSIPrimaryKeyAuthorityArtifactLoadRequest describes the physical artifact a
// startup/recovery path wants to load.
type BSIPrimaryKeyAuthorityArtifactLoadRequest struct {
	Entry        BSIPrimaryKeyAuthorityManifestEntry
	Artifact     BSIPrimaryKeyAuthorityManifestArtifact
	PhysicalPath string
}

// Validate checks the minimum contract required before a loader attempts to
// open a physical authority artifact.
func (r BSIPrimaryKeyAuthorityArtifactLoadRequest) Validate() error {
	if strings.TrimSpace(r.Entry.TableName) == "" {
		return fmt.Errorf("primary-key authority artifact load requires table")
	}
	if strings.TrimSpace(r.Entry.AuthorityMode) == "" {
		return fmt.Errorf("primary-key authority artifact load requires authority mode")
	}
	if strings.TrimSpace(r.Entry.AuthorityField) == "" {
		return fmt.Errorf("primary-key authority artifact load requires authority field")
	}
	if strings.TrimSpace(r.Artifact.Path) == "" {
		return fmt.Errorf("primary-key authority artifact load requires manifest artifact path")
	}
	if !isSupportedBSIPrimaryKeyAuthorityArtifactKind(r.Artifact.Kind) {
		return fmt.Errorf("unsupported primary-key authority artifact kind %q", r.Artifact.Kind)
	}
	if strings.TrimSpace(r.PhysicalPath) == "" {
		return fmt.Errorf("primary-key authority artifact load requires physical path")
	}
	return nil
}

// BSIPrimaryKeyAuthorityArtifactLoadResult describes a successfully loaded
// physical authority artifact. It intentionally does not define the concrete
// in-memory index shape yet.
type BSIPrimaryKeyAuthorityArtifactLoadResult struct {
	TableName      string
	AuthorityMode  string
	AuthorityField string
	ArtifactPath   string
	PhysicalPath   string
	FileCount      uint64
	KeyCount       uint64
	LoadedAt       time.Time
}
