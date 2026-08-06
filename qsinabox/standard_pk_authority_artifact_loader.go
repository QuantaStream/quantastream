package qsinabox

import (
	"fmt"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
)

// StandardBSIPrimaryKeyAuthorityArtifactLoader is the standard-mode prototype
// loader for manifest-described authority BSI artifact roots. It validates and
// resolves the physical artifact boundary without decoding the BSI files into a
// replacement in-memory structure yet.
type StandardBSIPrimaryKeyAuthorityArtifactLoader struct {
	Config StandardConfig
}

// LoadBSIPrimaryKeyAuthorityArtifact validates that the manifest artifact root
// exists under the standard-mode data directory and returns metadata about the
// physical files that would be loaded by the durable authority path.
func (l StandardBSIPrimaryKeyAuthorityArtifactLoader) LoadBSIPrimaryKeyAuthorityArtifact(request core.BSIPrimaryKeyAuthorityArtifactLoadRequest) (core.BSIPrimaryKeyAuthorityArtifactLoadResult, error) {
	if strings.TrimSpace(request.PhysicalPath) == "" {
		physicalPath, err := standardBSIPrimaryKeyAuthorityArtifactPhysicalPath(l.Config, request.Artifact.Path)
		if err != nil {
			return core.BSIPrimaryKeyAuthorityArtifactLoadResult{}, err
		}
		request.PhysicalPath = physicalPath
	}
	if err := request.Validate(); err != nil {
		return core.BSIPrimaryKeyAuthorityArtifactLoadResult{}, err
	}
	fileCount, exists, err := standardBSIPrimaryKeyAuthorityArtifactFileCount(l.Config, request.Artifact.Path)
	if err != nil {
		return core.BSIPrimaryKeyAuthorityArtifactLoadResult{}, err
	}
	if !exists {
		return core.BSIPrimaryKeyAuthorityArtifactLoadResult{}, fmt.Errorf("primary-key authority artifact path missing: %s", request.Artifact.Path)
	}
	keyCount := request.Artifact.KeyCount
	if keyCount == 0 {
		keyCount = request.Entry.KeyCount
	}
	return core.BSIPrimaryKeyAuthorityArtifactLoadResult{
		TableName:      request.Entry.TableName,
		AuthorityMode:  request.Entry.AuthorityMode,
		AuthorityField: request.Entry.AuthorityField,
		ArtifactPath:   request.Artifact.Path,
		PhysicalPath:   request.PhysicalPath,
		FileCount:      fileCount,
		KeyCount:       keyCount,
		LoadedAt:       time.Now().UTC(),
	}, nil
}
