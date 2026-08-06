package qsinabox

import (
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/shared"
)

// StandardBSIPrimaryKeyAuthorityManifestPublisher publishes standard-mode
// primary-key authority metadata after a successful state-changing flush.
type StandardBSIPrimaryKeyAuthorityManifestPublisher interface {
	PublishAfterFlush(profile shared.BatchBufferFlushProfile) (bool, error)
}

// StandardBSIPrimaryKeyAuthorityManifestFilePublisher writes the logical
// standard-mode authority manifest to the local data directory. It does not
// write or validate physical authority BSI artifacts.
type StandardBSIPrimaryKeyAuthorityManifestFilePublisher struct {
	Config StandardConfig
	Source string
}

// PublishAfterFlush writes the logical manifest only when the release observed
// a successful non-empty BatchBuffer flush.
func (p StandardBSIPrimaryKeyAuthorityManifestFilePublisher) PublishAfterFlush(profile shared.BatchBufferFlushProfile) (bool, error) {
	if strings.TrimSpace(profile.Error) != "" {
		return false, fmt.Errorf("cannot publish BSI primary-key authority manifest after failed flush: %s", profile.Error)
	}
	if !standardBSIPrimaryKeyAuthorityFlushMutated(profile) {
		return false, nil
	}
	source := strings.TrimSpace(p.Source)
	if source == "" {
		source = "standard-session-flush"
	}
	manifest, err := BuildStandardBSIPrimaryKeyAuthorityManifest(p.Config, source)
	if err != nil {
		return false, err
	}
	if err := standardPopulateBSIPrimaryKeyAuthorityArtifactFileCounts(p.Config, &manifest); err != nil {
		return false, err
	}
	if err := SaveStandardBSIPrimaryKeyAuthorityManifest(p.Config, manifest); err != nil {
		return false, err
	}
	return true, nil
}

func standardBSIPrimaryKeyAuthorityFlushMutated(profile shared.BatchBufferFlushProfile) bool {
	if profile.StartedAt.IsZero() {
		return false
	}
	return profile.PartitionStringEntryCount > 0 ||
		profile.BitmapSetEntryCount > 0 ||
		profile.BitmapClearEntryCount > 0 ||
		profile.BSIValueEntryCount > 0 ||
		profile.BSIClearValueEntryCount > 0
}
