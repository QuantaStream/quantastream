package qsinabox

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/QuantaStream/quantastream/core"
)

const primaryKeyShadowBSIMode = "bsi"

type primaryKeyShadowBenchmarkSnapshot struct {
	ComparisonCount      int
	MatchCount           int
	MismatchCount        int
	SkipCount            int
	AuthorityErrorCount  int
	ShadowErrorCount     int
	AuthorityExistingRow int
	ShadowExistingRow    int
	ExistingRowMatch     int
	FirstIssue           string
}

type primaryKeyShadowBenchmarkStats struct {
	mu       sync.Mutex
	snapshot primaryKeyShadowBenchmarkSnapshot
}

func (s *primaryKeyShadowBenchmarkStats) Observe(comparison core.PrimaryKeyShadowComparison) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.ComparisonCount++
	if comparison.AuthorityResult.ExistingRow {
		s.snapshot.AuthorityExistingRow++
	}
	if comparison.ShadowResult.ExistingRow {
		s.snapshot.ShadowExistingRow++
	}
	if comparison.Match && comparison.AuthorityResult.ExistingRow && comparison.ShadowResult.ExistingRow {
		s.snapshot.ExistingRowMatch++
	}
	switch comparison.Reason {
	case core.PrimaryKeyShadowMatchReason:
		s.snapshot.MatchCount++
	case core.PrimaryKeyShadowAuthorityErrorReason:
		s.snapshot.AuthorityErrorCount++
	case core.PrimaryKeyShadowShadowErrorReason:
		s.snapshot.ShadowErrorCount++
	case core.PrimaryKeyShadowNoAuthorityColumnIDReason:
		s.snapshot.SkipCount++
	default:
		s.snapshot.MismatchCount++
	}
	if !comparison.Match && s.snapshot.FirstIssue == "" {
		s.snapshot.FirstIssue = comparison.String()
	}
}

func (s *primaryKeyShadowBenchmarkStats) Snapshot() primaryKeyShadowBenchmarkSnapshot {
	if s == nil {
		return primaryKeyShadowBenchmarkSnapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func primaryKeyShadowModeEnv(name string) (string, error) {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch value {
	case "", "none", "off", "false", "0":
		return "", nil
	case primaryKeyShadowBSIMode, "memory_bsi":
		return primaryKeyShadowBSIMode, nil
	default:
		return "", fmt.Errorf("%s must be one of none, off, bsi, or memory_bsi: %q", name, value)
	}
}
