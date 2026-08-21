package version

import (
	"strings"
	"testing"
)

func TestMySQLVersionDefaultsToCompatibilityString(t *testing.T) {
	oldVersion, oldCommit, oldBuildDate := Version, Commit, BuildDate
	defer func() {
		Version, Commit, BuildDate = oldVersion, oldCommit, oldBuildDate
	}()
	Version = "0.0.0-dev"
	Commit = "unknown"
	BuildDate = "unknown"

	if got := MySQLVersion(); got != "8.0.0 QuantaStream 0.0.0-dev" {
		t.Fatalf("MySQLVersion() = %q", got)
	}
	if got := MySQLVersionComment(); got != ProductName {
		t.Fatalf("MySQLVersionComment() = %q, want product name", got)
	}
}

func TestSummaryIncludesReleaseMetadata(t *testing.T) {
	oldVersion, oldCommit, oldBuildDate := Version, Commit, BuildDate
	defer func() {
		Version, Commit, BuildDate = oldVersion, oldCommit, oldBuildDate
	}()
	Version = "1.2.3"
	Commit = "abcdef123456"
	BuildDate = "2026-08-21T12:00:00Z"

	summary := Summary()
	for _, want := range []string{ProductName, "1.2.3", "abcdef123456", "2026-08-21T12:00:00Z"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("Summary() = %q, missing %q", summary, want)
		}
	}
	if !strings.Contains(MySQLVersionComment(), summary) {
		t.Fatalf("MySQLVersionComment() = %q, want summary %q", MySQLVersionComment(), summary)
	}
	if got, want := MySQLVersion(), "8.0.0 QuantaStream 1.2.3"; got != want {
		t.Fatalf("MySQLVersion() = %q, want %q", got, want)
	}
}
