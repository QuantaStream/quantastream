// Package version exposes build identity shared by QuantaStream binaries and
// MySQL-facing metadata.
package version

import (
	"fmt"
	"strings"
)

const (
	// ProductName is the public product name rendered in support output.
	ProductName = "QuantaStream"

	// ShortName is the compact product label for operational artifact names and
	// narrow status surfaces.
	ShortName = "QStream"

	// MySQLCompatibilityPrefix is the server-version prefix exposed to MySQL
	// clients. Keep the MySQL-looking prefix stable for client/tool parsing.
	MySQLCompatibilityPrefix = "8.0.0"
)

// These variables are intentionally string vars so release builds can set exact
// build metadata with go build -ldflags -X
// github.com/QuantaStream/quantastream/version.Name=value. Source builds carry
// the current release version and omit commit/build-date metadata unless the
// caller supplies ldflags.
var (
	Version   = "0.1.1-rc2"
	Commit    = ""
	BuildDate = ""
)

// Summary returns a concise one-line support identifier.
func Summary() string {
	parts := []string{ProductName, normalized(Version, "0.0.0-dev")}
	metadata := make([]string, 0, 2)
	if commit := normalized(Commit, ""); commit != "" {
		metadata = append(metadata, "commit "+commit)
	}
	if buildDate := normalized(BuildDate, ""); buildDate != "" {
		metadata = append(metadata, "built "+buildDate)
	}
	if len(metadata) > 0 {
		parts = append(parts, "("+strings.Join(metadata, ", ")+")")
	}
	return strings.Join(parts, " ")
}

// BuildString returns the build metadata printed by older command surfaces.
func BuildString() string {
	return fmt.Sprintf("commit=%s build_date=%s", normalized(Commit, "unknown"), normalized(BuildDate, "unknown"))
}

// MySQLVersion returns the MySQL-compatible server version string.
func MySQLVersion() string {
	return fmt.Sprintf("%s %s %s", MySQLCompatibilityPrefix, ProductName, normalized(Version, "0.0.0-dev"))
}

// MySQLVersionComment returns human-facing product/build metadata for MySQL
// clients that inspect @@version_comment or SHOW VARIABLES.
func MySQLVersionComment() string {
	if normalized(Version, "0.0.0-dev") == "0.0.0-dev" && normalized(Commit, "unknown") == "unknown" && normalized(BuildDate, "unknown") == "unknown" {
		return ProductName
	}
	return Summary()
}

func normalized(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
