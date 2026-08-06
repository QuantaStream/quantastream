package qsinabox

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

const (
	primaryKeyAuthorityBSIMode = "bsi"
)

// primaryKeyAuthorityModeEnv treats the empty mode as the standard BSI
// authority path.
func primaryKeyAuthorityModeEnv(name string) (string, error) {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch value {
	case "", "default", primaryKeyAuthorityBSIMode, "memory_bsi", "native_bsi", "typed_bsi":
		return primaryKeyAuthorityBSIMode, nil
	case "none", "off", "false", "0":
		return "", nil
	default:
		return "", fmt.Errorf("%s must be one of default, bsi, memory_bsi, native_bsi, typed_bsi, none, off, false, or 0: %q", name, value)
	}
}

func TestPrimaryKeyBenchmarkAuthorityModeEnvAcceptsNativeBSIAliases(t *testing.T) {
	t.Setenv("QS_TEST_PK_AUTHORITY", "native_bsi")
	authority, err := primaryKeyAuthorityModeEnv("QS_TEST_PK_AUTHORITY")
	if err != nil {
		t.Fatalf("primaryKeyAuthorityModeEnv() error = %v", err)
	}
	if authority != primaryKeyAuthorityBSIMode {
		t.Fatalf("authority mode = %q, want %q", authority, primaryKeyAuthorityBSIMode)
	}
}

func TestPrimaryKeyBenchmarkAuthorityModeDefaultsToBSI(t *testing.T) {
	for _, value := range []string{"", "default"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("QS_TEST_PK_AUTHORITY", value)
			authority, err := primaryKeyAuthorityModeEnv("QS_TEST_PK_AUTHORITY")
			if err != nil {
				t.Fatalf("primaryKeyAuthorityModeEnv(%q) error = %v", value, err)
			}
			if authority != primaryKeyAuthorityBSIMode {
				t.Fatalf("authority mode = %q, want BSI authority mode for %q", authority, value)
			}
		})
	}
}
