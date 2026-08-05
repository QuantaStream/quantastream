package qsinabox

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

const (
	primaryKeyAuthorityBSIMode = "bsi"
	primaryKeyShadowBSIMode    = "bsi"
)

func primaryKeyAuthorityModeEnv(name string) (string, error) {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch value {
	case "", "none", "off", "false", "0", "kv", "default":
		return "", nil
	case primaryKeyAuthorityBSIMode, "memory_bsi", "native_bsi", "typed_bsi":
		return primaryKeyAuthorityBSIMode, nil
	default:
		return "", fmt.Errorf("%s must be one of none, off, kv, default, bsi, memory_bsi, native_bsi, or typed_bsi: %q", name, value)
	}
}

func primaryKeyShadowModeEnv(name string) (string, error) {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch value {
	case "", "none", "off", "false", "0":
		return "", nil
	case primaryKeyShadowBSIMode, "memory_bsi", "native_bsi", "typed_bsi":
		return primaryKeyShadowBSIMode, nil
	default:
		return "", fmt.Errorf("%s must be one of none, off, bsi, memory_bsi, native_bsi, or typed_bsi: %q", name, value)
	}
}

func TestPrimaryKeyBenchmarkModeEnvAcceptsNativeBSIAliases(t *testing.T) {
	t.Setenv("QS_TEST_PK_AUTHORITY", "native_bsi")
	authority, err := primaryKeyAuthorityModeEnv("QS_TEST_PK_AUTHORITY")
	if err != nil {
		t.Fatalf("primaryKeyAuthorityModeEnv() error = %v", err)
	}
	if authority != primaryKeyAuthorityBSIMode {
		t.Fatalf("authority mode = %q, want %q", authority, primaryKeyAuthorityBSIMode)
	}

	t.Setenv("QS_TEST_PK_SHADOW", "typed_bsi")
	shadow, err := primaryKeyShadowModeEnv("QS_TEST_PK_SHADOW")
	if err != nil {
		t.Fatalf("primaryKeyShadowModeEnv() error = %v", err)
	}
	if shadow != primaryKeyShadowBSIMode {
		t.Fatalf("shadow mode = %q, want %q", shadow, primaryKeyShadowBSIMode)
	}
}
