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

// primaryKeyAuthorityModeEnv treats the empty mode as the standard BSI
// authority path. KV authority is available only through explicit shadow
// comparison wiring.
func primaryKeyAuthorityModeEnv(name string) (string, error) {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch value {
	case "", "default", primaryKeyAuthorityBSIMode, "memory_bsi", "native_bsi", "typed_bsi":
		return primaryKeyAuthorityBSIMode, nil
	case "none", "off", "false", "0":
		return "", nil
	case "kv":
		return "", fmt.Errorf("%s no longer accepts kv authority; use QUANTASTREAM_TPCH_INGEST_BENCH_PK_SHADOW=bsi for transition comparison", name)
	default:
		return "", fmt.Errorf("%s must be one of default, bsi, memory_bsi, native_bsi, typed_bsi, none, off, false, or 0: %q", name, value)
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

func TestPrimaryKeyBenchmarkAuthorityModeRejectsKVDefault(t *testing.T) {
	t.Setenv("QS_TEST_PK_AUTHORITY", "kv")
	_, err := primaryKeyAuthorityModeEnv("QS_TEST_PK_AUTHORITY")
	if err == nil {
		t.Fatalf("primaryKeyAuthorityModeEnv(kv) error = nil, want explicit shadow guidance")
	}
	if !strings.Contains(err.Error(), "PK_SHADOW=bsi") {
		t.Fatalf("primaryKeyAuthorityModeEnv(kv) error = %v, want shadow guidance", err)
	}
}
