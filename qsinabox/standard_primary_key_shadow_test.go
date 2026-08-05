package qsinabox

import (
	"fmt"
	"os"
	"strings"
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
	case primaryKeyAuthorityBSIMode, "memory_bsi":
		return primaryKeyAuthorityBSIMode, nil
	default:
		return "", fmt.Errorf("%s must be one of none, off, kv, default, bsi, or memory_bsi: %q", name, value)
	}
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
