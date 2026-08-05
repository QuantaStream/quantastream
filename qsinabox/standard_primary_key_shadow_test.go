package qsinabox

import (
	"fmt"
	"os"
	"strings"
)

const primaryKeyShadowBSIMode = "bsi"

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
