package test

import "testing"

func TestEnvTruthyAcceptsExpectedNodesOnlyValues(t *testing.T) {
	for _, value := range []string{"1", "true", "TRUE", "yes", "YES", "on", "ON", " true "} {
		if !envTruthy(value) {
			t.Fatalf("envTruthy(%q) = false, want true", value)
		}
	}
}

func TestEnvTruthyRejectsDefaultNodesOnlyValues(t *testing.T) {
	for _, value := range []string{"", "0", "false", "no", "off", "banana"} {
		if envTruthy(value) {
			t.Fatalf("envTruthy(%q) = true, want false", value)
		}
	}
}
