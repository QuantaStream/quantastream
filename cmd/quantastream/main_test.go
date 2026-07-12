package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunStatusPrintsInaboxStandardSkeleton(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-status"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	output := stdout.String()
	for _, want := range []string{
		"mode=inabox-standard",
		"mysql=127.0.0.1:4000",
		"local_node_ready=false",
		"streaming_risk=",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
}

func TestRunRejectsUnsupportedMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-mode", "distributed"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("unsupported mode exited successfully")
	}
	if !strings.Contains(stderr.String(), "unsupported mode") {
		t.Fatalf("stderr = %q, want unsupported mode", stderr.String())
	}
}
