package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

func TestQuantaSuitesParseAndCarryMetadata(t *testing.T) {
	files, err := filepath.Glob("sqltests/quanta*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no QuantaStream suites found")
	}

	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			suite, err := roadmap.Parse(data)
			if err != nil {
				t.Fatal(err)
			}
			if len(suite.Tests) == 0 {
				t.Fatal("suite has no tests")
			}
			for _, test := range suite.Tests {
				if test.Status == roadmap.CaseSkip {
					continue
				}
				if test.Feature == "" {
					t.Fatalf("%s missing feature metadata", test.ID)
				}
				switch test.Compatibility {
				case roadmap.CompatibilityQuanta, roadmap.CompatibilityQuantaExtension:
				default:
					t.Fatalf("%s compatibility = %q, want quanta or quanta_extension", test.ID, test.Compatibility)
				}
				if len(test.Requires) == 0 {
					t.Fatalf("%s missing requires metadata", test.ID)
				}
			}
		})
	}
}
