package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

func TestMySQLCompatibilitySuitesParseAndCarryMetadata(t *testing.T) {
	files, err := filepath.Glob("sqltests/mysql_compat_*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no mysql compatibility suites found")
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
				if test.Compatibility != roadmap.CompatibilityMySQL {
					t.Fatalf("%s compatibility = %q, want mysql", test.ID, test.Compatibility)
				}
				if len(test.Requires) == 0 {
					t.Fatalf("%s missing requires metadata", test.ID)
				}
			}
		})
	}
}
