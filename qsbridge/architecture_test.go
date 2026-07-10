package qsbridge

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var bannedRuntimeImportPrefixes = []string{
	"github.com/QuantaStream/quantastream/core",
	"github.com/QuantaStream/quantastream/grpc",
	"github.com/QuantaStream/quantastream/qlbridge",
	"github.com/QuantaStream/quantastream/qsruntime",
	"github.com/QuantaStream/quantastream/server",
	"github.com/QuantaStream/quantastream/shared",
	"github.com/QuantaStream/quantastream/source",
}

func TestQSBridgeDoesNotImportLegacyOrRuntimePackages(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read qsbridge package: %v", err)
	}

	for _, entry := range files {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := filepath.Join(".", entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports for %s: %v", entry.Name(), err)
		}
		for _, imported := range file.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", entry.Name(), err)
			}
			if bannedRuntimeImport(importPath) {
				t.Fatalf("%s imports %q; qsbridge must remain detached from legacy/runtime packages", entry.Name(), importPath)
			}
		}
	}
}

func bannedRuntimeImport(importPath string) bool {
	for _, prefix := range bannedRuntimeImportPrefixes {
		if importPath == prefix || strings.HasPrefix(importPath, prefix+"/") {
			return true
		}
	}
	return false
}
