package qsruntime

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestQSruntimeOwnsLegacyRuntimeAdapterImports(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "quanta_legacy_adapter.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse adapter imports: %v", err)
	}
	imports := make(map[string]struct{}, len(file.Imports))
	for _, imported := range file.Imports {
		importPath, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatalf("unquote adapter import: %v", err)
		}
		imports[importPath] = struct{}{}
	}

	for _, want := range []string{
		"github.com/QuantaStream/quantastream/grpc",
		"github.com/QuantaStream/quantastream/qsbridge",
		"github.com/QuantaStream/quantastream/shared",
	} {
		if _, ok := imports[want]; !ok {
			t.Fatalf("quanta_legacy_adapter.go imports %#v, want %q", imports, want)
		}
	}
}

func TestQSRuntimeDoesNotCallLegacyQuantaJoin(t *testing.T) {
	forbidden := []string{"JoinMerge", "quanta_join.go", "quanta_join"}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read qsruntime package: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		if entry.Name() == "doc.go" {
			continue
		}
		body, err := os.ReadFile(filepath.Join(".", entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		text := string(body)
		for _, token := range forbidden {
			if strings.Contains(text, token) {
				t.Fatalf("%s references legacy join token %q; qsruntime must use explicit kernel contracts instead", entry.Name(), token)
			}
		}
	}
}

func TestQSRuntimeLegacyImportsStayInAdapterIslands(t *testing.T) {
	allowed := map[string]map[string]string{
		"legacy_bitmap_result.go": {
			"github.com/QuantaStream/quantastream/shared": "github.com/QuantaStream/quantastream/shared",
		},
		"legacy_catalog.go": {
			"github.com/QuantaStream/quantastream/core": "github.com/QuantaStream/quantastream/core",
		},
		"legacy_direct_runtime.go": {
			"github.com/QuantaStream/quantastream/core":   "github.com/QuantaStream/quantastream/core",
			"github.com/QuantaStream/quantastream/source": "github.com/QuantaStream/quantastream/source",
		},
		"legacy_direct_relationship_join.go": {
			"github.com/QuantaStream/quantastream/core":   "github.com/QuantaStream/quantastream/core",
			"github.com/QuantaStream/quantastream/source": "github.com/QuantaStream/quantastream/source",
		},
		"legacy_direct_relationship_vector_backend.go": {
			"github.com/QuantaStream/quantastream/core":   "github.com/QuantaStream/quantastream/core",
			"github.com/QuantaStream/quantastream/source": "github.com/QuantaStream/quantastream/source",
		},
		"legacy_direct_same_row_comparison.go": {
			"github.com/QuantaStream/quantastream/core":   "github.com/QuantaStream/quantastream/core",
			"github.com/QuantaStream/quantastream/source": "github.com/QuantaStream/quantastream/source",
		},
		"legacy_direct_same_row_bsi_comparator.go": {
			"github.com/QuantaStream/quantastream/core":   "github.com/QuantaStream/quantastream/core",
			"github.com/QuantaStream/quantastream/source": "github.com/QuantaStream/quantastream/source",
		},
		"legacy_direct_projection_field_reader.go": {
			"github.com/QuantaStream/quantastream/core":   "github.com/QuantaStream/quantastream/core",
			"github.com/QuantaStream/quantastream/source": "github.com/QuantaStream/quantastream/source",
		},
		"legacy_direct_bitmap_group_aggregate.go": {
			"github.com/QuantaStream/quantastream/core": "github.com/QuantaStream/quantastream/core",
		},
		"legacy_direct_bitmap_group_count.go": {
			"github.com/QuantaStream/quantastream/core": "github.com/QuantaStream/quantastream/core",
		},
		"legacy_direct_backing_string_lookup.go": {
			"github.com/QuantaStream/quantastream/core":   "github.com/QuantaStream/quantastream/core",
			"github.com/QuantaStream/quantastream/source": "github.com/QuantaStream/quantastream/source",
		},
		"legacy_metadata_invalidation.go": {
			"github.com/QuantaStream/quantastream/shared": "github.com/QuantaStream/quantastream/shared",
		},
		"schema_mutation.go": {
			"github.com/QuantaStream/quantastream/core":   "github.com/QuantaStream/quantastream/core",
			"github.com/QuantaStream/quantastream/shared": "github.com/QuantaStream/quantastream/shared",
			"github.com/QuantaStream/quantastream/source": "github.com/QuantaStream/quantastream/source",
		},
		"quanta_legacy_adapter.go": {
			"github.com/QuantaStream/quantastream/grpc":   "github.com/QuantaStream/quantastream/grpc",
			"github.com/QuantaStream/quantastream/shared": "github.com/QuantaStream/quantastream/shared",
		},
	}
	legacyImports := []string{
		"github.com/QuantaStream/quantastream/core",
		"github.com/QuantaStream/quantastream/grpc",
		"github.com/QuantaStream/quantastream/server",
		"github.com/QuantaStream/quantastream/shared",
		"github.com/QuantaStream/quantastream/source",
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read qsruntime package: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", entry.Name()), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s imports: %v", entry.Name(), err)
		}
		for _, imported := range file.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatalf("unquote %s import: %v", entry.Name(), err)
			}
			if !isLegacyRuntimeImport(importPath, legacyImports) {
				continue
			}
			inventorySubject, ok := allowed[entry.Name()][importPath]
			if !ok {
				t.Fatalf("%s imports legacy package %q outside allowed adapter islands", entry.Name(), importPath)
			}
			if _, ok := qsbridge.LegacyDependencyInventoryForSubject(inventorySubject); !ok {
				t.Fatalf("%s legacy import %q references missing retirement inventory subject %q", entry.Name(), importPath, inventorySubject)
			}
		}
	}
}

func isLegacyRuntimeImport(importPath string, legacyImports []string) bool {
	for _, legacyImport := range legacyImports {
		if importPath == legacyImport {
			return true
		}
	}
	return false
}
