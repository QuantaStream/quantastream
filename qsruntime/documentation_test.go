package qsruntime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQSRuntimePackageDocumentationStandardIsVisible(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "doc.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse package docs: %v", err)
	}
	if file.Doc == nil || !strings.Contains(file.Doc.Text(), "transitional runtime adapters") {
		t.Fatalf("package docs = %#v, want qsruntime package-level documentation", file.Doc)
	}
}

func TestQSRuntimeExportedDeclarationsHaveDocumentation(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read qsruntime package: %v", err)
	}

	var missing []string
	for _, entry := range files {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(".", entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		missing = append(missing, undocumentedRuntimeExports(entry.Name(), file)...)
	}
	if len(missing) > 0 {
		t.Fatalf("exported qsruntime declarations missing documentation:\n%s", strings.Join(missing, "\n"))
	}
}

func undocumentedRuntimeExports(fileName string, file *ast.File) []string {
	var missing []string
	for _, declaration := range file.Decls {
		switch decl := declaration.(type) {
		case *ast.FuncDecl:
			if decl.Name.IsExported() && decl.Doc == nil {
				missing = append(missing, fmt.Sprintf("%s: %s", fileName, decl.Name.Name))
			}
		case *ast.GenDecl:
			missing = append(missing, undocumentedRuntimeGenDecls(fileName, decl)...)
		}
	}
	return missing
}

func undocumentedRuntimeGenDecls(fileName string, decl *ast.GenDecl) []string {
	var missing []string
	for _, spec := range decl.Specs {
		switch typed := spec.(type) {
		case *ast.TypeSpec:
			if typed.Name.IsExported() && decl.Doc == nil && typed.Doc == nil {
				missing = append(missing, fmt.Sprintf("%s: %s", fileName, typed.Name.Name))
			}
		case *ast.ValueSpec:
			for _, name := range typed.Names {
				if name.IsExported() && decl.Doc == nil && typed.Doc == nil {
					missing = append(missing, fmt.Sprintf("%s: %s", fileName, name.Name))
				}
			}
		}
	}
	return missing
}
