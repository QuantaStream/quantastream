package qsbridge

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

func TestQSBridgePackageDocumentationStandardIsVisible(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "doc.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse package docs: %v", err)
	}
	if file.Doc == nil || !strings.Contains(file.Doc.Text(), "native SQL bridge planning vocabulary") {
		t.Fatalf("package docs = %#v, want qsbridge package-level documentation", file.Doc)
	}

	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	text := string(readme)
	for _, phrase := range []string{
		"All exported types, constants, and functions need documentation comments.",
		"counter-intuitive planner behavior",
		"Tests should assert typed behavior, stable diagnostic codes, and plan shape.",
	} {
		if !strings.Contains(text, phrase) {
			t.Fatalf("README.md missing documentation standard phrase %q", phrase)
		}
	}
}

func TestQSBridgeREADMEPointsToInternalArchitectureNotes(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	text := string(readme)
	for _, phrase := range []string{
		"quantastream-internal/blob/main/docs/qsbridge/README.md",
		"quantastream-internal/blob/main/docs/qsbridge/DESIGN_DECISIONS.md",
		"Keep internal roadmap state in those files",
	} {
		if !strings.Contains(text, phrase) {
			t.Fatalf("README.md missing internal documentation pointer %q", phrase)
		}
	}
}

func TestQSBridgeExportedDeclarationsHaveDocumentation(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read qsbridge package: %v", err)
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
		missing = append(missing, undocumentedExportedDeclarations(entry.Name(), file)...)
	}
	if len(missing) > 0 {
		t.Fatalf("exported declarations missing documentation:\n%s", strings.Join(missing, "\n"))
	}
}

func undocumentedExportedDeclarations(fileName string, file *ast.File) []string {
	var missing []string
	for _, declaration := range file.Decls {
		switch decl := declaration.(type) {
		case *ast.FuncDecl:
			if decl.Name.IsExported() && decl.Doc == nil {
				missing = append(missing, fmt.Sprintf("%s: %s", fileName, decl.Name.Name))
			}
		case *ast.GenDecl:
			missing = append(missing, undocumentedExportedGenDecls(fileName, decl)...)
		}
	}
	return missing
}

func undocumentedExportedGenDecls(fileName string, decl *ast.GenDecl) []string {
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
