package qsbridge

import "testing"

func TestCatalogExpressionPathParsesDottedPayloadPath(t *testing.T) {
	path, ok := ParseCatalogExpressionPath(" payload.order.id ")
	if !ok {
		t.Fatalf("ParseCatalogExpressionPath returned !ok")
	}
	if got, want := path.String(), "payload.order.id"; got != want {
		t.Fatalf("path.String() = %q, want %q", got, want)
	}
}

func TestCatalogExpressionCopiesDependencies(t *testing.T) {
	dependency := NewCatalogExpressionPath("price")
	expression := ColumnDefaultExpression("price * quantity", dependency)
	dependency.Parts[0] = "changed"

	if got, want := expression.Purpose, CatalogExpressionPurposeColumnDefault; got != want {
		t.Fatalf("Purpose = %q, want %q", got, want)
	}
	if got, want := expression.Dependencies[0].String(), "price"; got != want {
		t.Fatalf("dependency = %q, want %q", got, want)
	}

	replaced := expression.WithDependencies(NewCatalogExpressionPath("quantity"))
	if got, want := replaced.Dependencies[0].String(), "quantity"; got != want {
		t.Fatalf("replaced dependency = %q, want %q", got, want)
	}
	if got, want := expression.Dependencies[0].String(), "price"; got != want {
		t.Fatalf("original dependency = %q, want %q", got, want)
	}
}
