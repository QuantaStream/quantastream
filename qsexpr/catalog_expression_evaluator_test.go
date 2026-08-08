package qsexpr

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestCatalogExpressionEvaluatorEvaluatesBlindColumnDefault(t *testing.T) {
	evaluator := CatalogExpressionEvaluator{}
	cell, diagnostics := evaluator.EvaluateDefault(
		qsbridge.ColumnDefaultExpression("price * quantity"),
		map[string]any{
			"price":    1.50,
			"quantity": 3,
		},
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if got, want := cell.Kind, qsbridge.ValueFloat; got != want {
		t.Fatalf("Kind = %q, want %q", got, want)
	}
	if got, want := cell.Value.(float64), 4.5; got != want {
		t.Fatalf("Value = %v, want %v", got, want)
	}
}

func TestCatalogExpressionEvaluatorEvaluatesNestedSelector(t *testing.T) {
	evaluator := CatalogExpressionEvaluator{}
	matched, diagnostics := evaluator.EvaluateSelector(
		qsbridge.TableSelectorExpression(`lower(payload.kind) = "order" && payload.order.id != null`),
		map[string]any{
			"payload": map[string]any{
				"kind": "ORDER",
				"order": map[string]any{
					"id": 1001,
				},
			},
		},
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if !matched {
		t.Fatalf("matched = false, want true")
	}
}

func TestCompiledCatalogExpressionEvaluatorEvaluatesSelector(t *testing.T) {
	evaluator, diagnostics := CompileSelectorExpression(
		qsbridge.TableSelectorExpression(`payload.kind = "order" && payload.id != null`),
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("compile diagnostics = %#v", diagnostics)
	}

	matched, diagnostics := evaluator.EvaluateSelector(
		qsbridge.TableSelectorExpression(`payload.kind = "order" && payload.id != null`),
		map[string]any{
			"payload": map[string]any{
				"kind": "order",
				"id":   1001,
			},
		},
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("evaluate diagnostics = %#v", diagnostics)
	}
	if !matched {
		t.Fatalf("matched = false, want true")
	}
}

func TestCompileCatalogExpressionRejectsInvalidExpression(t *testing.T) {
	evaluator, diagnostics := CompileSelectorExpression(qsbridge.TableSelectorExpression(`payload.kind =`))
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking diagnostic", diagnostics)
	}
	if evaluator != nil {
		t.Fatalf("evaluator = %#v, want nil on compile failure", evaluator)
	}
}

func TestCatalogExpressionEvaluatorEvaluatesSelectorAliasThroughRegistry(t *testing.T) {
	evaluator := CatalogExpressionEvaluator{}
	matched, diagnostics := evaluator.EvaluateSelector(
		qsbridge.TableSelectorExpression(`lcase(payload.kind) = "order"`),
		map[string]any{
			"payload": map[string]any{
				"kind": "ORDER",
			},
		},
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if !matched {
		t.Fatalf("matched = false, want true")
	}
}

func TestCatalogExpressionEvaluatorEvaluatesNowDefault(t *testing.T) {
	fixedNow := time.Date(2026, 7, 7, 13, 14, 15, 123000000, time.UTC)
	evaluator := CatalogExpressionEvaluator{
		Now: func() time.Time {
			return fixedNow
		},
	}
	cell, diagnostics := evaluator.EvaluateDefault(
		qsbridge.ColumnDefaultExpression("now()"),
		map[string]any{},
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if got, want := cell.Kind, qsbridge.ValueTime; got != want {
		t.Fatalf("Kind = %q, want %q", got, want)
	}
	if got, want := cell.Value.(time.Time), fixedNow; !got.Equal(want) {
		t.Fatalf("Value = %v, want %v", got, want)
	}
}

func TestCatalogExpressionEvaluatorRejectsVolatileSelectorFunction(t *testing.T) {
	evaluator := CatalogExpressionEvaluator{
		Now: func() time.Time {
			return time.Date(2026, 7, 7, 13, 14, 15, 123000000, time.UTC)
		},
	}
	matched, diagnostics := evaluator.EvaluateSelector(
		qsbridge.TableSelectorExpression("now() != null"),
		map[string]any{},
	)

	if !diagnostics.BlocksNative() {
		t.Fatalf("expected diagnostics for volatile selector function")
	}
	if matched {
		t.Fatalf("matched = true, want false")
	}
}

func TestCatalogExpressionEvaluatorRejectsFunctionOutsideSelectorContext(t *testing.T) {
	evaluator := CatalogExpressionEvaluator{}
	matched, diagnostics := evaluator.EvaluateSelector(
		qsbridge.TableSelectorExpression(`substring(payload.kind, 1, 1) = "O"`),
		map[string]any{
			"payload": map[string]any{
				"kind": "ORDER",
			},
		},
	)

	if !diagnostics.BlocksNative() {
		t.Fatalf("expected diagnostics for substring selector function")
	}
	if matched {
		t.Fatalf("matched = true, want false")
	}
}

func TestCatalogExpressionEvaluatorEvaluatesNestedFunctionArguments(t *testing.T) {
	fixedNow := time.Date(2026, 7, 7, 13, 14, 15, 123000000, time.UTC)
	evaluator := CatalogExpressionEvaluator{
		Now: func() time.Time {
			return fixedNow
		},
	}
	cell, diagnostics := evaluator.EvaluateDefault(
		qsbridge.ColumnDefaultExpression("unixmillis(now()) + offset"),
		map[string]any{
			"offset": 7,
		},
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if got, want := cell.Kind, qsbridge.ValueFloat; got != want {
		t.Fatalf("Kind = %q, want %q", got, want)
	}
	if got, want := cell.Value.(float64), float64(fixedNow.UnixMilli()+7); got != want {
		t.Fatalf("Value = %v, want %v", got, want)
	}
}

func TestCatalogExpressionEvaluatorEvaluatesNamespacedHashFunction(t *testing.T) {
	evaluator := CatalogExpressionEvaluator{}
	cell, diagnostics := evaluator.EvaluateDefault(
		qsbridge.ColumnDefaultExpression("hash.sha256(cust_id)"),
		map[string]any{
			"cust_id": "C001",
		},
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if got, want := cell.Kind, qsbridge.ValueString; got != want {
		t.Fatalf("Kind = %q, want %q", got, want)
	}
	expected := sha256.Sum256([]byte("C001"))
	if got, want := cell.Value.(string), hex.EncodeToString(expected[:]); got != want {
		t.Fatalf("Value = %q, want %q", got, want)
	}
}

func TestCatalogExpressionEvaluatorReturnsFalseSelector(t *testing.T) {
	evaluator := CatalogExpressionEvaluator{}
	matched, diagnostics := evaluator.EvaluateSelector(
		qsbridge.TableSelectorExpression(`payload.kind = "lineitem" || payload.kind = "customer"`),
		map[string]any{
			"payload": map[string]any{
				"kind": "order",
			},
		},
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if matched {
		t.Fatalf("matched = true, want false")
	}
}

func TestCatalogExpressionEvaluatorReportsMissingReference(t *testing.T) {
	evaluator := CatalogExpressionEvaluator{}
	_, diagnostics := evaluator.EvaluateDefault(
		qsbridge.ColumnDefaultExpression("price * quantity"),
		map[string]any{
			"price": 1.50,
		},
	)

	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if got, want := diagnostics[0].Code, qsbridge.DiagnosticCatalogExpressionUnresolved; got != want {
		t.Fatalf("diagnostic code = %q, want %q", got, want)
	}
}

func TestCatalogExpressionDependenciesAreStable(t *testing.T) {
	dependencies, diagnostics := CatalogExpressionDependencies(`lower(payload.kind) = "order" && payload.order.id != null && price * quantity > 0 && hash.sha256(cust_id) != ""`)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	got := make([]string, 0, len(dependencies))
	for _, dependency := range dependencies {
		got = append(got, dependency.String())
	}
	want := []string{"cust_id", "payload.kind", "payload.order.id", "price", "quantity"}
	if len(got) != len(want) {
		t.Fatalf("dependencies = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("dependencies = %#v, want %#v", got, want)
		}
	}
}
