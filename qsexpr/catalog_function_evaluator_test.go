package qsexpr

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestCatalogBuiltinFunctionEvaluatorEvaluatesRegistryAlias(t *testing.T) {
	function, ok := qsbridge.BuiltinScalarFunctionDefinitionForContext("ucase", qsbridge.FunctionContextCatalogDefault)
	if !ok {
		t.Fatalf("ucase did not resolve in catalog default context")
	}

	result := CatalogBuiltinFunctionEvaluator{}.EvaluateFunction(qsbridge.FunctionCallRequest{
		Name:     "ucase",
		Function: function,
		Context:  qsbridge.FunctionContextCatalogDefault,
		Arguments: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueString, Value: "Tacoma"},
		},
	})

	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if got, want := result.Value.Kind, qsbridge.ValueString; got != want {
		t.Fatalf("Kind = %q, want %q", got, want)
	}
	if got, want := result.Value.Value, "TACOMA"; got != want {
		t.Fatalf("Value = %q, want %q", got, want)
	}
}

func TestCatalogBuiltinFunctionEvaluatorEvaluatesHashThroughContract(t *testing.T) {
	result := CatalogBuiltinFunctionEvaluator{}.EvaluateFunction(qsbridge.FunctionCallRequest{
		Name:    "hash.sha256",
		Context: qsbridge.FunctionContextTableSelector,
		Arguments: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueString, Value: "C001"},
		},
	})

	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	expected := sha256.Sum256([]byte("C001"))
	if got, want := result.Value.Value, hex.EncodeToString(expected[:]); got != want {
		t.Fatalf("Value = %q, want %q", got, want)
	}
}

func TestCatalogBuiltinFunctionEvaluatorRejectsNonCatalogContext(t *testing.T) {
	result := CatalogBuiltinFunctionEvaluator{}.EvaluateFunction(qsbridge.FunctionCallRequest{
		Name:    "lower",
		Context: qsbridge.FunctionContextSQLExpression,
		Arguments: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueString, Value: "Seattle"},
		},
	})

	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected diagnostics for SQL context")
	}
}

func TestCatalogBuiltinFunctionEvaluatorRejectsFunctionOutsideContext(t *testing.T) {
	result := CatalogBuiltinFunctionEvaluator{}.EvaluateFunction(qsbridge.FunctionCallRequest{
		Name:    "substring",
		Context: qsbridge.FunctionContextTableSelector,
		Arguments: []qsbridge.ResultCell{
			{Kind: qsbridge.ValueString, Value: "Seattle"},
			{Kind: qsbridge.ValueInt, Value: 1},
			{Kind: qsbridge.ValueInt, Value: 2},
		},
	})

	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected diagnostics for substring in selector context")
	}
}
