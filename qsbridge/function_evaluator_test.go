package qsbridge

import "testing"

func TestFunctionCallRequestCloneCopiesMutableFields(t *testing.T) {
	request := FunctionCallRequest{
		Name: "lcase",
		Function: FunctionDefinition{
			Name:     "lower",
			Aliases:  []string{"lcase"},
			Contexts: []FunctionBindingContext{FunctionContextCatalogDefault},
		},
		Context: FunctionContextCatalogDefault,
		Arguments: []ResultCell{
			{Kind: ValueString, Value: "Seattle"},
		},
	}

	cloned := request.Clone()
	cloned.Function.Aliases[0] = "changed"
	cloned.Function.Contexts[0] = FunctionContextSQLExpression
	cloned.Arguments[0] = ResultCell{Kind: ValueString, Value: "Tacoma"}

	if got, want := request.Function.Aliases[0], "lcase"; got != want {
		t.Fatalf("alias mutated through clone: got %q, want %q", got, want)
	}
	if got, want := request.Function.Contexts[0], FunctionContextCatalogDefault; got != want {
		t.Fatalf("context mutated through clone: got %q, want %q", got, want)
	}
	if got, want := request.Arguments[0].Value, "Seattle"; got != want {
		t.Fatalf("argument mutated through clone: got %q, want %q", got, want)
	}
}

func TestFunctionCallRequestCanonicalNamePrefersBoundFunction(t *testing.T) {
	request := FunctionCallRequest{
		Name:     "lcase",
		Function: FunctionDefinition{Name: "lower"},
	}

	if got, want := request.CanonicalName(), "lower"; got != want {
		t.Fatalf("CanonicalName = %q, want %q", got, want)
	}
}

func TestFunctionEvaluatorFuncEvaluatesRequest(t *testing.T) {
	evaluator := FunctionEvaluatorFunc(func(request FunctionCallRequest) FunctionCallResult {
		if request.CanonicalName() != "lower" {
			t.Fatalf("CanonicalName = %q, want lower", request.CanonicalName())
		}
		return FunctionCallResult{
			Value: ResultCell{Kind: ValueString, Value: "seattle"},
		}
	})

	result := evaluator.EvaluateFunction(FunctionCallRequest{
		Name:     "lcase",
		Function: FunctionDefinition{Name: "lower"},
	})
	if got, want := result.Value.Value, "seattle"; got != want {
		t.Fatalf("Value = %q, want %q", got, want)
	}
}
