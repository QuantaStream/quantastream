package qsbridge

// FunctionCallRequest is a protocol-neutral request to evaluate one bound function.
//
// The registry owns the function metadata and context legality; evaluators own
// context-specific coercion and execution. Keeping the call shape here lets SQL
// expressions, catalog defaults, streaming selectors, and future UDF adapters
// share one function execution contract without sharing one evaluator.
type FunctionCallRequest struct {
	Name      string
	Function  FunctionDefinition
	Context   FunctionBindingContext
	Arguments []ResultCell
}

// Clone returns an independent copy of the function call request.
func (r FunctionCallRequest) Clone() FunctionCallRequest {
	r.Function = cloneFunctionDefinition(r.Function)
	r.Arguments = append([]ResultCell(nil), r.Arguments...)
	return r
}

// CanonicalName returns the registry name when bound, otherwise the requested name.
func (r FunctionCallRequest) CanonicalName() string {
	if r.Function.Name != "" {
		return r.Function.Name
	}
	return r.Name
}

// FunctionCallResult is the protocol-neutral result of evaluating one function.
type FunctionCallResult struct {
	Value       ResultCell
	Diagnostics DiagnosticSet
}

// Clone returns an independent copy of the function call result.
func (r FunctionCallResult) Clone() FunctionCallResult {
	r.Diagnostics = cloneDiagnosticSet(r.Diagnostics)
	return r
}

// FunctionEvaluator evaluates a bound function call.
type FunctionEvaluator interface {
	EvaluateFunction(FunctionCallRequest) FunctionCallResult
}

// FunctionEvaluatorFunc adapts a function into a FunctionEvaluator.
type FunctionEvaluatorFunc func(FunctionCallRequest) FunctionCallResult

// EvaluateFunction evaluates a bound function call.
func (f FunctionEvaluatorFunc) EvaluateFunction(request FunctionCallRequest) FunctionCallResult {
	return f(request)
}
