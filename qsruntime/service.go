package qsruntime

import "context"

// ExecutionService routes neutral execution requests to configured executors.
type ExecutionService struct {
	Selector ExecutorSelector
}

// NewExecutionService builds an execution service from direct and legacy executors.
func NewExecutionService(direct Executor, legacy Executor) ExecutionService {
	return ExecutionService{
		Selector: ExecutorSelector{
			Direct: direct,
			Legacy: legacy,
		},
	}
}

// Execute selects an executor from the request route and runs it.
func (s ExecutionService) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	executor, diagnostics := s.Selector.SelectRequest(request)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, nil
	}
	return executor.Execute(ctx, request)
}
