package qsexpr

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// CatalogBuiltinFunctionEvaluator evaluates deterministic built-ins for
// schema-owned default and selector expressions.
//
// It consumes the qsbridge function-call contract so catalog expressions and
// future SQL/runtime evaluators share metadata and dispatch shape without
// forcing all expression contexts through one evaluator.
type CatalogBuiltinFunctionEvaluator struct{}

// EvaluateFunction evaluates one registry-backed catalog function call.
func (CatalogBuiltinFunctionEvaluator) EvaluateFunction(request qsbridge.FunctionCallRequest) qsbridge.FunctionCallResult {
	request = request.Clone()
	if request.Context != qsbridge.FunctionContextCatalogDefault && request.Context != qsbridge.FunctionContextTableSelector {
		return catalogFunctionError(fmt.Sprintf("catalog function evaluator cannot run context %q", request.Context))
	}
	function, diagnostics := catalogFunctionDefinition(request)
	if diagnostics.BlocksNative() {
		return qsbridge.FunctionCallResult{Value: qsbridge.ResultCell{Kind: qsbridge.ValueNull}, Diagnostics: diagnostics}
	}
	displayName := request.Name
	if displayName == "" {
		displayName = function.Name
	}
	switch strings.ToLower(function.Name) {
	case "lower":
		value, diagnostics := catalogSingleStringArgument(displayName, request.Arguments)
		if diagnostics.BlocksNative() {
			return qsbridge.FunctionCallResult{Value: qsbridge.ResultCell{Kind: qsbridge.ValueNull}, Diagnostics: diagnostics}
		}
		return qsbridge.FunctionCallResult{Value: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: strings.ToLower(value)}}
	case "upper":
		value, diagnostics := catalogSingleStringArgument(displayName, request.Arguments)
		if diagnostics.BlocksNative() {
			return qsbridge.FunctionCallResult{Value: qsbridge.ResultCell{Kind: qsbridge.ValueNull}, Diagnostics: diagnostics}
		}
		return qsbridge.FunctionCallResult{Value: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: strings.ToUpper(value)}}
	case "length":
		value, diagnostics := catalogSingleStringArgument(displayName, request.Arguments)
		if diagnostics.BlocksNative() {
			return qsbridge.FunctionCallResult{Value: qsbridge.ResultCell{Kind: qsbridge.ValueNull}, Diagnostics: diagnostics}
		}
		return qsbridge.FunctionCallResult{Value: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: len([]rune(value))}}
	case "hash.sha256":
		if len(request.Arguments) != 1 {
			return catalogFunctionError(fmt.Sprintf("%s() expects 1 argument", displayName))
		}
		text, ok := catalogHashInput(request.Arguments[0])
		if !ok {
			return catalogFunctionError(fmt.Sprintf("%s() requires a non-null scalar argument", displayName))
		}
		sum := sha256.Sum256([]byte(text))
		return qsbridge.FunctionCallResult{Value: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: hex.EncodeToString(sum[:])}}
	default:
		return catalogFunctionError(fmt.Sprintf("catalog function evaluator does not implement %q", function.Name))
	}
}

func catalogFunctionDefinition(request qsbridge.FunctionCallRequest) (qsbridge.FunctionDefinition, qsbridge.DiagnosticSet) {
	if request.Function.Name == "" {
		function, ok := qsbridge.BuiltinScalarFunctionDefinitionForContext(request.Name, request.Context)
		if !ok {
			return qsbridge.FunctionDefinition{}, qsbridge.DiagnosticSet{
				catalogExpressionError(fmt.Sprintf("unsupported catalog expression function %q", request.Name)),
			}
		}
		return function, nil
	}
	if request.Function.Kind != qsbridge.FunctionScalar {
		return qsbridge.FunctionDefinition{}, qsbridge.DiagnosticSet{
			catalogExpressionError(fmt.Sprintf("catalog expression function %q is not scalar", request.Function.Name)),
		}
	}
	if !request.Function.SupportsContext(request.Context) {
		name := request.Name
		if name == "" {
			name = request.Function.Name
		}
		return qsbridge.FunctionDefinition{}, qsbridge.DiagnosticSet{
			catalogExpressionError(fmt.Sprintf("unsupported catalog expression function %q", name)),
		}
	}
	return request.Function, nil
}

func catalogSingleStringArgument(name string, arguments []qsbridge.ResultCell) (string, qsbridge.DiagnosticSet) {
	if len(arguments) != 1 {
		return "", qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("%s() expects 1 argument", name))}
	}
	if arguments[0].Kind != qsbridge.ValueString {
		return "", qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("%s() requires a string argument", name))}
	}
	value, ok := arguments[0].Value.(string)
	if !ok {
		return "", qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("%s() requires a string argument", name))}
	}
	return value, nil
}

func catalogHashInput(cell qsbridge.ResultCell) (string, bool) {
	switch cell.Kind {
	case qsbridge.ValueBool:
		value, ok := cell.Value.(bool)
		if !ok {
			return "", false
		}
		return strconv.FormatBool(value), true
	case qsbridge.ValueInt, qsbridge.ValueFloat:
		number, ok := numberFromAny(cell.Value)
		if !ok {
			return "", false
		}
		return strconv.FormatFloat(number, 'f', -1, 64), true
	case qsbridge.ValueString:
		value, ok := cell.Value.(string)
		return value, ok
	case qsbridge.ValueTime:
		value, ok := cell.Value.(time.Time)
		if !ok {
			return "", false
		}
		return value.UTC().Format(time.RFC3339Nano), true
	default:
		return "", false
	}
}

func catalogFunctionError(message string) qsbridge.FunctionCallResult {
	return qsbridge.FunctionCallResult{
		Value: qsbridge.ResultCell{Kind: qsbridge.ValueNull},
		Diagnostics: qsbridge.DiagnosticSet{
			catalogExpressionError(message),
		},
	}
}
