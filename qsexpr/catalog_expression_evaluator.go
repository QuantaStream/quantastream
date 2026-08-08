package qsexpr

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// CatalogExpressionEvaluator evaluates schema-owned defaults and selectors without qlbridge.
type CatalogExpressionEvaluator struct {
	Now        func() time.Time
	expression qsbridge.CatalogExpression
	node       catalogExpressionNode
}

// CompileCatalogExpression returns an evaluator with expression parsed once for
// repeated selector/default evaluation.
func CompileCatalogExpression(expression qsbridge.CatalogExpression) (*CatalogExpressionEvaluator, qsbridge.DiagnosticSet) {
	node, diagnostics := parseCatalogExpression(expression.Raw)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	return &CatalogExpressionEvaluator{expression: expression, node: node}, nil
}

// CompileDefaultExpression returns an evaluator with a default expression parsed
// once for repeated evaluation.
func CompileDefaultExpression(expression qsbridge.CatalogExpression) (*CatalogExpressionEvaluator, qsbridge.DiagnosticSet) {
	return CompileCatalogExpression(expression)
}

// CompileSelectorExpression returns an evaluator with a selector expression
// parsed once for repeated evaluation.
func CompileSelectorExpression(expression qsbridge.CatalogExpression) (*CatalogExpressionEvaluator, qsbridge.DiagnosticSet) {
	return CompileCatalogExpression(expression)
}

// EvaluateDefault evaluates a blind-column INSERT default expression against row values.
func (e CatalogExpressionEvaluator) EvaluateDefault(expression qsbridge.CatalogExpression, values map[string]any) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	return e.evaluate(expression, values)
}

// EvaluateSelector evaluates a table selector expression against an ingest payload.
func (e CatalogExpressionEvaluator) EvaluateSelector(expression qsbridge.CatalogExpression, payload map[string]any) (bool, qsbridge.DiagnosticSet) {
	cell, diagnostics := e.evaluate(expression, payload)
	if diagnostics.BlocksNative() {
		return false, diagnostics
	}
	value, ok := catalogValueFromResultCell(cell)
	if !ok || value.kind != catalogValueBool {
		return false, qsbridge.DiagnosticSet{
			catalogExpressionError("selector expression must evaluate to bool"),
		}
	}
	return value.boolValue, nil
}

// CatalogExpressionDependencies extracts row or payload paths referenced by expression text.
func CatalogExpressionDependencies(raw string) ([]qsbridge.CatalogExpressionPath, qsbridge.DiagnosticSet) {
	node, diagnostics := parseCatalogExpression(raw)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	seen := map[string]qsbridge.CatalogExpressionPath{}
	node.collectDependencies(seen)
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	dependencies := make([]qsbridge.CatalogExpressionPath, 0, len(keys))
	for _, key := range keys {
		dependencies = append(dependencies, seen[key])
	}
	return dependencies, nil
}

func (e CatalogExpressionEvaluator) evaluate(expression qsbridge.CatalogExpression, values map[string]any) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	node := e.node
	if node == nil || e.expression.Raw != expression.Raw || e.expression.Purpose != expression.Purpose {
		var diagnostics qsbridge.DiagnosticSet
		node, diagnostics = parseCatalogExpression(expression.Raw)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull}, diagnostics
		}
	}
	value, diagnostics := node.eval(catalogExpressionEvalContext{values: values, now: e.now, purpose: expression.Purpose})
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull}, diagnostics
	}
	return value.resultCell(), nil
}

func (e CatalogExpressionEvaluator) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now().UTC()
}

type catalogExpressionTokenKind int

const (
	catalogTokenEOF catalogExpressionTokenKind = iota
	catalogTokenIdentifier
	catalogTokenNumber
	catalogTokenString
	catalogTokenBool
	catalogTokenNull
	catalogTokenOperator
	catalogTokenLeftParen
	catalogTokenRightParen
	catalogTokenComma
)

type catalogExpressionToken struct {
	kind    catalogExpressionTokenKind
	literal string
}

func parseCatalogExpression(raw string) (catalogExpressionNode, qsbridge.DiagnosticSet) {
	tokens, diagnostics := lexCatalogExpression(raw)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	parser := catalogExpressionParser{tokens: tokens}
	node, diagnostics := parser.parseOr()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	if parser.peek().kind != catalogTokenEOF {
		return nil, qsbridge.DiagnosticSet{
			catalogExpressionError(fmt.Sprintf("unexpected token %q", parser.peek().literal)),
		}
	}
	return node, nil
}

func lexCatalogExpression(raw string) ([]catalogExpressionToken, qsbridge.DiagnosticSet) {
	var tokens []catalogExpressionToken
	for index := 0; index < len(raw); {
		r := rune(raw[index])
		if unicode.IsSpace(r) {
			index++
			continue
		}
		if isCatalogIdentifierStart(r) {
			start := index
			index++
			for index < len(raw) && isCatalogIdentifierPart(rune(raw[index])) {
				index++
			}
			literal := raw[start:index]
			switch strings.ToLower(literal) {
			case "true", "false":
				tokens = append(tokens, catalogExpressionToken{kind: catalogTokenBool, literal: strings.ToLower(literal)})
			case "null":
				tokens = append(tokens, catalogExpressionToken{kind: catalogTokenNull, literal: "null"})
			case "and":
				tokens = append(tokens, catalogExpressionToken{kind: catalogTokenOperator, literal: "&&"})
			case "or":
				tokens = append(tokens, catalogExpressionToken{kind: catalogTokenOperator, literal: "||"})
			case "not":
				tokens = append(tokens, catalogExpressionToken{kind: catalogTokenOperator, literal: "!"})
			default:
				tokens = append(tokens, catalogExpressionToken{kind: catalogTokenIdentifier, literal: literal})
			}
			continue
		}
		if unicode.IsDigit(r) || r == '.' {
			start := index
			seenDigit := false
			seenDot := false
			for index < len(raw) {
				current := rune(raw[index])
				if unicode.IsDigit(current) {
					seenDigit = true
					index++
					continue
				}
				if current == '.' && !seenDot {
					seenDot = true
					index++
					continue
				}
				break
			}
			if !seenDigit {
				return nil, qsbridge.DiagnosticSet{catalogExpressionError("invalid numeric literal")}
			}
			tokens = append(tokens, catalogExpressionToken{kind: catalogTokenNumber, literal: raw[start:index]})
			continue
		}
		if r == '\'' || r == '"' {
			literal, next, diagnostics := lexCatalogString(raw, index)
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			tokens = append(tokens, catalogExpressionToken{kind: catalogTokenString, literal: literal})
			index = next
			continue
		}
		if index+1 < len(raw) {
			two := raw[index : index+2]
			switch two {
			case "==", "!=", ">=", "<=", "&&", "||", "<>":
				if two == "<>" {
					two = "!="
				}
				tokens = append(tokens, catalogExpressionToken{kind: catalogTokenOperator, literal: two})
				index += 2
				continue
			}
		}
		switch r {
		case '=':
			tokens = append(tokens, catalogExpressionToken{kind: catalogTokenOperator, literal: "=="})
		case '>', '<', '+', '-', '*', '/', '!':
			tokens = append(tokens, catalogExpressionToken{kind: catalogTokenOperator, literal: string(r)})
		case '(':
			tokens = append(tokens, catalogExpressionToken{kind: catalogTokenLeftParen, literal: string(r)})
		case ')':
			tokens = append(tokens, catalogExpressionToken{kind: catalogTokenRightParen, literal: string(r)})
		case ',':
			tokens = append(tokens, catalogExpressionToken{kind: catalogTokenComma, literal: string(r)})
		default:
			return nil, qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("unexpected character %q", r))}
		}
		index++
	}
	tokens = append(tokens, catalogExpressionToken{kind: catalogTokenEOF})
	return tokens, nil
}

func lexCatalogString(raw string, start int) (string, int, qsbridge.DiagnosticSet) {
	quote := raw[start]
	var builder strings.Builder
	for index := start + 1; index < len(raw); index++ {
		current := raw[index]
		if current == quote {
			return builder.String(), index + 1, nil
		}
		if current == '\\' && index+1 < len(raw) {
			index++
			builder.WriteByte(raw[index])
			continue
		}
		builder.WriteByte(current)
	}
	return "", len(raw), qsbridge.DiagnosticSet{catalogExpressionError("unterminated string literal")}
}

func isCatalogIdentifierStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isCatalogIdentifierPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.'
}

type catalogExpressionParser struct {
	tokens []catalogExpressionToken
	index  int
}

func (p *catalogExpressionParser) parseOr() (catalogExpressionNode, qsbridge.DiagnosticSet) {
	left, diagnostics := p.parseAnd()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	for p.matchOperator("||") {
		right, diagnostics := p.parseAnd()
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		left = catalogBinaryNode{op: "||", left: left, right: right}
	}
	return left, nil
}

func (p *catalogExpressionParser) parseAnd() (catalogExpressionNode, qsbridge.DiagnosticSet) {
	left, diagnostics := p.parseComparison()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	for p.matchOperator("&&") {
		right, diagnostics := p.parseComparison()
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		left = catalogBinaryNode{op: "&&", left: left, right: right}
	}
	return left, nil
}

func (p *catalogExpressionParser) parseComparison() (catalogExpressionNode, qsbridge.DiagnosticSet) {
	left, diagnostics := p.parseAdd()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	for {
		operator := p.peek().literal
		switch operator {
		case "==", "!=", ">", ">=", "<", "<=":
			p.advance()
			right, diagnostics := p.parseAdd()
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			left = catalogBinaryNode{op: operator, left: left, right: right}
		default:
			return left, nil
		}
	}
}

func (p *catalogExpressionParser) parseAdd() (catalogExpressionNode, qsbridge.DiagnosticSet) {
	left, diagnostics := p.parseMultiply()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	for {
		if p.matchOperator("+") {
			right, diagnostics := p.parseMultiply()
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			left = catalogBinaryNode{op: "+", left: left, right: right}
			continue
		}
		if p.matchOperator("-") {
			right, diagnostics := p.parseMultiply()
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			left = catalogBinaryNode{op: "-", left: left, right: right}
			continue
		}
		return left, nil
	}
}

func (p *catalogExpressionParser) parseMultiply() (catalogExpressionNode, qsbridge.DiagnosticSet) {
	left, diagnostics := p.parseUnary()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	for {
		if p.matchOperator("*") {
			right, diagnostics := p.parseUnary()
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			left = catalogBinaryNode{op: "*", left: left, right: right}
			continue
		}
		if p.matchOperator("/") {
			right, diagnostics := p.parseUnary()
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			left = catalogBinaryNode{op: "/", left: left, right: right}
			continue
		}
		return left, nil
	}
}

func (p *catalogExpressionParser) parseUnary() (catalogExpressionNode, qsbridge.DiagnosticSet) {
	if p.matchOperator("!") {
		child, diagnostics := p.parseUnary()
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		return catalogUnaryNode{op: "!", child: child}, nil
	}
	if p.matchOperator("-") {
		child, diagnostics := p.parseUnary()
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		return catalogUnaryNode{op: "-", child: child}, nil
	}
	return p.parsePrimary()
}

func (p *catalogExpressionParser) parsePrimary() (catalogExpressionNode, qsbridge.DiagnosticSet) {
	token := p.advance()
	switch token.kind {
	case catalogTokenIdentifier:
		if p.peek().kind == catalogTokenLeftParen {
			return p.parseCall(token.literal)
		}
		path, ok := qsbridge.ParseCatalogExpressionPath(token.literal)
		if !ok {
			return nil, qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("invalid reference %q", token.literal))}
		}
		return catalogIdentifierNode{path: path}, nil
	case catalogTokenNumber:
		number, err := strconv.ParseFloat(token.literal, 64)
		if err != nil {
			return nil, qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("invalid numeric literal %q", token.literal))}
		}
		return catalogLiteralNode{value: catalogNumber(number)}, nil
	case catalogTokenString:
		return catalogLiteralNode{value: catalogString(token.literal)}, nil
	case catalogTokenBool:
		return catalogLiteralNode{value: catalogBool(token.literal == "true")}, nil
	case catalogTokenNull:
		return catalogLiteralNode{value: catalogNull()}, nil
	case catalogTokenLeftParen:
		node, diagnostics := p.parseOr()
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		if p.peek().kind != catalogTokenRightParen {
			return nil, qsbridge.DiagnosticSet{catalogExpressionError("expected right parenthesis")}
		}
		p.advance()
		return node, nil
	default:
		return nil, qsbridge.DiagnosticSet{catalogExpressionError("expected expression")}
	}
}

func (p *catalogExpressionParser) parseCall(name string) (catalogExpressionNode, qsbridge.DiagnosticSet) {
	p.advance()
	args := []catalogExpressionNode{}
	if p.peek().kind == catalogTokenRightParen {
		p.advance()
		return catalogCallNode{name: name}, nil
	}
	for {
		arg, diagnostics := p.parseOr()
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		args = append(args, arg)
		if p.peek().kind == catalogTokenComma {
			p.advance()
			continue
		}
		if p.peek().kind != catalogTokenRightParen {
			return nil, qsbridge.DiagnosticSet{catalogExpressionError("expected comma or right parenthesis in function call")}
		}
		p.advance()
		return catalogCallNode{name: name, args: args}, nil
	}
}

func (p *catalogExpressionParser) matchOperator(operator string) bool {
	if p.peek().kind == catalogTokenOperator && p.peek().literal == operator {
		p.advance()
		return true
	}
	return false
}

func (p *catalogExpressionParser) peek() catalogExpressionToken {
	if p.index >= len(p.tokens) {
		return catalogExpressionToken{kind: catalogTokenEOF}
	}
	return p.tokens[p.index]
}

func (p *catalogExpressionParser) advance() catalogExpressionToken {
	token := p.peek()
	if p.index < len(p.tokens) {
		p.index++
	}
	return token
}

type catalogExpressionNode interface {
	eval(ctx catalogExpressionEvalContext) (catalogValue, qsbridge.DiagnosticSet)
	collectDependencies(seen map[string]qsbridge.CatalogExpressionPath)
}

type catalogExpressionEvalContext struct {
	values  map[string]any
	now     func() time.Time
	purpose qsbridge.CatalogExpressionPurpose
}

type catalogLiteralNode struct {
	value catalogValue
}

func (n catalogLiteralNode) eval(catalogExpressionEvalContext) (catalogValue, qsbridge.DiagnosticSet) {
	return n.value, nil
}

func (n catalogLiteralNode) collectDependencies(map[string]qsbridge.CatalogExpressionPath) {}

type catalogIdentifierNode struct {
	path qsbridge.CatalogExpressionPath
}

func (n catalogIdentifierNode) eval(ctx catalogExpressionEvalContext) (catalogValue, qsbridge.DiagnosticSet) {
	raw, ok := lookupCatalogExpressionPath(ctx.values, n.path.Parts)
	if !ok {
		return catalogNull(), qsbridge.DiagnosticSet{
			{
				Code:     qsbridge.DiagnosticCatalogExpressionUnresolved,
				Severity: qsbridge.SeverityError,
				Phase:    qsbridge.PhaseExecute,
				Message:  fmt.Sprintf("catalog expression reference %q was not found", n.path.String()),
			},
		}
	}
	value, ok := catalogValueFromAny(raw)
	if !ok {
		return catalogNull(), qsbridge.DiagnosticSet{
			catalogExpressionError(fmt.Sprintf("catalog expression reference %q has unsupported value type %T", n.path.String(), raw)),
		}
	}
	return value, nil
}

func (n catalogIdentifierNode) collectDependencies(seen map[string]qsbridge.CatalogExpressionPath) {
	seen[n.path.String()] = n.path
}

type catalogCallNode struct {
	name string
	args []catalogExpressionNode
}

func (n catalogCallNode) eval(ctx catalogExpressionEvalContext) (catalogValue, qsbridge.DiagnosticSet) {
	args := make([]catalogValue, 0, len(n.args))
	for _, arg := range n.args {
		value, diagnostics := arg.eval(ctx)
		if diagnostics.BlocksNative() {
			return catalogNull(), diagnostics
		}
		args = append(args, value)
	}
	return evalCatalogFunction(ctx, n.name, args)
}

func (n catalogCallNode) collectDependencies(seen map[string]qsbridge.CatalogExpressionPath) {
	for _, arg := range n.args {
		arg.collectDependencies(seen)
	}
}

type catalogUnaryNode struct {
	op    string
	child catalogExpressionNode
}

func (n catalogUnaryNode) eval(ctx catalogExpressionEvalContext) (catalogValue, qsbridge.DiagnosticSet) {
	child, diagnostics := n.child.eval(ctx)
	if diagnostics.BlocksNative() {
		return catalogNull(), diagnostics
	}
	switch n.op {
	case "!":
		if child.kind != catalogValueBool {
			return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError("logical not requires bool")}
		}
		return catalogBool(!child.boolValue), nil
	case "-":
		number, ok := child.asNumber()
		if !ok {
			return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError("numeric negation requires number")}
		}
		return catalogNumber(-number), nil
	default:
		return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("unsupported unary operator %q", n.op))}
	}
}

func (n catalogUnaryNode) collectDependencies(seen map[string]qsbridge.CatalogExpressionPath) {
	n.child.collectDependencies(seen)
}

type catalogBinaryNode struct {
	op    string
	left  catalogExpressionNode
	right catalogExpressionNode
}

func (n catalogBinaryNode) eval(ctx catalogExpressionEvalContext) (catalogValue, qsbridge.DiagnosticSet) {
	if n.op == "&&" || n.op == "||" {
		return n.evalBoolean(ctx)
	}
	left, diagnostics := n.left.eval(ctx)
	if diagnostics.BlocksNative() {
		return catalogNull(), diagnostics
	}
	right, diagnostics := n.right.eval(ctx)
	if diagnostics.BlocksNative() {
		return catalogNull(), diagnostics
	}
	switch n.op {
	case "+", "-", "*", "/":
		return evalCatalogArithmetic(n.op, left, right)
	case "==", "!=", ">", ">=", "<", "<=":
		return evalCatalogComparison(n.op, left, right)
	default:
		return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("unsupported binary operator %q", n.op))}
	}
}

func (n catalogBinaryNode) evalBoolean(ctx catalogExpressionEvalContext) (catalogValue, qsbridge.DiagnosticSet) {
	left, diagnostics := n.left.eval(ctx)
	if diagnostics.BlocksNative() {
		return catalogNull(), diagnostics
	}
	if left.kind != catalogValueBool {
		return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError("logical operator requires bool")}
	}
	if n.op == "&&" && !left.boolValue {
		return catalogBool(false), nil
	}
	if n.op == "||" && left.boolValue {
		return catalogBool(true), nil
	}
	right, diagnostics := n.right.eval(ctx)
	if diagnostics.BlocksNative() {
		return catalogNull(), diagnostics
	}
	if right.kind != catalogValueBool {
		return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError("logical operator requires bool")}
	}
	if n.op == "&&" {
		return catalogBool(left.boolValue && right.boolValue), nil
	}
	return catalogBool(left.boolValue || right.boolValue), nil
}

func (n catalogBinaryNode) collectDependencies(seen map[string]qsbridge.CatalogExpressionPath) {
	n.left.collectDependencies(seen)
	n.right.collectDependencies(seen)
}

func evalCatalogArithmetic(operator string, left catalogValue, right catalogValue) (catalogValue, qsbridge.DiagnosticSet) {
	leftNumber, leftOK := left.asNumber()
	rightNumber, rightOK := right.asNumber()
	if !leftOK || !rightOK {
		return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError("arithmetic requires numeric operands")}
	}
	switch operator {
	case "+":
		return catalogNumber(leftNumber + rightNumber), nil
	case "-":
		return catalogNumber(leftNumber - rightNumber), nil
	case "*":
		return catalogNumber(leftNumber * rightNumber), nil
	case "/":
		if rightNumber == 0 {
			return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError("division by zero")}
		}
		return catalogNumber(leftNumber / rightNumber), nil
	default:
		return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("unsupported arithmetic operator %q", operator))}
	}
}

func evalCatalogFunction(ctx catalogExpressionEvalContext, name string, args []catalogValue) (catalogValue, qsbridge.DiagnosticSet) {
	if !qsbridge.IsBuiltinCatalogScalarFunction(name, ctx.purpose) {
		return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("unsupported catalog expression function %q", name))}
	}
	switch strings.ToLower(name) {
	case "now", "current_timestamp":
		if len(args) != 0 {
			return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("%s() expects 0 arguments", name))}
		}
		now := time.Now().UTC()
		if ctx.now != nil {
			now = ctx.now()
		}
		return catalogTime(now), nil
	case "unixmillis", "unix_millis":
		if len(args) != 1 {
			return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("%s() expects 1 argument", name))}
		}
		if args[0].kind != catalogValueTime {
			return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("%s() requires a time argument", name))}
		}
		return catalogNumber(float64(args[0].timeValue.UnixMilli())), nil
	default:
		result := CatalogBuiltinFunctionEvaluator{}.EvaluateFunction(qsbridge.FunctionCallRequest{
			Name:      name,
			Context:   catalogFunctionBindingContext(ctx.purpose),
			Arguments: catalogFunctionArguments(args),
		})
		if result.Diagnostics.BlocksNative() {
			return catalogNull(), result.Diagnostics
		}
		value, ok := catalogValueFromResultCell(result.Value)
		if !ok {
			return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError(fmt.Sprintf("catalog expression function %q returned an unsupported value", name))}
		}
		return value, nil
	}
}

func catalogFunctionBindingContext(purpose qsbridge.CatalogExpressionPurpose) qsbridge.FunctionBindingContext {
	if purpose == qsbridge.CatalogExpressionPurposeTableSelector {
		return qsbridge.FunctionContextTableSelector
	}
	return qsbridge.FunctionContextCatalogDefault
}

func catalogFunctionArguments(args []catalogValue) []qsbridge.ResultCell {
	arguments := make([]qsbridge.ResultCell, 0, len(args))
	for _, arg := range args {
		arguments = append(arguments, arg.resultCell())
	}
	return arguments
}

func evalCatalogComparison(operator string, left catalogValue, right catalogValue) (catalogValue, qsbridge.DiagnosticSet) {
	if left.kind == catalogValueNull || right.kind == catalogValueNull {
		switch operator {
		case "==":
			return catalogBool(left.kind == catalogValueNull && right.kind == catalogValueNull), nil
		case "!=":
			return catalogBool(left.kind != right.kind), nil
		default:
			return catalogBool(false), nil
		}
	}
	if leftNumber, leftOK := left.asNumber(); leftOK {
		if rightNumber, rightOK := right.asNumber(); rightOK {
			return compareCatalogNumbers(operator, leftNumber, rightNumber), nil
		}
	}
	if left.kind == catalogValueString && right.kind == catalogValueString {
		return compareCatalogOrdered(operator, strings.Compare(left.stringValue, right.stringValue)), nil
	}
	if left.kind == catalogValueTime && right.kind == catalogValueTime {
		return compareCatalogOrdered(operator, compareCatalogTimes(left.timeValue, right.timeValue)), nil
	}
	if left.kind == catalogValueBool && right.kind == catalogValueBool {
		switch operator {
		case "==":
			return catalogBool(left.boolValue == right.boolValue), nil
		case "!=":
			return catalogBool(left.boolValue != right.boolValue), nil
		default:
			return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError("bool operands only support equality comparison")}
		}
	}
	return catalogNull(), qsbridge.DiagnosticSet{catalogExpressionError("comparison operands have incompatible types")}
}

func compareCatalogTimes(left time.Time, right time.Time) int {
	switch {
	case left.Before(right):
		return -1
	case left.After(right):
		return 1
	default:
		return 0
	}
}

func compareCatalogNumbers(operator string, left float64, right float64) catalogValue {
	switch operator {
	case "==":
		return catalogBool(left == right)
	case "!=":
		return catalogBool(left != right)
	case ">":
		return catalogBool(left > right)
	case ">=":
		return catalogBool(left >= right)
	case "<":
		return catalogBool(left < right)
	case "<=":
		return catalogBool(left <= right)
	default:
		return catalogBool(false)
	}
}

func compareCatalogOrdered(operator string, comparison int) catalogValue {
	switch operator {
	case "==":
		return catalogBool(comparison == 0)
	case "!=":
		return catalogBool(comparison != 0)
	case ">":
		return catalogBool(comparison > 0)
	case ">=":
		return catalogBool(comparison >= 0)
	case "<":
		return catalogBool(comparison < 0)
	case "<=":
		return catalogBool(comparison <= 0)
	default:
		return catalogBool(false)
	}
}

type catalogValueKind int

const (
	catalogValueNull catalogValueKind = iota
	catalogValueBool
	catalogValueNumber
	catalogValueString
	catalogValueTime
)

type catalogValue struct {
	kind        catalogValueKind
	boolValue   bool
	numberValue float64
	stringValue string
	timeValue   time.Time
}

func catalogNull() catalogValue {
	return catalogValue{kind: catalogValueNull}
}

func catalogBool(value bool) catalogValue {
	return catalogValue{kind: catalogValueBool, boolValue: value}
}

func catalogNumber(value float64) catalogValue {
	return catalogValue{kind: catalogValueNumber, numberValue: value}
}

func catalogString(value string) catalogValue {
	return catalogValue{kind: catalogValueString, stringValue: value}
}

func catalogTime(value time.Time) catalogValue {
	return catalogValue{kind: catalogValueTime, timeValue: value}
}

func (v catalogValue) asNumber() (float64, bool) {
	if v.kind != catalogValueNumber {
		return 0, false
	}
	return v.numberValue, true
}

func (v catalogValue) resultCell() qsbridge.ResultCell {
	switch v.kind {
	case catalogValueBool:
		return qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: v.boolValue}
	case catalogValueNumber:
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: v.numberValue}
	case catalogValueString:
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: v.stringValue}
	case catalogValueTime:
		return qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: v.timeValue}
	default:
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull}
	}
}

func catalogValueFromResultCell(cell qsbridge.ResultCell) (catalogValue, bool) {
	switch cell.Kind {
	case qsbridge.ValueBool:
		value, ok := cell.Value.(bool)
		return catalogBool(value), ok
	case qsbridge.ValueInt:
		number, ok := numberFromAny(cell.Value)
		if !ok {
			return catalogNull(), false
		}
		return catalogNumber(number), true
	case qsbridge.ValueFloat:
		number, ok := numberFromAny(cell.Value)
		if !ok {
			return catalogNull(), false
		}
		return catalogNumber(number), true
	case qsbridge.ValueString:
		value, ok := cell.Value.(string)
		return catalogString(value), ok
	case qsbridge.ValueTime:
		value, ok := cell.Value.(time.Time)
		return catalogTime(value), ok
	case qsbridge.ValueNull:
		return catalogNull(), true
	default:
		return catalogNull(), false
	}
}

func catalogValueFromAny(raw any) (catalogValue, bool) {
	if raw == nil {
		return catalogNull(), true
	}
	if cell, ok := raw.(qsbridge.ResultCell); ok {
		return catalogValueFromResultCell(cell)
	}
	switch typed := raw.(type) {
	case bool:
		return catalogBool(typed), true
	case string:
		return catalogString(typed), true
	case []byte:
		return catalogString(string(typed)), true
	case time.Time:
		return catalogTime(typed), true
	default:
		number, ok := numberFromAny(raw)
		if ok {
			return catalogNumber(number), true
		}
		return catalogNull(), false
	}
}

func numberFromAny(raw any) (float64, bool) {
	switch typed := raw.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func lookupCatalogExpressionPath(values map[string]any, parts []string) (any, bool) {
	var current any = values
	for _, part := range parts {
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			current = next
		default:
			return nil, false
		}
	}
	return current, true
}

func catalogExpressionError(message string) qsbridge.Diagnostic {
	return qsbridge.Diagnostic{
		Code:     qsbridge.DiagnosticCatalogExpressionInvalid,
		Severity: qsbridge.SeverityError,
		Phase:    qsbridge.PhaseExecute,
		Message:  message,
	}
}
