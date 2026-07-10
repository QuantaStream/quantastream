package qsbridge

import "strconv"

// ValueKind classifies literal values before physical type binding.
type ValueKind string

const (
	// ValueUnknown means the literal value has not been classified.
	ValueUnknown ValueKind = ""
	// ValueNull identifies a SQL NULL literal.
	ValueNull ValueKind = "null"
	// ValueBool identifies a boolean literal.
	ValueBool ValueKind = "bool"
	// ValueInt identifies an integer literal.
	ValueInt ValueKind = "int"
	// ValueFloat identifies a floating-point or decimal literal.
	ValueFloat ValueKind = "float"
	// ValueString identifies a string literal.
	ValueString ValueKind = "string"
	// ValueTime identifies a date, time, or timestamp literal.
	ValueTime ValueKind = "time"
)

// LiteralExpr is a scalar literal in the native expression tree.
type LiteralExpr struct {
	Kind  ValueKind
	Value any
}

// ExpressionKind reports that LiteralExpr is a literal expression.
func (LiteralExpr) ExpressionKind() ExprKind {
	return ExprLiteral
}

// Literal creates a literal expression.
func Literal(kind ValueKind, value any) LiteralExpr {
	return LiteralExpr{Kind: kind, Value: value}
}

// FieldExpr references one resolved physical field.
type FieldExpr struct {
	Ref FieldRef
}

// ExpressionKind reports that FieldExpr is a field expression.
func (FieldExpr) ExpressionKind() ExprKind {
	return ExprField
}

// Field creates a field reference expression.
func Field(ref FieldRef) FieldExpr {
	return FieldExpr{Ref: ref}
}

// ParameterExpr references one prepared-statement placeholder.
type ParameterExpr struct {
	Ref ParameterRef
}

// ExpressionKind reports that ParameterExpr is a parameter expression.
func (ParameterExpr) ExpressionKind() ExprKind {
	return ExprParameter
}

// Parameter creates a prepared-statement parameter expression.
func Parameter(index int, dataType DataType) ParameterExpr {
	return ParameterExpr{Ref: ParameterRef{Index: index, Type: dataType, Nullable: true}}
}

// ListExpr stores a scalar expression list such as the right side of IN.
type ListExpr struct {
	Items []Expr
}

// ExpressionKind reports that ListExpr is an expression list.
func (ListExpr) ExpressionKind() ExprKind {
	return ExprList
}

// List creates a scalar expression list and copies the supplied item slice.
func List(items ...Expr) ListExpr {
	return ListExpr{Items: append([]Expr(nil), items...)}
}

// CallExpr is a scalar function call.
type CallExpr struct {
	Name          string
	Args          []Expr
	Type          DataType
	Origin        FunctionOrigin
	Placement     FunctionPlacement
	Deterministic bool
}

// ExpressionKind reports that CallExpr is a function-call expression.
func (CallExpr) ExpressionKind() ExprKind {
	return ExprCall
}

// Call creates a scalar function-call expression.
func Call(name string, args ...Expr) CallExpr {
	copied := append([]Expr(nil), args...)
	return CallExpr{Name: name, Args: copied}
}

// TypedCall creates a scalar function-call expression with bound return type metadata.
func TypedCall(name string, returnType DataType, args ...Expr) CallExpr {
	call := Call(name, args...)
	call.Type = returnType
	return call
}

// FunctionCall creates a scalar function-call expression from resolved catalog metadata.
func FunctionCall(function FunctionDefinition, args ...Expr) CallExpr {
	call := TypedCall(function.Name, function.ReturnType, args...)
	call.Origin = function.Origin
	call.Placement = function.EffectivePlacement()
	call.Deterministic = function.Deterministic
	return call
}

// BinaryOp names a binary SQL operator after parsing.
type BinaryOp string

const (
	// BinaryOpEqual is the equality operator.
	BinaryOpEqual BinaryOp = "="
	// BinaryOpNotEqual is the inequality operator.
	BinaryOpNotEqual BinaryOp = "!="
	// BinaryOpLess is the less-than operator.
	BinaryOpLess BinaryOp = "<"
	// BinaryOpLessEqual is the less-than-or-equal operator.
	BinaryOpLessEqual BinaryOp = "<="
	// BinaryOpGreater is the greater-than operator.
	BinaryOpGreater BinaryOp = ">"
	// BinaryOpGreaterEqual is the greater-than-or-equal operator.
	BinaryOpGreaterEqual BinaryOp = ">="
	// BinaryOpAnd is the boolean AND operator.
	BinaryOpAnd BinaryOp = "and"
	// BinaryOpOr is the boolean OR operator.
	BinaryOpOr BinaryOp = "or"
	// BinaryOpLike is the SQL LIKE operator.
	BinaryOpLike BinaryOp = "like"
	// BinaryOpNotLike is the SQL NOT LIKE operator.
	BinaryOpNotLike BinaryOp = "not like"
	// BinaryOpIn is the SQL IN operator.
	BinaryOpIn BinaryOp = "in"
	// BinaryOpNotIn is the SQL NOT IN operator.
	BinaryOpNotIn BinaryOp = "not in"
	// BinaryOpBetween is the SQL BETWEEN range operator.
	BinaryOpBetween BinaryOp = "between"
	// BinaryOpNotBetween is the SQL NOT BETWEEN range operator.
	BinaryOpNotBetween BinaryOp = "not between"
	// BinaryOpAdd is the numeric addition operator.
	BinaryOpAdd BinaryOp = "+"
	// BinaryOpSubtract is the numeric subtraction operator.
	BinaryOpSubtract BinaryOp = "-"
	// BinaryOpMultiply is the numeric multiplication operator.
	BinaryOpMultiply BinaryOp = "*"
	// BinaryOpDivide is the numeric division operator.
	BinaryOpDivide BinaryOp = "/"
)

// BinaryExpr combines two scalar expressions with a binary operator.
type BinaryExpr struct {
	Op    BinaryOp
	Left  Expr
	Right Expr
}

// ExpressionKind reports that BinaryExpr is a binary expression.
func (BinaryExpr) ExpressionKind() ExprKind {
	return ExprBinary
}

// Binary creates a binary expression.
func Binary(op BinaryOp, left Expr, right Expr) BinaryExpr {
	return BinaryExpr{Op: op, Left: left, Right: right}
}

// SearchedCaseWhen stores one WHEN condition and THEN result expression.
type SearchedCaseWhen struct {
	Condition Expr
	Result    Expr
}

// SearchedCaseExpr is a searched SQL CASE expression.
type SearchedCaseExpr struct {
	Whens []SearchedCaseWhen
	Else  Expr
	Type  DataType
}

// ExpressionKind reports that SearchedCaseExpr is a searched CASE expression.
func (SearchedCaseExpr) ExpressionKind() ExprKind {
	return ExprSearchedCase
}

// SearchedCase creates a searched CASE expression and copies the supplied WHEN arms.
func SearchedCase(whens []SearchedCaseWhen, elseExpr Expr) SearchedCaseExpr {
	copied := append([]SearchedCaseWhen(nil), whens...)
	return SearchedCaseExpr{Whens: copied, Else: elseExpr, Type: searchedCaseDataType(copied, elseExpr)}
}

// AggregateRefExpr references an already-computed aggregate slot.
type AggregateRefExpr struct {
	Alias string
	Index int
	Type  DataType
}

// ExpressionKind reports that AggregateRefExpr is an aggregate reference.
func (AggregateRefExpr) ExpressionKind() ExprKind {
	return ExprAggregateRef
}

// AggregateRef creates an aggregate-slot reference expression.
func AggregateRef(alias string, index int) AggregateRefExpr {
	return AggregateRefExpr{Alias: alias, Index: index}
}

// TypedAggregateRef creates an aggregate-slot reference with bound result type metadata.
func TypedAggregateRef(alias string, index int, resultType DataType) AggregateRefExpr {
	ref := AggregateRef(alias, index)
	ref.Type = resultType
	return ref
}

// ExprDataType returns the SQL-facing data type carried by a bound expression.
func ExprDataType(expr Expr) DataType {
	switch n := expr.(type) {
	case nil:
		return DataTypeUnknown
	case LiteralExpr:
		return literalDataType(n.Kind)
	case *LiteralExpr:
		if n != nil {
			return literalDataType(n.Kind)
		}
	case FieldExpr:
		return n.Ref.Type
	case *FieldExpr:
		if n != nil {
			return n.Ref.Type
		}
	case ParameterExpr:
		return n.Ref.Type
	case *ParameterExpr:
		if n != nil {
			return n.Ref.Type
		}
	case ListExpr:
		return DataTypeUnknown
	case *ListExpr:
		return DataTypeUnknown
	case CallExpr:
		return n.Type
	case *CallExpr:
		if n != nil {
			return n.Type
		}
	case BinaryExpr:
		return binaryDataType(n.Op)
	case *BinaryExpr:
		if n != nil {
			return binaryDataType(n.Op)
		}
	case SearchedCaseExpr:
		return n.Type
	case *SearchedCaseExpr:
		if n != nil {
			return n.Type
		}
	case AggregateRefExpr:
		return n.Type
	case *AggregateRefExpr:
		if n != nil {
			return n.Type
		}
	}
	return DataTypeUnknown
}

// ExprNullable reports whether an expression may produce SQL NULL.
func ExprNullable(expr Expr) bool {
	switch n := expr.(type) {
	case nil:
		return true
	case LiteralExpr:
		return n.Kind == ValueNull
	case *LiteralExpr:
		return n == nil || n.Kind == ValueNull
	case FieldExpr:
		return n.Ref.Nullable
	case *FieldExpr:
		return n == nil || n.Ref.Nullable
	case ParameterExpr:
		return n.Ref.Nullable
	case *ParameterExpr:
		return n == nil || n.Ref.Nullable
	case SearchedCaseExpr:
		return true
	case *SearchedCaseExpr:
		return true
	}
	return true
}

// literalDataType maps literal kinds to SQL-facing data types.
func literalDataType(kind ValueKind) DataType {
	switch kind {
	case ValueBool:
		return DataTypeBool
	case ValueInt:
		return DataTypeInt
	case ValueFloat:
		return DataTypeFloat
	case ValueString:
		return DataTypeString
	case ValueTime:
		return DataTypeTime
	default:
		return DataTypeUnknown
	}
}

// binaryDataType maps predicate operators to boolean result metadata.
func binaryDataType(op BinaryOp) DataType {
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual, BinaryOpLess, BinaryOpLessEqual, BinaryOpGreater, BinaryOpGreaterEqual, BinaryOpAnd, BinaryOpOr, BinaryOpLike, BinaryOpNotLike, BinaryOpIn, BinaryOpNotIn:
		return DataTypeBool
	case BinaryOpAdd, BinaryOpSubtract, BinaryOpMultiply, BinaryOpDivide:
		return DataTypeFloat
	default:
		return DataTypeUnknown
	}
}

// FieldRefs returns the field references used by expr in first-seen order.
func FieldRefs(expr Expr) []FieldRef {
	refs := make([]FieldRef, 0)
	seen := make(map[string]struct{})
	collectFieldRefs(expr, seen, &refs)
	return refs
}

func collectFieldRefs(expr Expr, seen map[string]struct{}, refs *[]FieldRef) {
	switch n := expr.(type) {
	case nil:
		return
	case FieldExpr:
		appendFieldRef(n.Ref, seen, refs)
	case *FieldExpr:
		if n != nil {
			appendFieldRef(n.Ref, seen, refs)
		}
	case ListExpr:
		for _, item := range n.Items {
			collectFieldRefs(item, seen, refs)
		}
	case *ListExpr:
		if n != nil {
			for _, item := range n.Items {
				collectFieldRefs(item, seen, refs)
			}
		}
	case CallExpr:
		for _, arg := range n.Args {
			collectFieldRefs(arg, seen, refs)
		}
	case *CallExpr:
		if n != nil {
			for _, arg := range n.Args {
				collectFieldRefs(arg, seen, refs)
			}
		}
	case BinaryExpr:
		collectFieldRefs(n.Left, seen, refs)
		collectFieldRefs(n.Right, seen, refs)
	case *BinaryExpr:
		if n != nil {
			collectFieldRefs(n.Left, seen, refs)
			collectFieldRefs(n.Right, seen, refs)
		}
	case SearchedCaseExpr:
		for _, when := range n.Whens {
			collectFieldRefs(when.Condition, seen, refs)
			collectFieldRefs(when.Result, seen, refs)
		}
		collectFieldRefs(n.Else, seen, refs)
	case *SearchedCaseExpr:
		if n != nil {
			for _, when := range n.Whens {
				collectFieldRefs(when.Condition, seen, refs)
				collectFieldRefs(when.Result, seen, refs)
			}
			collectFieldRefs(n.Else, seen, refs)
		}
	}
}

// ParameterRefs returns parameter references used by expr in first-seen order.
func ParameterRefs(expr Expr) []ParameterRef {
	refs := make([]ParameterRef, 0)
	seen := make(map[string]struct{})
	collectParameterRefs(expr, seen, &refs)
	return refs
}

func collectParameterRefs(expr Expr, seen map[string]struct{}, refs *[]ParameterRef) {
	switch n := expr.(type) {
	case nil:
		return
	case ParameterExpr:
		appendParameterRef(n.Ref, seen, refs)
	case *ParameterExpr:
		if n != nil {
			appendParameterRef(n.Ref, seen, refs)
		}
	case ListExpr:
		for _, item := range n.Items {
			collectParameterRefs(item, seen, refs)
		}
	case *ListExpr:
		if n != nil {
			for _, item := range n.Items {
				collectParameterRefs(item, seen, refs)
			}
		}
	case CallExpr:
		for _, arg := range n.Args {
			collectParameterRefs(arg, seen, refs)
		}
	case *CallExpr:
		if n != nil {
			for _, arg := range n.Args {
				collectParameterRefs(arg, seen, refs)
			}
		}
	case BinaryExpr:
		collectParameterRefs(n.Left, seen, refs)
		collectParameterRefs(n.Right, seen, refs)
	case *BinaryExpr:
		if n != nil {
			collectParameterRefs(n.Left, seen, refs)
			collectParameterRefs(n.Right, seen, refs)
		}
	case SearchedCaseExpr:
		for _, when := range n.Whens {
			collectParameterRefs(when.Condition, seen, refs)
			collectParameterRefs(when.Result, seen, refs)
		}
		collectParameterRefs(n.Else, seen, refs)
	case *SearchedCaseExpr:
		if n != nil {
			for _, when := range n.Whens {
				collectParameterRefs(when.Condition, seen, refs)
				collectParameterRefs(when.Result, seen, refs)
			}
			collectParameterRefs(n.Else, seen, refs)
		}
	}
}

func searchedCaseDataType(whens []SearchedCaseWhen, elseExpr Expr) DataType {
	dataType := DataTypeUnknown
	for _, when := range whens {
		dataType = mergeSearchedCaseDataType(dataType, ExprDataType(when.Result))
	}
	if elseExpr != nil {
		dataType = mergeSearchedCaseDataType(dataType, ExprDataType(elseExpr))
	}
	return dataType
}

func mergeSearchedCaseDataType(current DataType, next DataType) DataType {
	if next == DataTypeUnknown {
		return current
	}
	if current == DataTypeUnknown || current == next {
		return next
	}
	if (current == DataTypeInt && next == DataTypeFloat) || (current == DataTypeFloat && next == DataTypeInt) {
		return DataTypeFloat
	}
	return DataTypeUnknown
}

func appendFieldRef(ref FieldRef, seen map[string]struct{}, refs *[]FieldRef) {
	key := string(ref.Table.ID) + "\x00" + ref.Name + "\x00" + ref.PhysicalName
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	*refs = append(*refs, ref)
}

func appendParameterRef(ref ParameterRef, seen map[string]struct{}, refs *[]ParameterRef) {
	key := parameterRefKey(ref)
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	*refs = append(*refs, ref)
}

func parameterRefKey(ref ParameterRef) string {
	if ref.Name != "" {
		return "name:" + ref.Name
	}
	return "index:" + strconv.Itoa(ref.Index)
}
