package qsbridge

import "sort"

// ClientCharacterSet describes one adapter-visible character set row.
type ClientCharacterSet struct {
	Name             string
	Description      string
	DefaultCollation string
	MaxLen           int
}

// ClientCollation describes one adapter-visible collation row.
type ClientCollation struct {
	Name         string
	CharacterSet string
	ID           int
	Default      bool
	Compiled     bool
	SortLen      int
}

// ClientCharsetSummaryRow describes aggregate character set and collation metadata.
type ClientCharsetSummaryRow struct {
	CharacterSetCount      int
	CollationCount         int
	DefaultCollationCount  int
	CompiledCollationCount int
	MultiByteCharsetCount  int
	ZeroSortLenCount       int
}

// ClientCharacterSetExchange is adapter-facing character set metadata.
type ClientCharacterSetExchange struct {
	Connection    ConnectionContext
	Pattern       string
	CharacterSets []ClientCharacterSet
	Result        ExecutionResult
	ResultSchema  ProtocolResultSchema
	Diagnostics   DiagnosticSet
}

// ClientCollationExchange is adapter-facing collation metadata.
type ClientCollationExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Collations   []ClientCollation
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCharacterSets returns SHOW CHARACTER SET-style metadata supplied by the adapter.
func (s PlanningService) ListClientCharacterSets(connection ConnectionContext, characterSets []ClientCharacterSet, pattern string) ClientCharacterSetExchange {
	_ = s
	exchange := ClientCharacterSetExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.CharacterSets = filterClientCharacterSets(characterSets, pattern)
	}
	exchange.Result = exchange.characterSetResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// ListClientCollations returns SHOW COLLATION-style metadata supplied by the adapter.
func (s PlanningService) ListClientCollations(connection ConnectionContext, collations []ClientCollation, pattern string) ClientCollationExchange {
	_ = s
	exchange := ClientCollationExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Collations = filterClientCollations(collations, pattern)
	}
	exchange.Result = exchange.collationResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether character set metadata can be returned.
func (e ClientCharacterSetExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts character set diagnostics into protocol-facing errors.
func (e ClientCharacterSetExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking character set error, if any.
func (e ClientCharacterSetExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

// Supported reports whether collation metadata can be returned.
func (e ClientCollationExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts collation diagnostics into protocol-facing errors.
func (e ClientCollationExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking collation error, if any.
func (e ClientCollationExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCharacterSetExchange) characterSetResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     characterSetResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.characterSetRows(),
		Final: true,
	})
}

func characterSetResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Charset", Type: DataTypeString},
		{Name: "Description", Type: DataTypeString, Nullable: true},
		{Name: "Default collation", Type: DataTypeString, Nullable: true},
		{Name: "Maxlen", Type: DataTypeInt},
	}
}

func (e ClientCharacterSetExchange) characterSetRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.CharacterSets))
	for _, charset := range e.CharacterSets {
		rows = append(rows, ResultRow{
			metadataStringCell(charset.Name),
			metadataStringCell(charset.Description),
			metadataStringCell(charset.DefaultCollation),
			metadataIntCell(charset.MaxLen),
		})
	}
	return rows
}

func (e ClientCollationExchange) collationResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     collationResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.collationRows(),
		Final: true,
	})
}

func collationResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Collation", Type: DataTypeString},
		{Name: "Charset", Type: DataTypeString},
		{Name: "Id", Type: DataTypeInt},
		{Name: "Default", Type: DataTypeBool},
		{Name: "Compiled", Type: DataTypeBool},
		{Name: "Sortlen", Type: DataTypeInt},
	}
}

func (e ClientCollationExchange) collationRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Collations))
	for _, collation := range e.Collations {
		rows = append(rows, ResultRow{
			metadataStringCell(collation.Name),
			metadataStringCell(collation.CharacterSet),
			metadataIntCell(collation.ID),
			metadataBoolCell(collation.Default),
			metadataBoolCell(collation.Compiled),
			metadataIntCell(collation.SortLen),
		})
	}
	return rows
}

func filterClientCharacterSets(characterSets []ClientCharacterSet, pattern string) []ClientCharacterSet {
	cloned := cloneClientCharacterSets(characterSets)
	sort.Slice(cloned, func(i, j int) bool {
		return cloned[i].Name < cloned[j].Name
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]ClientCharacterSet, 0, len(cloned))
	for _, charset := range cloned {
		if catalogFieldPatternMatch(pattern, charset.Name) {
			filtered = append(filtered, charset)
		}
	}
	return filtered
}

func filterClientCollations(collations []ClientCollation, pattern string) []ClientCollation {
	cloned := cloneClientCollations(collations)
	sort.Slice(cloned, func(i, j int) bool {
		return cloned[i].Name < cloned[j].Name
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]ClientCollation, 0, len(cloned))
	for _, collation := range cloned {
		if catalogFieldPatternMatch(pattern, collation.Name) || catalogFieldPatternMatch(pattern, collation.CharacterSet) {
			filtered = append(filtered, collation)
		}
	}
	return filtered
}

func cloneClientCharacterSets(characterSets []ClientCharacterSet) []ClientCharacterSet {
	if len(characterSets) == 0 {
		return nil
	}
	return append([]ClientCharacterSet(nil), characterSets...)
}

func cloneClientCollations(collations []ClientCollation) []ClientCollation {
	if len(collations) == 0 {
		return nil
	}
	return append([]ClientCollation(nil), collations...)
}

func summarizeClientCharsetMetadata(characterSets []ClientCharacterSet, collations []ClientCollation) ClientCharsetSummaryRow {
	summary := ClientCharsetSummaryRow{
		CharacterSetCount: len(characterSets),
		CollationCount:    len(collations),
	}
	for _, charset := range characterSets {
		if charset.MaxLen > 1 {
			summary.MultiByteCharsetCount++
		}
	}
	for _, collation := range collations {
		if collation.Default {
			summary.DefaultCollationCount++
		}
		if collation.Compiled {
			summary.CompiledCollationCount++
		}
		if collation.SortLen == 0 {
			summary.ZeroSortLenCount++
		}
	}
	return summary
}
