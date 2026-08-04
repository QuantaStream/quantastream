package core

import (
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsexpr"
)

// IngestSelectorRequest describes an envelope/payload pair that should be
// routed to the first table whose selector evaluates true.
type IngestSelectorRequest struct {
	Tables   []*Table
	Envelope map[string]interface{}
	Payload  map[string]interface{}
}

// IngestSelectorResult is the selector-routing decision for a parsed stream
// record.
type IngestSelectorResult struct {
	Matched   bool
	TableName string
	Table     *Table
	Evaluated int
}

// SelectIngestTable evaluates non-empty table selectors in request order and
// returns the first matching table.
func SelectIngestTable(request IngestSelectorRequest) (IngestSelectorResult, qsbridge.DiagnosticSet) {
	context := BuildIngestSelectorContext(request.Envelope, request.Payload)
	result := IngestSelectorResult{}
	for _, table := range request.Tables {
		if table == nil || strings.TrimSpace(table.Selector) == "" {
			continue
		}
		result.Evaluated++
		evaluator := qsexpr.CatalogExpressionEvaluator{}
		if table.SelectorNode != nil {
			evaluator = *table.SelectorNode
		}
		matched, diagnostics := evaluator.EvaluateSelector(qsbridge.TableSelectorExpression(table.Selector), context)
		if diagnostics.BlocksNative() {
			return result, diagnostics
		}
		if matched {
			result.Matched = true
			result.TableName = table.Name
			result.Table = table
			return result, nil
		}
	}
	return result, nil
}

// BuildIngestSelectorContext exposes envelope and payload through explicit
// roots while preserving bare-field access for existing selector expressions.
// Payload fields win top-level name conflicts; envelope and payload roots are
// always reserved for disambiguation.
func BuildIngestSelectorContext(envelope, payload map[string]interface{}) map[string]interface{} {
	context := make(map[string]interface{}, len(envelope)+len(payload)+2)
	for key, value := range envelope {
		context[key] = value
	}
	for key, value := range payload {
		context[key] = value
	}
	if envelope == nil {
		envelope = map[string]interface{}{}
	}
	if payload == nil {
		payload = map[string]interface{}{}
	}
	context["envelope"] = envelope
	context["payload"] = payload
	return context
}
