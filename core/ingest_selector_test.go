package core

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsexpr"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

func TestBuildIngestSelectorContextExposesEnvelopePayloadAndBareFields(t *testing.T) {
	context := BuildIngestSelectorContext(
		map[string]interface{}{
			"source": "tpch-stream",
			"kind":   "envelope-kind",
		},
		map[string]interface{}{
			"kind":  "order",
			"order": map[string]interface{}{"id": 1001},
		},
	)

	require.Equal(t, "order", context["kind"])
	require.Equal(t, "tpch-stream", context["source"])
	require.Equal(t, "tpch-stream", context["envelope"].(map[string]interface{})["source"])
	require.Equal(t, "order", context["payload"].(map[string]interface{})["kind"])
}

func TestSelectIngestTableReturnsFirstMatchingSelector(t *testing.T) {
	customers := ingestSelectorTable("customers", `payload.kind = "customer"`)
	orders := ingestSelectorTable("orders", `payload.kind = "order" && envelope.source = "tpch-stream"`)
	lineitems := ingestSelectorTable("lineitem", `payload.kind = "lineitem"`)

	result, diagnostics := SelectIngestTable(IngestSelectorRequest{
		Tables: []*Table{
			nil,
			ingestSelectorTable("ignored", ""),
			customers,
			orders,
			lineitems,
		},
		Envelope: map[string]interface{}{"source": "tpch-stream"},
		Payload:  map[string]interface{}{"kind": "order"},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.True(t, result.Matched)
	require.Equal(t, "orders", result.TableName)
	require.Same(t, orders, result.Table)
	require.Equal(t, 2, result.Evaluated)
}

func TestSelectIngestTableSupportsBarePayloadSelector(t *testing.T) {
	result, diagnostics := SelectIngestTable(IngestSelectorRequest{
		Tables: []*Table{
			ingestSelectorTable("cities", `type = "cities"`),
		},
		Payload: map[string]interface{}{"type": "cities"},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.True(t, result.Matched)
	require.Equal(t, "cities", result.TableName)
}

func TestSelectIngestTableUsesCompiledSelectorNode(t *testing.T) {
	orders := ingestSelectorTable("orders", `payload.kind = "order"`)
	selector, diagnostics := qsexpr.CompileSelectorExpression(qsbridge.TableSelectorExpression(orders.Selector))
	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	orders.SelectorNode = selector

	result, diagnostics := SelectIngestTable(IngestSelectorRequest{
		Tables:  []*Table{orders},
		Payload: map[string]interface{}{"kind": "order"},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.True(t, result.Matched)
	require.Equal(t, "orders", result.TableName)
}

func TestSelectIngestTableReturnsFalseWhenNoSelectorMatches(t *testing.T) {
	result, diagnostics := SelectIngestTable(IngestSelectorRequest{
		Tables: []*Table{
			ingestSelectorTable("customers", `payload.kind = "customer"`),
			ingestSelectorTable("orders", `payload.kind = "order"`),
		},
		Payload: map[string]interface{}{"kind": "lineitem"},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.False(t, result.Matched)
	require.Empty(t, result.TableName)
	require.Equal(t, 2, result.Evaluated)
}

func TestSelectIngestTableReturnsDiagnosticsForInvalidSelector(t *testing.T) {
	result, diagnostics := SelectIngestTable(IngestSelectorRequest{
		Tables: []*Table{
			ingestSelectorTable("orders", "now() != null"),
		},
		Payload: map[string]interface{}{"kind": "order"},
	})

	require.True(t, diagnostics.BlocksNative())
	require.False(t, result.Matched)
	require.Equal(t, 1, result.Evaluated)
}

func ingestSelectorTable(name, selector string) *Table {
	return &Table{BasicTable: &shared.BasicTable{Name: name, Selector: selector}}
}
