package core

import (
	"testing"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

func TestExpandNestedIngestRecordsBuildsOrderLineitemChildren(t *testing.T) {
	table := &Table{
		BasicTable: &shared.BasicTable{Name: "orders"},
		Attributes: []Attribute{
			{
				BasicAttribute: &shared.BasicAttribute{
					FieldName:       "lineitems",
					SourceName:      "lineitems",
					MappingStrategy: "ChildRelation",
					ChildTable:      "lineitem",
				},
			},
		},
	}
	parent := IngestRecord{
		TableName: "orders",
		ShardKey:  "dedup:tpch:evt-1",
		EventID:   "evt-1",
		Source:    "tpch",
		Data: map[string]interface{}{
			"o_orderkey": 1001,
			"lineitems": []interface{}{
				map[string]interface{}{"l_linenumber": 1, "l_quantity": 3},
				map[string]interface{}{"l_linenumber": 2, "l_quantity": 5},
			},
		},
	}

	result, err := ExpandNestedIngestRecords(NestedIngestExpansionRequest{
		Parent: parent,
		Table:  table,
	})

	require.NoError(t, err)
	require.Equal(t, parent, result.Parent)
	require.Len(t, result.Children, 2)
	require.Equal(t, "lineitem", result.Children[0].TableName)
	require.Equal(t, parent.ShardKey, result.Children[0].ShardKey)
	require.Equal(t, parent.EventID, result.Children[0].EventID)
	require.Equal(t, 1001, result.Children[0].Data["o_orderkey"])
	require.Equal(t, map[string]interface{}{"l_linenumber": 1, "l_quantity": 3}, result.Children[0].Data["lineitems"])
	require.Equal(t, map[string]interface{}{"l_linenumber": 2, "l_quantity": 5}, result.Children[1].Data["lineitems"])
	require.IsType(t, []interface{}{}, parent.Data["lineitems"])
}

func TestExpandNestedIngestRecordsIgnoresMissingOrNonArrayChildren(t *testing.T) {
	table := &Table{
		BasicTable: &shared.BasicTable{Name: "orders"},
		Attributes: []Attribute{
			{
				BasicAttribute: &shared.BasicAttribute{
					FieldName:       "lineitems",
					SourceName:      "lineitems",
					MappingStrategy: "ChildRelation",
					ChildTable:      "lineitem",
				},
			},
		},
	}

	result, err := ExpandNestedIngestRecords(NestedIngestExpansionRequest{
		Parent: IngestRecord{TableName: "orders", Data: map[string]interface{}{"lineitems": "not-array"}},
		Table:  table,
	})

	require.NoError(t, err)
	require.Empty(t, result.Children)
}
