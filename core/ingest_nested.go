package core

import (
	"fmt"

	"github.com/QuantaStream/quantastream/shared"
)

// NestedIngestExpansionRequest describes a parent ingest record whose payload
// may include child arrays defined by ChildRelation catalog attributes.
type NestedIngestExpansionRequest struct {
	Parent IngestRecord
	Table  *Table
}

// NestedIngestExpansionResult contains the parent record and any child records
// derived from nested payload arrays.
type NestedIngestExpansionResult struct {
	Parent   IngestRecord
	Children []IngestRecord
}

// ExpandNestedIngestRecords mirrors the current PutRow child-array convention
// without calling PutRow. It is a harness for proving parent/child payload shape.
func ExpandNestedIngestRecords(request NestedIngestExpansionRequest) (NestedIngestExpansionResult, error) {
	if request.Table == nil {
		return NestedIngestExpansionResult{}, fmt.Errorf("nested ingest table is required")
	}
	if request.Parent.Data == nil {
		return NestedIngestExpansionResult{}, fmt.Errorf("nested ingest parent payload is required")
	}
	result := NestedIngestExpansionResult{Parent: request.Parent}
	for _, attr := range request.Table.Attributes {
		if attr.MappingStrategy != "ChildRelation" || attr.ChildTable == "" {
			continue
		}
		path := firstNonEmpty(attr.SourceName, attr.FieldName)
		value, err := shared.GetPath(path, request.Parent.Data, false, false)
		if err != nil {
			continue
		}
		childRows, ok := value.([]interface{})
		if !ok {
			continue
		}
		for _, childRow := range childRows {
			childPayload := cloneIngestPayload(request.Parent.Data)
			childPayload[path] = childRow
			child := request.Parent
			child.TableName = attr.ChildTable
			child.Data = childPayload
			result.Children = append(result.Children, child)
		}
	}
	return result, nil
}

func cloneIngestPayload(payload map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}
