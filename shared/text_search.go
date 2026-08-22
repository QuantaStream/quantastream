package shared

import (
	"strings"

	"github.com/QuantaStream/quantastream/searchindex"
)

// EnsureSearchHashAttributes adds hidden BSI fields used by MATCH ... AGAINST.
// User writes still map the declared searchable text field; this companion field
// stores stable search-token hashes keyed by rownum for native pushdown.
func EnsureSearchHashAttributes(table *BasicTable) bool {
	if table == nil {
		return false
	}
	existing := make(map[string]struct{}, len(table.Attributes))
	for _, attr := range table.Attributes {
		if strings.TrimSpace(attr.FieldName) != "" {
			existing[strings.ToLower(strings.TrimSpace(attr.FieldName))] = struct{}{}
		}
	}
	added := false
	for _, attr := range table.Attributes {
		if !attr.Searchable || !strings.EqualFold(strings.TrimSpace(attr.Type), "String") {
			continue
		}
		fieldName := strings.TrimSpace(attr.FieldName)
		if fieldName == "" {
			fieldName = strings.TrimSpace(attr.SourceName)
		}
		if fieldName == "" {
			continue
		}
		hashField := searchindex.HashFieldName(fieldName)
		if _, ok := existing[strings.ToLower(hashField)]; ok {
			continue
		}
		table.Attributes = append(table.Attributes, BasicAttribute{
			FieldName:       hashField,
			Type:            "Integer",
			MappingStrategy: "IntBSI",
			Desc:            "QuantaStream text-search hash for " + fieldName,
			System:          true,
		})
		existing[strings.ToLower(hashField)] = struct{}{}
		added = true
	}
	return added
}
