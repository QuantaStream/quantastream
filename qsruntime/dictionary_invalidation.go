package qsruntime

import (
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// RuntimeDictionaryInvalidator centralizes KVStore-backed dictionary cache invalidation.
//
// StringEnum dictionaries are persisted through KVStore/pogreb, not Consul
// schema metadata. Writer paths should allocate or retrieve the enum ID through
// the legacy StringEnum KVStore API, update their local table-cache maps, then
// invalidate the process-local dictionary resolver entry for the touched field.
type RuntimeDictionaryInvalidator struct {
	Dictionaries  DictionaryInvalidationTarget
	DefaultSchema string
}

// DictionaryInvalidationTarget is the narrow cache hook required for StringEnum dictionaries.
type DictionaryInvalidationTarget interface {
	InvalidateDictionary(ref qsbridge.DictionaryRef)
}

// DictionaryValueChange records a KVStore-backed dictionary value update.
type DictionaryValueChange struct {
	Schema string
	Table  string
	Field  string
	Label  string
	ID     qsbridge.StringEnumID
}

// InvalidateValueChange evicts cached lookups for one dictionary field.
func (i RuntimeDictionaryInvalidator) InvalidateValueChange(change DictionaryValueChange) {
	if i.Dictionaries == nil {
		return
	}
	ref := change.DictionaryRef(i.DefaultSchema)
	if ref.Table == "" || ref.Field == "" {
		return
	}
	i.Dictionaries.InvalidateDictionary(ref)
}

// DictionaryRef returns the field-level dictionary identity touched by change.
func (c DictionaryValueChange) DictionaryRef(defaultSchema string) qsbridge.DictionaryRef {
	schema := strings.TrimSpace(c.Schema)
	if schema == "" {
		schema = strings.TrimSpace(defaultSchema)
	}
	return qsbridge.DictionaryRef{
		Schema: schema,
		Table:  strings.TrimSpace(c.Table),
		Field:  strings.TrimSpace(c.Field),
	}
}
