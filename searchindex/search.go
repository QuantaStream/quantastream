package searchindex

import "github.com/aviddiviner/go-murmur"

// HashFieldPrefix marks the hidden BSI field that maps rownums to searchable
// string hashes for MATCH ... AGAINST pushdown.
const HashFieldPrefix = "__qs_search_hash__"

// HashFieldName returns the hidden field paired with a searchable logical field.
func HashFieldName(field string) string {
	return HashFieldPrefix + field
}

// HashValue returns the stable hash key used by StringSearch and its row artifact.
func HashValue(value string) uint64 {
	return uint64(murmur.MurmurHash2([]byte(value), 0))
}
