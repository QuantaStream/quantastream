package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestRuntimeMetadataInvalidatorAppliesMetadataChange(t *testing.T) {
	catalog := &recordingInvalidatableCatalog{}
	tables := &recordingTableInvalidationTarget{}
	invalidator := RuntimeMetadataInvalidator{
		Catalog:       catalog,
		Tables:        tables,
		DefaultSchema: "quanta",
	}

	invalidator.ApplyChange(MetadataChangeEvent{Table: "customers_qa", Kind: MetadataChangeModify})

	if len(catalog.tables) != 2 || catalog.tables[0] != "quanta.customers_qa" || catalog.tables[1] != ".customers_qa" {
		t.Fatalf("invalidated tables = %#v, want schema-qualified and unqualified customers_qa", catalog.tables)
	}
	if len(tables.tables) != 1 || tables.tables[0] != "customers_qa" {
		t.Fatalf("runtime invalidated tables = %#v, want customers_qa", tables.tables)
	}
}

func TestRuntimeDictionaryInvalidatorInvalidatesStringEnumField(t *testing.T) {
	dictionaries := &recordingDictionaryCache{}
	invalidator := RuntimeDictionaryInvalidator{
		Dictionaries:  dictionaries,
		DefaultSchema: "quanta",
	}

	invalidator.InvalidateValueChange(DictionaryValueChange{
		Table: "customers_qa",
		Field: "city",
		Label: "Seattle",
		ID:    1,
	})

	if len(dictionaries.refs) != 1 || dictionaries.refs[0].QualifiedName() != "quanta.customers_qa.city" {
		t.Fatalf("invalidated dictionaries = %#v, want quanta.customers_qa.city", dictionaries.refs)
	}
}

type recordingInvalidatableCatalog struct {
	tables []string
}

type recordingTableInvalidationTarget struct {
	tables []string
}

func (t *recordingTableInvalidationTarget) InvalidateTable(table string) {
	t.tables = append(t.tables, table)
}

func (c *recordingInvalidatableCatalog) Table(string, string) (qsbridge.TableDefinition, qsbridge.DiagnosticSet) {
	return qsbridge.TableDefinition{}, nil
}

func (c *recordingInvalidatableCatalog) Relationship(string) (qsbridge.RelationshipDefinition, qsbridge.DiagnosticSet) {
	return qsbridge.RelationshipDefinition{}, nil
}

func (c *recordingInvalidatableCatalog) Function(string) (qsbridge.FunctionDefinition, qsbridge.DiagnosticSet) {
	return qsbridge.FunctionDefinition{}, nil
}

func (c *recordingInvalidatableCatalog) InvalidateTable(schema string, table string) {
	c.tables = append(c.tables, schema+"."+table)
}

type recordingDictionaryCache struct {
	refs []qsbridge.DictionaryRef
}

func (c *recordingDictionaryCache) Dictionary(qsbridge.DictionaryRef) (qsbridge.DictionaryDefinition, qsbridge.DiagnosticSet) {
	return qsbridge.DictionaryDefinition{}, nil
}

func (c *recordingDictionaryCache) LookupLabel(qsbridge.DictionaryRef, string) (qsbridge.DictionaryEntry, qsbridge.DiagnosticSet) {
	return qsbridge.DictionaryEntry{}, nil
}

func (c *recordingDictionaryCache) LookupID(qsbridge.DictionaryRef, qsbridge.StringEnumID) (qsbridge.DictionaryEntry, qsbridge.DiagnosticSet) {
	return qsbridge.DictionaryEntry{}, nil
}

func (c *recordingDictionaryCache) InvalidateDictionary(ref qsbridge.DictionaryRef) {
	c.refs = append(c.refs, ref)
}
