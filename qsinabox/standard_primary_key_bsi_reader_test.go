package qsinabox

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/core"
)

func TestStandardSingleColumnBSIPrimaryKeyReaderLooksUpCommittedKey(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	requireStandardProcessSQLSuccess(t, process, "insert into sample (id, city) values (101, 'Buenos Aires')")
	requireStandardProcessSQLSuccess(t, process, "commit")

	table := standardCachedTable(process.TableCache, "sample")
	if table == nil {
		t.Fatalf("cached table sample = nil")
	}
	attr, err := table.GetAttribute("id")
	if err != nil {
		t.Fatalf("GetAttribute(id) error = %v", err)
	}
	reader := StandardSingleColumnBSIPrimaryKeyReader{
		Pool:       process.RuntimeMount.Pool,
		TableCache: process.TableCache,
		Direct:     process.Backend.Adapter.BitmapIndex,
	}
	found, err := reader.LookupSingleColumnBSIPrimaryKey(core.SingleColumnBSIPrimaryKeyReadRequest{
		TableName:       "sample",
		FieldName:       "id",
		MappingStrategy: "IntBSI",
		Attribute:       attr,
		Value:           101,
	})
	if err != nil {
		t.Fatalf("LookupSingleColumnBSIPrimaryKey(101) error = %v", err)
	}
	if len(found.ColumnIDs) != 1 {
		t.Fatalf("LookupSingleColumnBSIPrimaryKey(101) = %#v, want one column ID", found.ColumnIDs)
	}

	missing, err := reader.LookupSingleColumnBSIPrimaryKey(core.SingleColumnBSIPrimaryKeyReadRequest{
		TableName:       "sample",
		FieldName:       "id",
		MappingStrategy: "IntBSI",
		Attribute:       attr,
		Value:           404,
	})
	if err != nil {
		t.Fatalf("LookupSingleColumnBSIPrimaryKey(404) error = %v", err)
	}
	if len(missing.ColumnIDs) != 0 {
		t.Fatalf("LookupSingleColumnBSIPrimaryKey(404) = %#v, want no column IDs", missing.ColumnIDs)
	}

	fallbackReader := reader
	fallbackReader.Direct = nil
	fallback, err := fallbackReader.LookupSingleColumnBSIPrimaryKey(core.SingleColumnBSIPrimaryKeyReadRequest{
		TableName:       "sample",
		FieldName:       "id",
		MappingStrategy: "IntBSI",
		Attribute:       attr,
		Value:           101,
	})
	if err != nil {
		t.Fatalf("fallback LookupSingleColumnBSIPrimaryKey(101) error = %v", err)
	}
	if !reflect.DeepEqual(fallback.ColumnIDs, found.ColumnIDs) {
		t.Fatalf("fallback LookupSingleColumnBSIPrimaryKey(101) = %#v, want %#v", fallback.ColumnIDs, found.ColumnIDs)
	}
}
