package core

import (
	"testing"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/assert"
)

func TestLoadTable(t *testing.T) {
	tcs := NewTableCacheStruct()
	table, err := LoadTable(tcs, "./testdata", nil, "cities", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, table) {
		assert.NotNil(t, table.BasicTable)
		assert.Equal(t, 15, len(table.Attributes))
		regionList, err2 := table.GetAttribute("region_list")
		assert.Nil(t, err2)
		if assert.NotNil(t, regionList) {
			assert.NotNil(t, regionList.MapperConfig)
			assert.Equal(t, regionList.MapperConfig["delim"], ",")
			assert.Equal(t, MapperTypeFromString(regionList.MappingStrategy), StringEnum)
			assert.NotNil(t, regionList.mapperInstance)
		}

		name, err3 := table.GetAttribute("name")
		assert.Nil(t, err3)
		if assert.NotNil(t, name) {
			assert.True(t, name.IsBSI())
		}
	}
}

func TestLoadTableWithPK(t *testing.T) {
	tcs := NewTableCacheStruct()
	table, err := LoadTable(tcs, "./testdata", nil, "cityzip", nil)
	assert.Nil(t, err)
	pki, err2 := table.GetPrimaryKeyInfo()
	assert.Nil(t, err2)
	assert.NotNil(t, pki)
	assert.Equal(t, len(pki), 2)
}

func TestLoadTableWithRelation(t *testing.T) {
	tcs := NewTableCacheStruct()
	table, err := LoadTable(tcs, "./testdata", nil, "cityzip", nil)
	assert.Nil(t, err)
	fka, err2 := table.GetAttribute("city_id")
	assert.Nil(t, err2)
	tab, spec, err3 := fka.GetFKSpec()
	assert.Nil(t, err3)
	assert.NotNil(t, tab)
	assert.NotNil(t, spec)
}

func TestAttributeGetValueUsesLocalCacheHit(t *testing.T) {
	table := &Table{BasicTable: &shared.BasicTable{Name: "lineitem"}}
	attr := &Attribute{
		BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipmode"},
		Parent:         table,
		valueMap:       map[interface{}]uint64{"TRUCK": 7},
		reverseMap:     map[uint64]interface{}{7: "TRUCK"},
	}

	value, err := attr.GetValue(" TRUCK ")

	assert.NoError(t, err)
	assert.Equal(t, uint64(7), value)
}

func TestStringEnumAttributeCopyCanonicalizesToTableCache(t *testing.T) {
	tcs := NewTableCacheStruct()
	table := &Table{
		BasicTable:       &shared.BasicTable{Name: "orders"},
		AttributeNameMap: map[string]*Attribute{},
		tableCache:       tcs,
	}
	canonical := &Attribute{
		BasicAttribute: &shared.BasicAttribute{FieldName: "o_orderstatus", MappingStrategy: "StringEnum"},
		Parent:         table,
		valueMap:       map[interface{}]uint64{"READY": 9},
		reverseMap:     map[uint64]interface{}{9: "READY"},
	}
	table.AttributeNameMap["o_orderstatus"] = canonical
	tcs.TableCache["orders"] = table

	copied := *canonical
	copied.valueMap = map[interface{}]uint64{}
	copied.reverseMap = map[uint64]interface{}{}

	assert.Same(t, canonical, copied.canonicalStringEnumAttribute())
	value, err := copied.GetValue("READY")
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), value)
	reverse, err := copied.GetValueForID(9)
	assert.NoError(t, err)
	assert.Equal(t, "READY", reverse)
}
