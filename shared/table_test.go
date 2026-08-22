package shared

import (
	"testing"

	"github.com/QuantaStream/quantastream/searchindex"
	"github.com/stretchr/testify/assert"
)

func TestLoadTable(t *testing.T) {

	schema, err := LoadSchema("./testdata/config2", "cities", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, schema) {
		gender, err2 := schema.GetAttribute("gender")
		assert.Nil(t, err2)
		if assert.NotNil(t, gender) {
			assert.Equal(t, gender.MappingStrategy, "StringEnum")
		}
		assert.Equal(t, len(gender.Values), 2)

		regionList, err2 := schema.GetAttribute("region_list")
		assert.Nil(t, err2)
		if assert.NotNil(t, regionList) {
			assert.NotNil(t, regionList.MapperConfig)
			assert.Equal(t, regionList.MapperConfig["delim"], ",")
		}

		name, err3 := schema.GetAttribute("name")
		assert.Nil(t, err3)
		if assert.NotNil(t, name) {
			assert.True(t, name.IsBSI())
		}
	}
}

func TestLoadTableWithPK(t *testing.T) {

	schema, err := LoadSchema("./testdata/config", "cityzip", nil)
	assert.Nil(t, err)
	pki, err2 := schema.GetPrimaryKeyInfo()
	assert.Nil(t, err2)
	assert.NotNil(t, pki)
	assert.Equal(t, len(pki), 2)
}

func TestBasicTableGetAttributeInitializesGeneratedAttributeMap(t *testing.T) {
	table := &BasicTable{
		Name: "scratch",
		Attributes: []BasicAttribute{
			{FieldName: "id", SourceName: "id", Type: "Integer", MappingStrategy: "IntBSI"},
		},
	}

	attr, err := table.GetAttribute("id")

	assert.Nil(t, err)
	if assert.NotNil(t, attr) {
		assert.Equal(t, "IntBSI", attr.MappingStrategy)
	}
}

func TestLoadSchemaAddsCompoundPrimaryKeyAuthorityAttribute(t *testing.T) {

	schema, err := LoadSchema("./testdata/config", "cityzip", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, schema) {
		attr, err := schema.GetAttribute(CompoundPrimaryKeyAuthorityFieldName)
		assert.Nil(t, err)
		if assert.NotNil(t, attr) {
			assert.Equal(t, "IntBSI", attr.MappingStrategy)
			assert.True(t, attr.IsBSI())
			assert.True(t, attr.System)
		}
	}
}

func TestEnsureSearchHashAttributesAddsSystemBSI(t *testing.T) {
	table := &BasicTable{
		Name: "customer",
		Attributes: []BasicAttribute{
			{FieldName: "c_custkey", Type: "Integer", MappingStrategy: "IntBSI"},
			{FieldName: "c_name", Type: "String", MappingStrategy: "StringLexBSI", Searchable: true},
		},
	}

	assert.True(t, EnsureSearchHashAttributes(table))
	assert.False(t, EnsureSearchHashAttributes(table))

	attr, err := table.GetAttribute(searchindex.HashFieldName("c_name"))
	assert.Nil(t, err)
	if assert.NotNil(t, attr) {
		assert.Equal(t, "IntBSI", attr.MappingStrategy)
		assert.Equal(t, "Integer", attr.Type)
		assert.True(t, attr.System)
		assert.True(t, attr.IsBSI())
	}
}

func TestSchemaCompare(t *testing.T) {

	current, err := LoadSchema("./testdata/config", "cities", nil)
	assert.Nil(t, err)
	new, err := LoadSchema("./testdata/config2", "cities", nil)
	assert.Nil(t, err)

	ok, warnings, err := current.Compare(new)
	assert.Nil(t, err)
	assert.False(t, ok)
	if assert.Equal(t, 1, len(warnings)) {
		assert.Equal(t, warnings[0], "new attribute 'gender', addition is allowable")
	}

	// new.DisableDedup = true TODO: if DisableDedup is gone then delete this block
	// or else change the expected val to 1
	// ok, warnings, err = current.Compare(new)
	// assert.Nil(t, err)
	// assert.Equal(t, 2, len(warnings))
	// _ = ok

	currState, errx := current.GetAttribute("state_name")
	assert.Nil(t, errx)
	assert.NotNil(t, currState)
	newState, errx := new.GetAttribute("state_name")
	assert.Nil(t, errx)
	assert.NotNil(t, newState)
	ok, warnings, err = currState.Compare(newState)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, 0, len(warnings))

	newState.Desc = "State name."
	ok, warnings, err = currState.Compare(newState)
	assert.Nil(t, err)
	assert.False(t, ok)
	if assert.Equal(t, 1, len(warnings)) {
		assert.Equal(t, warnings[0],
			"attribute 'state_name' description changed existing = '', new = 'State name.'")
	}
}

func TestAttributeCompareDetectsRelationshipArtifactChange(t *testing.T) {
	current := &BasicAttribute{
		FieldName:       "child_id",
		Type:            "Integer",
		ForeignKey:      "parent",
		MappingStrategy: "IntBSI",
	}
	next := *current
	next.RelationshipArtifacts.ParentToChild = true

	ok, warnings, err := current.Compare(&next)
	assert.Nil(t, err)
	assert.False(t, ok)
	if assert.Equal(t, 1, len(warnings)) {
		assert.Contains(t, warnings[0], "relationship parent-to-child artifact changed")
	}
}
