package qsbridge

// CatalogViewKind identifies the audience for a catalog projection.
type CatalogViewKind string

const (
	// CatalogViewNode is the lean physical catalog surface consumed by data nodes.
	CatalogViewNode CatalogViewKind = "node"
	// CatalogViewQuery is the semantic catalog surface consumed by query planning and runtime code.
	CatalogViewQuery CatalogViewKind = "query"
)

// NodeCatalogView is the node-facing projection of catalog metadata.
//
// It intentionally excludes query-only semantic state such as StringEnum
// dictionary entries. Nodes need enough information to open storage, locate
// shards, read BSI/bitmap fields, and traverse relationship vectors.
type NodeCatalogView struct {
	Tables        []NodeTableView
	Relationships []NodeRelationshipView
}

// NodeTableView contains physical table metadata needed by storage nodes.
type NodeTableView struct {
	Schema  string
	Name    string
	Storage StorageProfile
	Fields  []NodeFieldView
}

// NodeFieldView contains physical field metadata needed by storage nodes.
type NodeFieldView struct {
	Name         string
	PhysicalName string
	Type         DataType
	Index        IndexKind
	Nullable     bool
	PrimaryKey   bool
	Storage      StorageProfile
	Encoding     EncodingProfile
}

// NodeRelationshipView contains physical relationship-vector metadata.
type NodeRelationshipView struct {
	Name      string
	FromTable string
	FromField string
	ToTable   string
	ToField   string
	Direction JoinDirection
	Encoding  RelationshipEncodingProfile
}

// QueryCatalogView is the query-facing projection of catalog metadata.
//
// It keeps semantic state required by binding, planning, rehydration,
// dictionary resolution, SQL capability checks, and relationship traversal.
type QueryCatalogView struct {
	Tables        []QueryTableView
	Relationships []RelationshipDefinition
	Functions     []FunctionDefinition
}

// QueryTableView contains semantic table metadata for planner/runtime use.
type QueryTableView struct {
	Schema        string
	Name          string
	Storage       StorageProfile
	Fields        []FieldDefinition
	Relationships []RelationshipDefinition
}

// NewNodeCatalogView projects table definitions into the lean node-facing view.
func NewNodeCatalogView(tables []TableDefinition) NodeCatalogView {
	view := NodeCatalogView{}
	for _, table := range tables {
		nodeTable := NodeTableView{
			Schema:  table.Schema,
			Name:    table.Name,
			Storage: table.Storage,
			Fields:  make([]NodeFieldView, 0, len(table.Fields)),
		}
		for _, field := range table.Fields {
			nodeTable.Fields = append(nodeTable.Fields, NodeFieldView{
				Name:         field.Name,
				PhysicalName: field.PhysicalName,
				Type:         field.Type,
				Index:        field.Index,
				Nullable:     field.Nullable,
				PrimaryKey:   field.PrimaryKey,
				Storage:      field.Storage,
				Encoding:     cloneEncodingProfile(field.Encoding),
			})
		}
		view.Tables = append(view.Tables, nodeTable)
		for _, relationship := range table.Relationships {
			view.Relationships = append(view.Relationships, NodeRelationshipView{
				Name:      relationship.Name,
				FromTable: relationship.FromTable,
				FromField: relationship.FromField,
				ToTable:   relationship.ToTable,
				ToField:   relationship.ToField,
				Direction: relationship.Direction,
				Encoding:  cloneRelationshipEncodingProfile(relationship.Encoding),
			})
		}
	}
	return view
}

// NewQueryCatalogView projects table, relationship, and function definitions into the semantic query-facing view.
func NewQueryCatalogView(tables []TableDefinition, relationships []RelationshipDefinition, functions []FunctionDefinition) QueryCatalogView {
	view := QueryCatalogView{
		Tables:        make([]QueryTableView, 0, len(tables)),
		Relationships: make([]RelationshipDefinition, 0, len(relationships)),
		Functions:     append([]FunctionDefinition(nil), functions...),
	}
	for _, table := range tables {
		queryTable := QueryTableView{
			Schema:        table.Schema,
			Name:          table.Name,
			Storage:       table.Storage,
			Fields:        make([]FieldDefinition, 0, len(table.Fields)),
			Relationships: make([]RelationshipDefinition, 0, len(table.Relationships)),
		}
		for _, field := range table.Fields {
			queryTable.Fields = append(queryTable.Fields, cloneCatalogViewFieldDefinition(field))
		}
		for _, relationship := range table.Relationships {
			queryTable.Relationships = append(queryTable.Relationships, cloneRelationshipDefinition(relationship))
		}
		view.Tables = append(view.Tables, queryTable)
	}
	for _, relationship := range relationships {
		view.Relationships = append(view.Relationships, cloneRelationshipDefinition(relationship))
	}
	return view
}

func cloneCatalogViewFieldDefinition(field FieldDefinition) FieldDefinition {
	cloned := field
	cloned.Storage = field.Storage
	cloned.Encoding = cloneEncodingProfile(field.Encoding)
	cloned.Dictionary = cloneDictionaryDefinition(field.Dictionary)
	return cloned
}
