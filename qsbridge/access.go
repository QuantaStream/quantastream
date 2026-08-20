package qsbridge

// AccessPrivilege describes one SQL authorization action needed by a plan.
type AccessPrivilege string

const (
	// AccessSelect permits reading table or column data.
	AccessSelect AccessPrivilege = "select"
	// AccessInsert permits inserting rows into a table.
	AccessInsert AccessPrivilege = "insert"
	// AccessUpdate permits updating rows or columns in a table.
	AccessUpdate AccessPrivilege = "update"
	// AccessDelete permits deleting rows from a table.
	AccessDelete AccessPrivilege = "delete"
	// AccessTruncate permits clearing all rows from a table.
	AccessTruncate AccessPrivilege = "truncate"
	// AccessCreate permits activating a table schema.
	AccessCreate AccessPrivilege = "create"
	// AccessDrop permits dropping a table schema and its data.
	AccessDrop AccessPrivilege = "drop"
)

// AccessRequirement describes one authorization check required by a plan.
type AccessRequirement struct {
	Privilege AccessPrivilege
	Table     TableInstance
	Fields    []FieldRef
}

// RequiredAccess returns authorization metadata for this query.
func (q QueryIR) RequiredAccess() []AccessRequirement {
	collector := newAccessCollector()
	for _, branch := range q.UnionAll {
		for _, requirement := range branch.RequiredAccess() {
			collector.addRequirement(requirement)
		}
	}
	for _, field := range q.requiredReadAccessFields() {
		collector.addField(AccessSelect, field)
	}
	for _, source := range q.Sources {
		collector.ensureTable(AccessSelect, source)
	}
	switch q.Mutation.Kind {
	case MutationInsert:
		collector.addFields(AccessInsert, q.Mutation.Target, q.Mutation.Columns)
	case MutationUpdate:
		collector.addFields(AccessUpdate, q.Mutation.Target, q.Mutation.Columns)
	case MutationDelete:
		collector.ensureTable(AccessDelete, q.Mutation.Target)
	case MutationTruncate:
		collector.ensureTable(AccessTruncate, q.Mutation.Target)
	case MutationCreateTable:
		collector.ensureTable(AccessCreate, q.Mutation.Target)
	case MutationDropTable:
		collector.ensureTable(AccessDrop, q.Mutation.Target)
	case MutationAlterTableAddPrimaryKey:
		collector.addFields(AccessCreate, q.Mutation.Target, q.Mutation.Columns)
	case MutationCreateView:
		collector.ensureTable(AccessCreate, q.Mutation.Target)
	case MutationDropView:
		collector.ensureTable(AccessDrop, q.Mutation.Target)
	}
	return collector.requirements
}

// RequiredAccess returns authorization metadata for this plan result.
func (r PlanResult) RequiredAccess() []AccessRequirement {
	return r.Query.RequiredAccess()
}

// RequiredAccess returns authorization metadata for this prepared plan.
func (p PreparedPlan) RequiredAccess() []AccessRequirement {
	return p.Query.RequiredAccess()
}

func (q QueryIR) requiredReadAccessFields() []FieldRef {
	collector := newFieldCollector()
	for _, predicate := range q.Predicates {
		collector.addExpr(predicate.Expr)
	}
	for _, edge := range q.Joins {
		collector.addField(edge.Left)
		collector.addField(edge.Right)
		for _, predicate := range edge.On {
			collector.addExpr(predicate.Expr)
		}
	}
	for _, edge := range q.Memberships {
		collector.addField(edge.Left)
		collector.addField(edge.Right)
		for _, expr := range edge.LeftTuple {
			collector.addExpr(expr)
		}
		for _, expr := range edge.RightTuple {
			collector.addExpr(expr)
		}
		for _, predicate := range edge.Predicates {
			collector.addExpr(predicate.Expr)
		}
	}
	for _, projection := range q.Projection {
		collector.addExpr(projection.Expr)
	}
	for _, expr := range q.GroupBy {
		collector.addExpr(expr)
	}
	for _, aggregate := range q.Aggregates {
		collector.addExpr(aggregate.Input)
		collector.addExpr(aggregate.Filter)
	}
	for _, predicate := range q.Having {
		collector.addExpr(predicate.Expr)
	}
	for _, sort := range q.OrderBy {
		collector.addExpr(sort.Expr)
	}
	for _, row := range q.Mutation.Rows {
		for _, value := range row.Values {
			collector.addExpr(value)
		}
	}
	for _, assignment := range q.Mutation.Assignments {
		collector.addExpr(assignment.Value)
	}
	for _, predicate := range q.Mutation.Predicates {
		collector.addExpr(predicate.Expr)
	}
	return collector.refs
}

type accessCollector struct {
	requirements []AccessRequirement
	index        map[string]int
	fields       map[string]struct{}
}

func newAccessCollector() accessCollector {
	return accessCollector{
		requirements: make([]AccessRequirement, 0),
		index:        make(map[string]int),
		fields:       make(map[string]struct{}),
	}
}

func (c *accessCollector) addFields(privilege AccessPrivilege, table TableInstance, fields []FieldRef) {
	c.ensureTable(privilege, table)
	for _, field := range fields {
		c.addField(privilege, field)
	}
}

func (c *accessCollector) addRequirement(requirement AccessRequirement) {
	c.ensureTable(requirement.Privilege, requirement.Table)
	for _, field := range requirement.Fields {
		c.addField(requirement.Privilege, field)
	}
}

func (c *accessCollector) addField(privilege AccessPrivilege, field FieldRef) {
	if field.Name == "" && field.PhysicalName == "" {
		c.ensureTable(privilege, field.Table)
		return
	}
	index := c.ensureTable(privilege, field.Table)
	fieldKey := accessFieldKey(privilege, field)
	if _, ok := c.fields[fieldKey]; ok {
		return
	}
	c.fields[fieldKey] = struct{}{}
	c.requirements[index].Fields = append(c.requirements[index].Fields, field)
}

func (c *accessCollector) ensureTable(privilege AccessPrivilege, table TableInstance) int {
	key := accessTableKey(privilege, table)
	if index, ok := c.index[key]; ok {
		return index
	}
	index := len(c.requirements)
	c.index[key] = index
	c.requirements = append(c.requirements, AccessRequirement{
		Privilege: privilege,
		Table:     table,
	})
	return index
}

func accessTableKey(privilege AccessPrivilege, table TableInstance) string {
	return string(privilege) + "\x00" + string(table.ID) + "\x00" + table.Schema + "\x00" + table.Table + "\x00" + table.Alias
}

func accessFieldKey(privilege AccessPrivilege, field FieldRef) string {
	return accessTableKey(privilege, field.Table) + "\x00" + field.Name + "\x00" + field.PhysicalName
}
