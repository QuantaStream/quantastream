package qsbridge

import (
	"sort"
	"strings"
)

// ClientGrantsExchange is adapter-facing metadata for SQL grant introspection.
type ClientGrantsExchange struct {
	Connection   ConnectionContext
	Grants       []AccessGrant
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ClientGrantSummaryRow describes aggregate SHOW GRANTS-style metadata.
type ClientGrantSummaryRow struct {
	GrantCount        int
	UserGrantCount    int
	RoleGrantCount    int
	SelectCount       int
	InsertCount       int
	UpdateCount       int
	DeleteCount       int
	TableCount        int
	FieldScopedCount  int
	FieldMentionCount int
}

// ListClientAccessGrants returns SHOW GRANTS-style metadata for an access policy.
func (s PlanningService) ListClientAccessGrants(connection ConnectionContext, policy AccessPolicy) ClientGrantsExchange {
	_ = s
	exchange := ClientGrantsExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.grantsResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Grants = filterAccessGrantsForSession(policy.Grants(), connection.Session)
	sort.Slice(exchange.Grants, func(i, j int) bool {
		left := grantSortKey(exchange.Grants[i])
		right := grantSortKey(exchange.Grants[j])
		return left < right
	})
	exchange.Result = exchange.grantsResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether grant metadata can be returned.
func (e ClientGrantsExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts grant metadata diagnostics into protocol-facing errors.
func (e ClientGrantsExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking grant metadata error, if any.
func (e ClientGrantsExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientGrantsExchange) grantsResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     grantsResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.grantRows(),
		Final: true,
	})
}

func grantsResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Principal_kind", Type: DataTypeString},
		{Name: "Principal", Type: DataTypeString},
		{Name: "Privilege", Type: DataTypeString},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Table", Type: DataTypeString},
		{Name: "Fields", Type: DataTypeString, Nullable: true},
		{Name: "Grant", Type: DataTypeString},
	}
}

func (e ClientGrantsExchange) grantRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Grants))
	for _, grant := range e.Grants {
		rows = append(rows, ResultRow{
			metadataStringCell(string(grant.PrincipalKind)),
			metadataStringCell(grant.Principal),
			metadataStringCell(string(grant.Privilege)),
			metadataStringCell(grant.Table.Schema),
			metadataStringCell(grant.Table.Table),
			metadataStringCell(grantFieldList(grant.Fields)),
			metadataStringCell(formatAccessGrant(grant)),
		})
	}
	return rows
}

func filterAccessGrantsForSession(grants []AccessGrant, session SessionContext) []AccessGrant {
	filtered := make([]AccessGrant, 0, len(grants))
	for _, grant := range grants {
		if grant.matchesPrincipal(session) {
			filtered = append(filtered, cloneAccessGrant(grant))
		}
	}
	return filtered
}

func formatAccessGrant(grant AccessGrant) string {
	privilege := strings.ToUpper(string(grant.Privilege))
	fields := grantFieldList(grant.Fields)
	if fields != "" {
		privilege += " (" + fields + ")"
	}
	return "GRANT " + privilege + " ON " + grantTableName(grant.Table) + " TO " + grantPrincipalName(grant)
}

func grantPrincipalName(grant AccessGrant) string {
	switch grant.PrincipalKind {
	case AccessPrincipalRole:
		return "ROLE '" + grant.Principal + "'"
	default:
		return "USER '" + grant.Principal + "'"
	}
}

func grantTableName(table TableInstance) string {
	if table.Schema != "" && table.Table != "" {
		return table.Schema + "." + table.Table
	}
	if table.Table != "" {
		return table.Table
	}
	return string(table.ID)
}

func grantFieldList(fields []FieldRef) string {
	if len(fields) == 0 {
		return ""
	}
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.Name != "" {
			names = append(names, field.Name)
			continue
		}
		if field.PhysicalName != "" {
			names = append(names, field.PhysicalName)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func grantSortKey(grant AccessGrant) string {
	return string(grant.PrincipalKind) + "\x00" + grant.Principal + "\x00" + string(grant.Privilege) + "\x00" + grantTableName(grant.Table) + "\x00" + grantFieldList(grant.Fields)
}

func summarizeAccessGrants(grants []AccessGrant) ClientGrantSummaryRow {
	summary := ClientGrantSummaryRow{GrantCount: len(grants)}
	tables := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		switch grant.PrincipalKind {
		case AccessPrincipalUser:
			summary.UserGrantCount++
		case AccessPrincipalRole:
			summary.RoleGrantCount++
		}
		switch grant.Privilege {
		case AccessSelect:
			summary.SelectCount++
		case AccessInsert:
			summary.InsertCount++
		case AccessUpdate:
			summary.UpdateCount++
		case AccessDelete:
			summary.DeleteCount++
		}
		if grant.Table.Table != "" || grant.Table.ID != "" {
			tables[grantTableName(grant.Table)] = struct{}{}
		}
		if len(grant.Fields) > 0 {
			summary.FieldScopedCount++
			summary.FieldMentionCount += len(grant.Fields)
		}
	}
	summary.TableCount = len(tables)
	return summary
}
