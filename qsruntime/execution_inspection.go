package qsruntime

import (
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// ExecutionInspectionExecutor names the configured executor selected for a request.
type ExecutionInspectionExecutor string

const (
	// ExecutionInspectionExecutorDirect means the direct QIAB executor would run the request.
	ExecutionInspectionExecutorDirect ExecutionInspectionExecutor = "direct"
	// ExecutionInspectionExecutorLegacy means a legacy compatibility executor would run the request.
	ExecutionInspectionExecutorLegacy ExecutionInspectionExecutor = "legacy"
)

// ExecutionJoinStatus aliases qsbridge relationship join execution status during the package split.
type ExecutionJoinStatus = qsbridge.RelationshipJoinExecutionStatus

const (
	// ExecutionJoinStatusPlanned means no runtime boundary is currently known for the join edge.
	ExecutionJoinStatusPlanned = qsbridge.RelationshipJoinExecutionStatusPlanned
	// ExecutionJoinStatusNotWiredYet marks a known plan shape with no runtime executor wiring yet.
	ExecutionJoinStatusNotWiredYet = qsbridge.RelationshipJoinExecutionStatusNotWiredYet
)

// ExecutionJoinInspection describes one runtime-visible join edge.
type ExecutionJoinInspection struct {
	Left            string
	Right           string
	SQLKind         qsbridge.JoinKind
	JoinKind        string
	EncodingKind    qsbridge.RelationshipEncodingKind
	Capabilities    qsbridge.RelationshipCapabilities
	ExecutionStatus ExecutionJoinStatus
}

// ExecutionShapeInspection captures optimizer-relevant execution shapes.
type ExecutionShapeInspection struct {
	GroupedAggregateTopNCandidate bool
	GroupedAggregateTopNDetail    string
	FoundsetFollowUpCandidate     bool
	FoundsetFollowUpDetail        string
}

// ExecutionCatalogInspection summarizes catalog views attached to a request.
type ExecutionCatalogInspection struct {
	NodeTableCount            int
	NodeFieldCount            int
	NodeRelationshipCount     int
	QueryTableCount           int
	QueryFieldCount           int
	QueryRelationshipCount    int
	QueryFunctionCount        int
	QueryDictionaryFieldCount int
}

// ExecutionInspection describes runtime routing and call planning without executing a request.
type ExecutionInspection struct {
	Route            ExecutionRoute
	SelectedExecutor ExecutionInspectionExecutor
	RuntimeProfile   RuntimeInspectionProfile
	JoinPlan         RelationshipJoinPlan
	Joins            []ExecutionJoinInspection
	Shape            ExecutionShapeInspection
	FilterDomain     qsbridge.QuantaFilterDomainTranslation
	FilterDomainPlan qsbridge.FilterDomainNormalizationPlan
	Catalog          ExecutionCatalogInspection
	Materialization  ProjectionMaterializationCapabilityReport
	CallPlan         LegacyExecutionCallPlan
	Diagnostics      qsbridge.DiagnosticSet
}

// Supported reports whether the inspection found an executable route and call plan.
func (i ExecutionInspection) Supported() bool {
	return !i.Diagnostics.BlocksNative()
}

// InspectExecutionRequest selects the executor and builds the runtime call plan without executing.
func InspectExecutionRequest(selector ExecutorSelector, request ExecutionRequest) ExecutionInspection {
	joinPlan := PlanRelationshipJoins(request.Joins)
	inspection := ExecutionInspection{
		Route:           request.Route,
		JoinPlan:        joinPlan,
		Joins:           executionJoinInspections(joinPlan),
		Shape:           executionShapeInspection(request, joinPlan),
		FilterDomain:    request.FilterDomain,
		Catalog:         executionCatalogInspection(request),
		Materialization: executionMaterializationCapabilityInspection(request),
	}
	inspection.FilterDomainPlan = request.FilterDomain.NormalizationPlan(qsbridge.FilterDomainNormalizeGroupedFilter, joinPlan)
	_, diagnostics := selector.SelectRequest(request)
	if diagnostics.BlocksNative() {
		inspection.Diagnostics = diagnostics
		return inspection
	}
	inspection.SelectedExecutor = executionInspectionExecutor(request.Route)
	if inspection.FilterDomain.Required {
		inspection.Diagnostics = qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhasePlan, filterDomainTranslationDiagnosticMessage(inspection.FilterDomain)),
		}
		return inspection
	}
	if diagnostics := relationshipVectorJoinDiagnostics(joinPlan); diagnostics.BlocksNative() {
		inspection.Diagnostics = diagnostics
		return inspection
	}
	callPlan, planDiagnostics := PlanLegacyExecutionCall(request)
	inspection.CallPlan = callPlan
	inspection.Diagnostics = append(inspection.Diagnostics, planDiagnostics...)
	return inspection
}

func executionMaterializationCapabilityInspection(request ExecutionRequest) ProjectionMaterializationCapabilityReport {
	if request.Materialization.ProjectionCount() == 0 {
		return ProjectionMaterializationCapabilityReport{}
	}
	return ProjectionMaterializationCapabilityReportForRequestWithCatalog(qsbridge.ProjectionMaterializationKernelRequest{
		ID:       "inspection_materialization_capability",
		Requests: []qsbridge.QuantaMaterializationRequest{request.Materialization},
	}, qsbridge.ProjectionMaterializationKernelResult{}, request.QueryCatalog)
}

func executionCatalogInspection(request ExecutionRequest) ExecutionCatalogInspection {
	inspection := ExecutionCatalogInspection{
		NodeTableCount:         len(request.NodeCatalog.Tables),
		NodeRelationshipCount:  len(request.NodeCatalog.Relationships),
		QueryTableCount:        len(request.QueryCatalog.Tables),
		QueryRelationshipCount: len(request.QueryCatalog.Relationships),
		QueryFunctionCount:     len(request.QueryCatalog.Functions),
	}
	for _, table := range request.NodeCatalog.Tables {
		inspection.NodeFieldCount += len(table.Fields)
	}
	for _, table := range request.QueryCatalog.Tables {
		inspection.QueryFieldCount += len(table.Fields)
		inspection.QueryRelationshipCount += len(table.Relationships)
		for _, field := range table.Fields {
			if field.Dictionary.Ref.Field != "" {
				inspection.QueryDictionaryFieldCount++
			}
		}
	}
	return inspection
}

func executionShapeInspection(request ExecutionRequest, joinPlan RelationshipJoinPlan) ExecutionShapeInspection {
	return ExecutionShapeInspection{
		GroupedAggregateTopNCandidate: executionGroupedAggregateTopNCandidate(request),
		GroupedAggregateTopNDetail:    executionGroupedAggregateTopNDetail(request),
		FoundsetFollowUpCandidate:     executionFoundsetFollowUpCandidate(request, joinPlan),
		FoundsetFollowUpDetail:        executionFoundsetFollowUpDetail(request, joinPlan),
	}
}

func executionGroupedAggregateTopNCandidate(request ExecutionRequest) bool {
	if len(request.GroupBy) == 0 || len(request.SQLAggregates) == 0 || request.Result.Limit <= 0 || len(request.OrderBy) == 0 {
		return false
	}
	for _, order := range request.OrderBy {
		if _, ok := executionAggregateRef(order.Expr); ok {
			return true
		}
	}
	return false
}

func executionGroupedAggregateTopNDetail(request ExecutionRequest) string {
	if !executionGroupedAggregateTopNCandidate(request) {
		return ""
	}
	return fmt.Sprintf("group_by=%d aggregates=%d having=%d order_by=%d limit=%d",
		len(request.GroupBy),
		len(request.SQLAggregates),
		len(request.Having),
		len(request.OrderBy),
		request.Result.Limit,
	)
}

func executionAggregateRef(expr qsbridge.Expr) (qsbridge.AggregateRefExpr, bool) {
	switch n := expr.(type) {
	case qsbridge.AggregateRefExpr:
		return n, true
	case *qsbridge.AggregateRefExpr:
		if n != nil {
			return *n, true
		}
	}
	return qsbridge.AggregateRefExpr{}, false
}

func executionFoundsetFollowUpCandidate(request ExecutionRequest, joinPlan RelationshipJoinPlan) bool {
	if !joinPlan.NeedsRelationshipVectorExecution() {
		return false
	}
	joinedTables := executionRelationshipTables(joinPlan)
	joinFields := executionRelationshipFields(joinPlan)
	for _, fragment := range request.Query.Fragments {
		table := strings.ToLower(fragment.Index)
		if _, ok := joinedTables[table]; !ok {
			continue
		}
		fieldKey := table + "." + strings.ToLower(fragment.Field)
		if _, isJoinField := joinFields[fieldKey]; isJoinField {
			continue
		}
		return true
	}
	return false
}

func executionFoundsetFollowUpDetail(request ExecutionRequest, joinPlan RelationshipJoinPlan) string {
	if !executionFoundsetFollowUpCandidate(request, joinPlan) {
		return ""
	}
	joinedTables := executionRelationshipTables(joinPlan)
	joinFields := executionRelationshipFields(joinPlan)
	for _, fragment := range request.Query.Fragments {
		table := strings.ToLower(fragment.Index)
		if _, ok := joinedTables[table]; !ok {
			continue
		}
		fieldKey := table + "." + strings.ToLower(fragment.Field)
		if _, isJoinField := joinFields[fieldKey]; isJoinField {
			continue
		}
		return fmt.Sprintf("fragment=%s.%s edges=%d", fragment.Index, fragment.Field, len(joinPlan.Edges))
	}
	return fmt.Sprintf("edges=%d", len(joinPlan.Edges))
}

func executionRelationshipTables(joinPlan RelationshipJoinPlan) map[string]struct{} {
	tables := make(map[string]struct{})
	for _, edge := range joinPlan.Edges {
		if edge.Left.Table.Table != "" {
			tables[strings.ToLower(edge.Left.Table.Table)] = struct{}{}
		}
		if edge.Right.Table.Table != "" {
			tables[strings.ToLower(edge.Right.Table.Table)] = struct{}{}
		}
	}
	return tables
}

func executionRelationshipFields(joinPlan RelationshipJoinPlan) map[string]struct{} {
	fields := make(map[string]struct{})
	for _, edge := range joinPlan.Edges {
		executionRememberRelationshipField(fields, edge.Left)
		executionRememberRelationshipField(fields, edge.Right)
	}
	return fields
}

func executionRememberRelationshipField(fields map[string]struct{}, field qsbridge.FieldRef) {
	table := strings.ToLower(field.Table.Table)
	name := field.PhysicalName
	if name == "" {
		name = field.Name
	}
	if table == "" || name == "" {
		return
	}
	fields[table+"."+strings.ToLower(name)] = struct{}{}
}

// Inspect returns routing and call-plan metadata for the supplied request.
func (s ExecutionService) Inspect(request ExecutionRequest) ExecutionInspection {
	return InspectExecutionRequest(s.Selector, request)
}

func executionInspectionExecutor(route ExecutionRoute) ExecutionInspectionExecutor {
	if route.Direct() {
		return ExecutionInspectionExecutorDirect
	}
	if route.CompatibilityPath() {
		return ExecutionInspectionExecutorLegacy
	}
	return ""
}

func executionJoinInspections(plan RelationshipJoinPlan) []ExecutionJoinInspection {
	joins := make([]ExecutionJoinInspection, 0, len(plan.Edges))
	for _, planned := range plan.Edges {
		joins = append(joins, ExecutionJoinInspection{
			Left:            planned.Left.QualifiedName(),
			Right:           planned.Right.QualifiedName(),
			SQLKind:         planned.SQLKind,
			JoinKind:        string(planned.ExecutionKind),
			EncodingKind:    planned.EncodingKind,
			Capabilities:    append(qsbridge.RelationshipCapabilities(nil), planned.Capabilities...),
			ExecutionStatus: planned.Status,
		})
	}
	return joins
}
