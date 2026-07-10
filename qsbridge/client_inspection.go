package qsbridge

// ClientPlanInspectionExchange is client-facing explain/profile planning metadata.
type ClientPlanInspectionExchange struct {
	Connection  ConnectionContext
	Statement   ClientStatementText
	Request     PlanRequest
	Prepared    PreparedPlan
	Inspection  InspectionReport
	Profile     ExecutionProfile
	Diagnostics DiagnosticSet
}

// PrepareClientPlanInspection prepares one statement and returns explain/profile metadata.
func (s PlanningService) PrepareClientPlanInspection(connection ConnectionContext, plan ConnectionPlanOptions, sql string, options ExecutionOptions) ClientPlanInspectionExchange {
	bundle := NewClientStatementBundle(connection, plan, sql)
	planned := s.PrepareClientStatementBundle(bundle)
	exchange := ClientPlanInspectionExchange{
		Connection:  cloneConnectionContext(planned.Connection),
		Diagnostics: mergeDiagnosticSets(planned.Diagnostics, options.Diagnostics(), validateClientPlanInspectionOptions(connection.Protocol, options)),
	}
	if len(planned.Statements) == 0 {
		return exchange
	}
	statement := planned.Statements[0]
	profile := newExecutionProfile(options, statement.Prepared)
	exchange.Statement = statement.Statement
	exchange.Request = statement.Request
	exchange.Prepared = clonePreparedPlan(statement.Prepared)
	exchange.Inspection = cloneInspectionReport(statement.Prepared.Inspection)
	exchange.Profile = cloneExecutionProfile(profile)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, profile.Diagnostics)
	return exchange
}

// Supported reports whether explain/profile metadata can proceed.
func (e ClientPlanInspectionExchange) Supported() bool {
	return e.Connection.Supported() && e.Prepared.Supported && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts explain/profile diagnostics into protocol-facing errors.
func (e ClientPlanInspectionExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking explain/profile error, if any.
func (e ClientPlanInspectionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func validateClientPlanInspectionOptions(profile ProtocolProfile, options ExecutionOptions) DiagnosticSet {
	diagnostics := options.Diagnostics()
	if options.TraceExplain && !profile.Supports(ProtocolCapabilityExplain) {
		diagnostics = mergeDiagnosticSets(diagnostics, DiagnosticSet{
			protocolCapabilityDiagnostic("explain metadata is not supported by protocol profile"),
		})
	}
	if options.IncludeProfile && !profile.Supports(ProtocolCapabilityProfile) {
		diagnostics = mergeDiagnosticSets(diagnostics, DiagnosticSet{
			protocolCapabilityDiagnostic("execution profile metadata is not supported by protocol profile"),
		})
	}
	return diagnostics
}

func cloneInspectionReport(report InspectionReport) InspectionReport {
	return InspectionReport{
		Query:          cloneQueryInspection(report.Query),
		Supported:      report.Supported,
		Capabilities:   append([]PlanCapability(nil), report.Capabilities...),
		Diagnostics:    cloneDiagnosticSet(report.Diagnostics),
		Classification: cloneNativeClassification(report.Classification),
		Optimization:   report.Optimization.Clone(),
		Logical:        clonePlanExplanation(report.Logical),
		Physical:       clonePhysicalPlanExplanation(report.Physical),
	}
}

func cloneQueryInspection(query QueryInspection) QueryInspection {
	query.Sources = append([]string(nil), query.Sources...)
	query.Fields = append([]string(nil), query.Fields...)
	query.Access = cloneAccessInspections(query.Access)
	query.FieldEncodings = cloneFieldEncodingInspections(query.FieldEncodings)
	query.Parameters = append([]ParameterRef(nil), query.Parameters...)
	query.ResultColumns = append([]ResultColumn(nil), query.ResultColumns...)
	query.FunctionUsages = append([]FunctionUsage(nil), query.FunctionUsages...)
	query.SubqueryIntents = cloneSubqueryPlanIntentReports(query.SubqueryIntents)
	query.SubqueryHelperPlans = cloneSubqueryHelperPlanReports(query.SubqueryHelperPlans)
	query.NativeSubquerySteps = cloneNativeSubqueryStepReports(query.NativeSubquerySteps)
	query.Blockers = append([]NativeBlocker(nil), query.Blockers...)
	query.Statement = cloneStatementResult(query.Statement)
	query.Mutation.Columns = append([]string(nil), query.Mutation.Columns...)
	query.JoinEdges = cloneJoinInspections(query.JoinEdges)
	query.MembershipEdges = cloneMembershipInspections(query.MembershipEdges)
	query.Result.Columns = append([]FieldRef(nil), query.Result.Columns...)
	query.Result.Hidden = append([]FieldRef(nil), query.Result.Hidden...)
	query.Result.Statement = cloneStatementResult(query.Result.Statement)
	return query
}

func cloneAccessInspections(inspections []AccessInspection) []AccessInspection {
	if len(inspections) == 0 {
		return nil
	}
	cloned := make([]AccessInspection, 0, len(inspections))
	for _, inspection := range inspections {
		inspection.Fields = append([]string(nil), inspection.Fields...)
		cloned = append(cloned, inspection)
	}
	return cloned
}

func cloneFieldEncodingInspections(inspections []FieldEncodingInspection) []FieldEncodingInspection {
	if len(inspections) == 0 {
		return nil
	}
	cloned := make([]FieldEncodingInspection, 0, len(inspections))
	for _, inspection := range inspections {
		inspection.PredicateCapabilities = append([]PredicateCapability(nil), inspection.PredicateCapabilities...)
		inspection.ProjectionCapabilities = append([]ProjectionCapability(nil), inspection.ProjectionCapabilities...)
		cloned = append(cloned, inspection)
	}
	return cloned
}

func cloneSubqueryPlanIntentReports(reports []SubqueryPlanIntentReport) []SubqueryPlanIntentReport {
	if len(reports) == 0 {
		return nil
	}
	cloned := make([]SubqueryPlanIntentReport, 0, len(reports))
	for _, report := range reports {
		report.HelperKinds = append([]string(nil), report.HelperKinds...)
		if report.Scalar != nil {
			scalar := *report.Scalar
			report.Scalar = &scalar
		}
		if report.CorrelatedAggregate != nil {
			correlated := *report.CorrelatedAggregate
			correlated.RequiredFilters = append([]string(nil), correlated.RequiredFilters...)
			report.CorrelatedAggregate = &correlated
		}
		cloned = append(cloned, report)
	}
	return cloned
}

func cloneJoinInspections(inspections []JoinInspection) []JoinInspection {
	if len(inspections) == 0 {
		return nil
	}
	cloned := make([]JoinInspection, 0, len(inspections))
	for _, inspection := range inspections {
		inspection.Capabilities = append([]RelationshipCapability(nil), inspection.Capabilities...)
		cloned = append(cloned, inspection)
	}
	return cloned
}

func cloneMembershipInspections(inspections []MembershipInspection) []MembershipInspection {
	if len(inspections) == 0 {
		return nil
	}
	cloned := make([]MembershipInspection, 0, len(inspections))
	for _, inspection := range inspections {
		inspection.Capabilities = append([]RelationshipCapability(nil), inspection.Capabilities...)
		cloned = append(cloned, inspection)
	}
	return cloned
}

func cloneNativeClassification(classification NativeClassification) NativeClassification {
	classification.Capabilities = append([]PlanCapability(nil), classification.Capabilities...)
	classification.Diagnostics = cloneDiagnosticSet(classification.Diagnostics)
	classification.Fields = append([]FieldRef(nil), classification.Fields...)
	return classification
}

func clonePlanExplanation(explanation PlanExplanation) PlanExplanation {
	return PlanExplanation{
		Supported:    explanation.Supported,
		Capabilities: append([]PlanCapability(nil), explanation.Capabilities...),
		Diagnostics:  cloneDiagnosticSet(explanation.Diagnostics),
		Optimization: explanation.Optimization.Clone(),
		Nodes:        clonePlanNodeExplanations(explanation.Nodes),
	}
}

func clonePlanNodeExplanations(nodes []PlanNodeExplanation) []PlanNodeExplanation {
	if len(nodes) == 0 {
		return nil
	}
	cloned := make([]PlanNodeExplanation, 0, len(nodes))
	for _, node := range nodes {
		node.Fields = append([]string(nil), node.Fields...)
		node.Predicates.Capabilities = append([]PlanCapability(nil), node.Predicates.Capabilities...)
		node.Membership.Capabilities = append([]PlanCapability(nil), node.Membership.Capabilities...)
		node.ScalarSubquery.OutputNames = append([]string(nil), node.ScalarSubquery.OutputNames...)
		node.ScalarSubquery.HelperPlans = cloneSubqueryHelperPlanReports(node.ScalarSubquery.HelperPlans)
		node.ScalarSubquery.NativeSteps = cloneNativeSubqueryStepReports(node.ScalarSubquery.NativeSteps)
		node.CorrelatedAggregate.AggregateFunctions = append([]string(nil), node.CorrelatedAggregate.AggregateFunctions...)
		node.CorrelatedAggregate.InnerKeyRefs = append([]string(nil), node.CorrelatedAggregate.InnerKeyRefs...)
		node.CorrelatedAggregate.OuterKeyRefs = append([]string(nil), node.CorrelatedAggregate.OuterKeyRefs...)
		node.CorrelatedAggregate.HelperKinds = append([]string(nil), node.CorrelatedAggregate.HelperKinds...)
		node.CorrelatedAggregate.HelperPlans = cloneSubqueryHelperPlanReports(node.CorrelatedAggregate.HelperPlans)
		node.CorrelatedAggregate.NativeSteps = cloneNativeSubqueryStepReports(node.CorrelatedAggregate.NativeSteps)
		node.Join.On.Capabilities = append([]PlanCapability(nil), node.Join.On.Capabilities...)
		node.Join.Capabilities = append([]PlanCapability(nil), node.Join.Capabilities...)
		node.Aggregate.Having.Capabilities = append([]PlanCapability(nil), node.Aggregate.Having.Capabilities...)
		node.Diagnostics = append([]DiagnosticCode(nil), node.Diagnostics...)
		cloned = append(cloned, node)
	}
	return cloned
}

func clonePhysicalPlanExplanation(explanation PhysicalPlanExplanation) PhysicalPlanExplanation {
	return PhysicalPlanExplanation{
		Supported:   explanation.Supported,
		Diagnostics: cloneDiagnosticSet(explanation.Diagnostics),
		Nodes:       clonePhysicalNodeExplanations(explanation.Nodes),
	}
}

func clonePhysicalNodeExplanations(nodes []PhysicalNodeExplanation) []PhysicalNodeExplanation {
	if len(nodes) == 0 {
		return nil
	}
	cloned := make([]PhysicalNodeExplanation, 0, len(nodes))
	for _, node := range nodes {
		node.Fields = append([]string(nil), node.Fields...)
		node.Strategies = append([]PhysicalStrategy(nil), node.Strategies...)
		node.Scope.Shards = append([]string(nil), node.Scope.Shards...)
		node.Scope.Replicas = append([]string(nil), node.Scope.Replicas...)
		node.Join.On.Capabilities = append([]PlanCapability(nil), node.Join.On.Capabilities...)
		node.Join.Capabilities = append([]PlanCapability(nil), node.Join.Capabilities...)
		node.Diagnostics = append([]DiagnosticCode(nil), node.Diagnostics...)
		cloned = append(cloned, node)
	}
	return cloned
}
