package qsruntime

import (
	"context"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestExecutionServiceInspectReportsDirectCallPlan(t *testing.T) {
	service := NewExecutionService(ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute direct executor")
		return ExecutionResult{}, nil
	}), nil)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
		}},
	})

	inspection := service.Inspect(request)
	if !inspection.Supported() {
		t.Fatalf("inspection diagnostics = %#v", inspection.Diagnostics)
	}
	if inspection.SelectedExecutor != ExecutionInspectionExecutorDirect {
		t.Fatalf("selected executor = %q, want direct", inspection.SelectedExecutor)
	}
	if inspection.RuntimeProfile.Implementation != "" {
		t.Fatalf("runtime implementation = %q, want empty service-level profile", inspection.RuntimeProfile.Implementation)
	}
	if inspection.CallPlan.RootIndex != "orders" {
		t.Fatalf("root index = %q, want orders", inspection.CallPlan.RootIndex)
	}
	if !inspection.CallPlan.Contains(LegacyExecutionStepQueryBitIndex) {
		t.Fatalf("call plan missing bit index query step: %v", inspection.CallPlan.Steps)
	}
}

func TestExecutionServiceInspectReportsLegacyRoute(t *testing.T) {
	service := NewExecutionService(nil, ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute legacy executor")
		return ExecutionResult{}, nil
	}))
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"partsupp"}
	request = request.WithRoute(LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a"}))

	inspection := service.Inspect(request)
	if !inspection.Supported() {
		t.Fatalf("inspection diagnostics = %#v", inspection.Diagnostics)
	}
	if inspection.SelectedExecutor != ExecutionInspectionExecutorLegacy {
		t.Fatalf("selected executor = %q, want legacy", inspection.SelectedExecutor)
	}
	if inspection.CallPlan.RootIndex != "partsupp" {
		t.Fatalf("root index = %q, want partsupp", inspection.CallPlan.RootIndex)
	}
}

func TestExecutionServiceInspectReportsRelationshipVectorJoinBoundary(t *testing.T) {
	service := NewExecutionService(ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute direct executor")
		return ExecutionResult{}, nil
	}), nil)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "lineitem"}},
	})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_orderkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "orders", Alias: "o"},
			Name:  "o_orderkey",
		},
		Kind: qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{
			Kind: qsbridge.RelationshipEncodingVector,
			Capabilities: qsbridge.RelationshipCapabilities{
				qsbridge.RelationshipCapabilityParentLookup,
				qsbridge.RelationshipCapabilityJoinReduction,
			},
		},
		Legal: true,
	}}

	inspection := service.Inspect(request)
	if inspection.Supported() {
		t.Fatalf("inspection supported = true, want false")
	}
	if inspection.SelectedExecutor != ExecutionInspectionExecutorDirect {
		t.Fatalf("selected executor = %q, want direct", inspection.SelectedExecutor)
	}
	if got := inspection.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticUnsupportedJoin {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticUnsupportedJoin)
	}
	if len(inspection.Joins) != 1 {
		t.Fatalf("joins = %#v, want one join inspection", inspection.Joins)
	}
	if got := inspection.Joins[0].JoinKind; got != "relationship_vector" {
		t.Fatalf("join kind = %q, want relationship_vector", got)
	}
	if got := inspection.Joins[0].ExecutionStatus; got != ExecutionJoinStatusNotWiredYet {
		t.Fatalf("join execution status = %q, want %q", got, ExecutionJoinStatusNotWiredYet)
	}
	if inspection.CallPlan.RootIndex != "" {
		t.Fatalf("root index = %q, want empty call plan", inspection.CallPlan.RootIndex)
	}
}

func TestExecutionServiceInspectReportsGroupedAggregateTopNShape(t *testing.T) {
	service := NewExecutionService(ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute direct executor")
		return ExecutionResult{}, nil
	}), nil)
	partKey := qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "partsupp"}, Name: "ps_partkey"}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "partsupp", Field: "ps_partkey"}},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.FieldExpr{Ref: partKey}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "sum", Alias: "part_value"}}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.AggregateRefExpr{Alias: "part_value", Index: 0}, Direction: qsbridge.SortDescending}}
	request.Result.Limit = 10

	inspection := service.Inspect(request)
	if !inspection.Shape.GroupedAggregateTopNCandidate {
		t.Fatalf("grouped top-N shape not detected: %#v", inspection.Shape)
	}
	if inspection.Shape.GroupedAggregateTopNDetail == "" {
		t.Fatalf("grouped top-N detail is empty")
	}
}

func TestExecutionServiceInspectCarriesFilterDomainTranslation(t *testing.T) {
	service := NewExecutionService(ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute direct executor")
		return ExecutionResult{}, nil
	}), nil)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterUnion,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_partkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		Kind:     qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{Kind: qsbridge.RelationshipEncodingVector},
	}}

	inspection := service.Inspect(request)
	if !inspection.FilterDomain.Required {
		t.Fatalf("filter domain = %#v, want translation required", inspection.FilterDomain)
	}
	if inspection.FilterDomain.TargetDomain != "lineitem" {
		t.Fatalf("target domain = %q, want lineitem", inspection.FilterDomain.TargetDomain)
	}
	if len(inspection.FilterDomain.SourceDomains) != 2 || inspection.FilterDomain.SourceDomains[0] != "lineitem" || inspection.FilterDomain.SourceDomains[1] != "part" {
		t.Fatalf("source domains = %#v, want lineitem/part", inspection.FilterDomain.SourceDomains)
	}
	if !inspection.FilterDomainPlan.Required() {
		t.Fatalf("filter-domain plan = %#v, want required", inspection.FilterDomainPlan)
	}
	if len(inspection.FilterDomainPlan.Requests) != 1 {
		t.Fatalf("filter-domain requests = %#v, want one", inspection.FilterDomainPlan.Requests)
	}
	if len(inspection.FilterDomainPlan.Requests[0].RelationshipPath) != 1 {
		t.Fatalf("relationship path = %#v, want one-hop path", inspection.FilterDomainPlan.Requests[0].RelationshipPath)
	}
	if inspection.Supported() {
		t.Fatalf("inspection should block until filter-domain translation is executable")
	}
	if got := inspection.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticUnsupportedSQL)
	}
	for _, want := range []string{
		"sources=lineitem,part",
		"target=lineitem",
		"strategies=relationship_vector_normalization",
	} {
		if !strings.Contains(inspection.Diagnostics[0].Message, want) {
			t.Fatalf("diagnostic message = %q, want %q", inspection.Diagnostics[0].Message, want)
		}
	}
}

func TestExecutionServiceInspectReportsFoundsetFollowUpShape(t *testing.T) {
	service := NewExecutionService(ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute direct executor")
		return ExecutionResult{}, nil
	}), nil)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "lineitem", Field: "l_shipdate"}},
	})
	request.Joins = []qsbridge.JoinEdge{{
		Left: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_suppkey",
		},
		Right: qsbridge.FieldRef{
			Table: qsbridge.TableInstance{Table: "supplier", Alias: "s"},
			Name:  "s_suppkey",
		},
		Kind: qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{
			Kind: qsbridge.RelationshipEncodingVector,
			Capabilities: qsbridge.RelationshipCapabilities{
				qsbridge.RelationshipCapabilityParentLookup,
				qsbridge.RelationshipCapabilityJoinReduction,
			},
		},
		Legal: true,
	}}

	inspection := service.Inspect(request)
	if !inspection.Shape.FoundsetFollowUpCandidate {
		t.Fatalf("foundset follow-up shape not detected: %#v", inspection.Shape)
	}
	if inspection.Shape.FoundsetFollowUpDetail != "fragment=lineitem.l_shipdate edges=1" {
		t.Fatalf("foundset follow-up detail = %q", inspection.Shape.FoundsetFollowUpDetail)
	}
}

func TestExecutionServiceInspectReportsCatalogViewSummary(t *testing.T) {
	service := NewExecutionService(ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute direct executor")
		return ExecutionResult{}, nil
	}), nil)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "orders", Field: "o_orderkey"}},
	})
	table := qsbridge.TableDefinition{
		Schema: "quanta",
		Name:   "orders",
		Fields: []qsbridge.FieldDefinition{
			{Name: "o_orderkey", Type: qsbridge.DataTypeInt},
			{
				Name: "o_orderpriority",
				Type: qsbridge.DataTypeString,
				Dictionary: qsbridge.DictionaryDefinition{
					Ref: qsbridge.DictionaryRef{Table: "orders", Field: "o_orderpriority"},
				},
			},
		},
		Relationships: []qsbridge.RelationshipDefinition{{Name: "orders.customer"}},
	}
	request.NodeCatalog = qsbridge.NewNodeCatalogView([]qsbridge.TableDefinition{table})
	request.QueryCatalog = qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{table}, nil, []qsbridge.FunctionDefinition{{Name: "substr"}})

	inspection := service.Inspect(request)

	if !inspection.Supported() {
		t.Fatalf("inspection diagnostics = %#v", inspection.Diagnostics)
	}
	if inspection.Catalog.NodeTableCount != 1 || inspection.Catalog.NodeFieldCount != 2 ||
		inspection.Catalog.NodeRelationshipCount != 1 {
		t.Fatalf("node catalog summary = %#v, want one table, two fields, one relationship", inspection.Catalog)
	}
	if inspection.Catalog.QueryTableCount != 1 || inspection.Catalog.QueryFieldCount != 2 ||
		inspection.Catalog.QueryRelationshipCount != 1 || inspection.Catalog.QueryFunctionCount != 1 ||
		inspection.Catalog.QueryDictionaryFieldCount != 1 {
		t.Fatalf("query catalog summary = %#v, want semantic catalog counts", inspection.Catalog)
	}
}

func TestExecutionServiceInspectReportsMaterializationCapability(t *testing.T) {
	service := NewExecutionService(ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute direct executor")
		return ExecutionResult{}, nil
	}), nil)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "orders", Field: "o_orderkey"}},
	})
	request.Materialization = qsbridge.QuantaMaterializationRequest{
		Index: "orders",
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Field: "o_orderkey", Type: qsbridge.DataTypeInt},
			{Field: "o_orderpriority", Type: qsbridge.DataTypeString},
		},
	}

	inspection := service.Inspect(request)

	if !inspection.Supported() {
		t.Fatalf("inspection diagnostics = %#v", inspection.Diagnostics)
	}
	if inspection.Materialization.NativeFieldCount != 1 || inspection.Materialization.CompatFallbackFieldCount != 1 {
		t.Fatalf("materialization capability = %#v, want one native and one fallback", inspection.Materialization)
	}
	if !inspection.Materialization.LegacyMaterializerReachable || inspection.Materialization.LegacyMaterializerUsed {
		t.Fatalf("legacy materializer visibility = reachable %v used %v, want reachable but not used during inspect", inspection.Materialization.LegacyMaterializerReachable, inspection.Materialization.LegacyMaterializerUsed)
	}
	if inspection.Materialization.Fields[1].LookupKind != NativeProjectionLookupBackingString {
		t.Fatalf("string capability = %#v, want backing-string lookup boundary", inspection.Materialization.Fields[1])
	}
	if inspection.Materialization.Fields[1].Source != "kvstore_needed" ||
		inspection.Materialization.Fields[1].ReasonCode != ProjectionMaterializationReasonBackingStringKV {
		t.Fatalf("string capability = %#v, want explicit KVStore fallback reason", inspection.Materialization.Fields[1])
	}
}

func TestExecutionServiceInspectReturnsSelectorDiagnostics(t *testing.T) {
	inspection := NewExecutionService(nil, nil).Inspect(NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if inspection.Supported() {
		t.Fatalf("inspection supported = true, want false")
	}
	if got := inspection.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
	if inspection.CallPlan.RootIndex != "" {
		t.Fatalf("root index = %q, want empty", inspection.CallPlan.RootIndex)
	}
}

func TestExecutionServiceInspectReturnsCallPlanDiagnostics(t *testing.T) {
	service := NewExecutionService(ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute direct executor")
		return ExecutionResult{}, nil
	}), nil)

	inspection := service.Inspect(NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if inspection.Supported() {
		t.Fatalf("inspection supported = true, want false")
	}
	if got := inspection.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticInvalidExecutionOption {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInvalidExecutionOption)
	}
}
