package qsfixture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
)

type runtimeFixtureRow map[string]qsbridge.ResultCell

type runtimeFixtureCandidateRows struct {
	Set  qsbridge.QuantaCandidateSet
	Rows []runtimeFixtureRow
}

type runtimeFixtureStore struct {
	mu   sync.RWMutex
	rows map[string][]runtimeFixtureRow
}

var runtimeFixtureOrders = []runtimeFixtureRow{
	{
		"o_orderkey":      {Kind: qsbridge.ValueInt, Value: int64(1001)},
		"o_custkey":       {Kind: qsbridge.ValueInt, Value: int64(501)},
		"o_totalprice":    {Kind: qsbridge.ValueFloat, Value: float64(101.50)},
		"o_orderpriority": {Kind: qsbridge.ValueString, Value: "1-URGENT"},
	},
	{
		"o_orderkey":      {Kind: qsbridge.ValueInt, Value: int64(1002)},
		"o_custkey":       {Kind: qsbridge.ValueInt, Value: int64(502)},
		"o_totalprice":    {Kind: qsbridge.ValueFloat, Value: float64(88.25)},
		"o_orderpriority": {Kind: qsbridge.ValueString, Value: "3-MEDIUM"},
	},
	{
		"o_orderkey":      {Kind: qsbridge.ValueInt, Value: int64(1003)},
		"o_custkey":       {Kind: qsbridge.ValueInt, Value: int64(501)},
		"o_totalprice":    {Kind: qsbridge.ValueFloat, Value: float64(203.75)},
		"o_orderpriority": {Kind: qsbridge.ValueString, Value: "5-LOW"},
	},
}

// NewSQLRuntime builds the deterministic runtime fixture used by SQLRunner smoke suites.
func NewSQLRuntime(ctx context.Context) (qsruntime.SQLRuntime, error) {
	store := newRuntimeFixtureStore()
	builder := qsruntime.SQLRuntimeBuilder{
		Parser:                  qsbridge.SimpleParserBridge{},
		Lowerer:                 qsbridge.QuantaIntermediateLowerer{},
		DefaultSchema:           "quanta",
		CatalogVersion:          qsbridge.CatalogVersion("sqlrunner-runtime-fixture-v1"),
		EnableFilterExpressions: true,
		EnvironmentBuilder: qsruntime.RuntimeEnvironmentBuilder{
			Config:  qsruntime.NewDirectRuntimeConfig("", "", 0, 0),
			Profile: qsruntime.FixtureRuntimeProfile("sqlrunner-runtime-fixture-v1"),
			CatalogFactory: qsruntime.RuntimeCatalogFactoryFunc(func(context.Context, qsruntime.DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error) {
				return runtimeFixtureCatalog(), nil, nil
			}),
			DirectFactory: qsruntime.DirectRuntimeFactoryFunc(func(context.Context, qsruntime.DirectRuntimeConfig) (qsruntime.DirectRuntime, qsbridge.DiagnosticSet, error) {
				return qsruntime.DirectRuntimeFunc(store.ExecuteDirect), nil, nil
			}),
		},
	}
	runtime, diagnostics, err := builder.Build(ctx)
	if err != nil {
		return qsruntime.SQLRuntime{}, err
	}
	if diagnostics.BlocksNative() {
		return qsruntime.SQLRuntime{}, runtimeFixtureDiagnosticsError(diagnostics)
	}
	return runtime, nil
}

func newRuntimeFixtureStore() *runtimeFixtureStore {
	return &runtimeFixtureStore{
		rows: map[string][]runtimeFixtureRow{
			"orders":           cloneRuntimeFixtureRows(runtimeFixtureOrders),
			"customers_qa":     nil,
			"orders_qa":        nil,
			"lineitems_qa":     nil,
			"deliveries_qa":    nil,
			"part_qa":          nil,
			"partsupp_qa":      nil,
			"supplier_qa":      nil,
			"customer_tpch_qa": nil,
			"orders_tpch_qa":   nil,
			"lineitem_tpch_qa": nil,
		},
	}
}

func runtimeFixtureCatalog() qsbridge.Catalog {
	return qsbridge.NewCachedCatalog(qsbridge.MemoryCatalog{
		Tables: []qsbridge.TableDefinition{
			{
				Schema: "quanta",
				Name:   "orders",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("o_orderkey", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, true),
					runtimeFixtureField("o_custkey", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
					runtimeFixtureField("o_totalprice", qsbridge.DataTypeFloat, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
					runtimeFixtureField("o_orderpriority", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
				},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "customers_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("cust_id", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, true),
					runtimeFixtureField("first_name", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("last_name", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("address", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("city", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("state", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
					runtimeFixtureField("zip", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("phone", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("phoneType", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
					runtimeFixtureField("createdAtTimestamp", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("timestamp_micro", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("timestamp_millis", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("hashedCustId", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("isActive", qsbridge.DataTypeBool, qsbridge.IndexBitmap, qsbridge.EncodingBitmap, false),
					runtimeFixtureField("birthdate", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("isLegalAge", qsbridge.DataTypeBool, qsbridge.IndexBitmap, qsbridge.EncodingBitmap, false),
					runtimeFixtureField("age", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
					runtimeFixtureScaledNumericField("height", 2),
					runtimeFixtureField("numFamilyMembers", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
					runtimeFixtureField("rownum", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
				},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "orders_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("cust_id", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("order_id", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, true),
					runtimeFixtureField("order_date", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("ship_date", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("ship_via", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
				},
				Relationships: []qsbridge.RelationshipDefinition{{
					Name:        "orders_qa_customers_qa",
					FromTable:   "orders_qa",
					FromField:   "cust_id",
					ToTable:     "customers_qa",
					ToField:     "cust_id",
					Direction:   qsbridge.JoinChildToParent,
					Cardinality: "many_to_one",
					Encoding: qsbridge.RelationshipEncodingProfile{
						Kind:       qsbridge.RelationshipEncodingVector,
						LegacyName: "ParentRelation",
						Capabilities: qsbridge.RelationshipCapabilities{
							qsbridge.RelationshipCapabilityParentLookup,
							qsbridge.RelationshipCapabilityJoinReduction,
						},
					},
				}},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "lineitems_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("line_id", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, true),
					runtimeFixtureField("order_id", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("category", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
					runtimeFixtureField("quantity", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
				},
				Relationships: []qsbridge.RelationshipDefinition{{
					Name:        "lineitems_qa_orders_qa",
					FromTable:   "lineitems_qa",
					FromField:   "order_id",
					ToTable:     "orders_qa",
					ToField:     "order_id",
					Direction:   qsbridge.JoinChildToParent,
					Cardinality: "many_to_one",
					Encoding: qsbridge.RelationshipEncodingProfile{
						Kind:       qsbridge.RelationshipEncodingVector,
						LegacyName: "ParentRelation",
						Capabilities: qsbridge.RelationshipCapabilities{
							qsbridge.RelationshipCapabilityParentLookup,
							qsbridge.RelationshipCapabilityJoinReduction,
						},
					},
				}},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "deliveries_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("delivery_id", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, true),
					runtimeFixtureField("line_id", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("status", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
				},
				Relationships: []qsbridge.RelationshipDefinition{{
					Name:        "deliveries_qa_lineitems_qa",
					FromTable:   "deliveries_qa",
					FromField:   "line_id",
					ToTable:     "lineitems_qa",
					ToField:     "line_id",
					Direction:   qsbridge.JoinChildToParent,
					Cardinality: "many_to_one",
					Encoding: qsbridge.RelationshipEncodingProfile{
						Kind:       qsbridge.RelationshipEncodingVector,
						LegacyName: "ParentRelation",
						Capabilities: qsbridge.RelationshipCapabilities{
							qsbridge.RelationshipCapabilityParentLookup,
							qsbridge.RelationshipCapabilityJoinReduction,
						},
					},
				}},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "part_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("p_partkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, true),
					runtimeFixtureField("p_brand", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
					runtimeFixtureField("p_type", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("p_container", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
					runtimeFixtureField("p_size", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
				},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "partsupp_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("ps_partkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("ps_suppkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureScaledNumericField("ps_supplycost", 2),
					runtimeFixtureField("ps_availqty", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
				},
				Relationships: []qsbridge.RelationshipDefinition{
					{
						Name:        "partsupp_qa_part_qa",
						FromTable:   "partsupp_qa",
						FromField:   "ps_partkey",
						ToTable:     "part_qa",
						ToField:     "p_partkey",
						Direction:   qsbridge.JoinChildToParent,
						Cardinality: "many_to_one",
						Encoding: qsbridge.RelationshipEncodingProfile{
							Kind:       qsbridge.RelationshipEncodingVector,
							LegacyName: "ParentRelation",
							Capabilities: qsbridge.RelationshipCapabilities{
								qsbridge.RelationshipCapabilityParentLookup,
								qsbridge.RelationshipCapabilityJoinReduction,
							},
						},
					},
					{
						Name:        "partsupp_qa_supplier_qa",
						FromTable:   "partsupp_qa",
						FromField:   "ps_suppkey",
						ToTable:     "supplier_qa",
						ToField:     "s_suppkey",
						Direction:   qsbridge.JoinChildToParent,
						Cardinality: "many_to_one",
						Encoding: qsbridge.RelationshipEncodingProfile{
							Kind:       qsbridge.RelationshipEncodingVector,
							LegacyName: "ParentRelation",
							Capabilities: qsbridge.RelationshipCapabilities{
								qsbridge.RelationshipCapabilityParentLookup,
								qsbridge.RelationshipCapabilityJoinReduction,
							},
						},
					},
				},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "supplier_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("s_suppkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, true),
					runtimeFixtureField("s_comment", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
				},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "customer_tpch_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("c_custkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, true),
					runtimeFixtureField("c_name", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("c_phone", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureScaledNumericField("c_acctbal", 2),
					runtimeFixtureField("c_mktsegment", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
				},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "orders_tpch_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("o_orderkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, true),
					runtimeFixtureField("o_custkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("o_orderdate", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("o_orderpriority", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
				},
				Relationships: []qsbridge.RelationshipDefinition{{
					Name:        "orders_tpch_qa_customer_tpch_qa",
					FromTable:   "orders_tpch_qa",
					FromField:   "o_custkey",
					ToTable:     "customer_tpch_qa",
					ToField:     "c_custkey",
					Direction:   qsbridge.JoinChildToParent,
					Cardinality: "many_to_one",
					Encoding: qsbridge.RelationshipEncodingProfile{
						Kind:       qsbridge.RelationshipEncodingVector,
						LegacyName: "ParentRelation",
						Capabilities: qsbridge.RelationshipCapabilities{
							qsbridge.RelationshipCapabilityParentLookup,
							qsbridge.RelationshipCapabilityJoinReduction,
						},
					},
				}},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
			{
				Schema: "quanta",
				Name:   "lineitem_tpch_qa",
				Fields: []qsbridge.FieldDefinition{
					runtimeFixtureField("l_orderkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("l_partkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("l_suppkey", qsbridge.DataTypeString, qsbridge.IndexBackingString, qsbridge.EncodingBackingString, false),
					runtimeFixtureField("l_shipdate", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("l_commitdate", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureField("l_receiptdate", qsbridge.DataTypeTime, qsbridge.IndexDateTime, qsbridge.EncodingTimeBSI, false),
					runtimeFixtureScaledNumericField("l_extendedprice", 2),
					runtimeFixtureScaledNumericField("l_discount", 2),
					runtimeFixtureScaledNumericField("l_tax", 2),
					runtimeFixtureField("l_quantity", qsbridge.DataTypeInt, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false),
					runtimeFixtureField("l_shipmode", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
					runtimeFixtureField("l_shipinstruct", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
					runtimeFixtureField("l_returnflag", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
					runtimeFixtureField("l_linestatus", qsbridge.DataTypeString, qsbridge.IndexStringEnum, qsbridge.EncodingStringEnum, false),
				},
				Relationships: []qsbridge.RelationshipDefinition{
					{
						Name:        "lineitem_tpch_qa_orders_tpch_qa",
						FromTable:   "lineitem_tpch_qa",
						FromField:   "l_orderkey",
						ToTable:     "orders_tpch_qa",
						ToField:     "o_orderkey",
						Direction:   qsbridge.JoinChildToParent,
						Cardinality: "many_to_one",
						Encoding: qsbridge.RelationshipEncodingProfile{
							Kind:       qsbridge.RelationshipEncodingVector,
							LegacyName: "ParentRelation",
							Capabilities: qsbridge.RelationshipCapabilities{
								qsbridge.RelationshipCapabilityParentLookup,
								qsbridge.RelationshipCapabilityJoinReduction,
							},
						},
					},
					{
						Name:        "lineitem_tpch_qa_part_qa",
						FromTable:   "lineitem_tpch_qa",
						FromField:   "l_partkey",
						ToTable:     "part_qa",
						ToField:     "p_partkey",
						Direction:   qsbridge.JoinChildToParent,
						Cardinality: "many_to_one",
						Encoding: qsbridge.RelationshipEncodingProfile{
							Kind:       qsbridge.RelationshipEncodingVector,
							LegacyName: "ParentRelation",
							Capabilities: qsbridge.RelationshipCapabilities{
								qsbridge.RelationshipCapabilityParentLookup,
								qsbridge.RelationshipCapabilityJoinReduction,
							},
						},
					},
				},
				Storage: qsbridge.StorageProfile{Engine: "runtime_fixture"},
			},
		},
		Functions: []qsbridge.FunctionDefinition{
			runtimeFixtureAggregateFunction("count", qsbridge.DataTypeInt),
			runtimeFixtureAggregateFunction("sum", qsbridge.DataTypeFloat),
			runtimeFixtureAggregateFunction("min", qsbridge.DataTypeFloat),
			runtimeFixtureAggregateFunction("max", qsbridge.DataTypeFloat),
			runtimeFixtureAggregateFunction("avg", qsbridge.DataTypeFloat),
			runtimeFixtureAggregateFunction("topn", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("todate", qsbridge.DataTypeTime),
			runtimeFixtureScalarFunction("tostring", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("toint", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("tonumber", qsbridge.DataTypeFloat),
			runtimeFixtureScalarFunction("lower", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("lcase", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("upper", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("ucase", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("length", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("char_length", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("substr", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("substring", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("mid", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("timediff", qsbridge.DataTypeFloat),
			runtimeFixtureScalarFunction("hash.sha256", qsbridge.DataTypeString),
			runtimeFixtureScalarFunction("year", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("yy", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("mm", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("monthofyear", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("dayofweek", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("hourofday", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("hourofweek", qsbridge.DataTypeInt),
			runtimeFixtureScalarFunction("seconds", qsbridge.DataTypeInt),
		},
	})
}

func runtimeFixtureAggregateFunction(name string, returnType qsbridge.DataType) qsbridge.FunctionDefinition {
	return qsbridge.FunctionDefinition{
		Name:          name,
		Kind:          qsbridge.FunctionAggregate,
		Origin:        qsbridge.FunctionOriginMySQLCompatible,
		Placement:     qsbridge.FunctionPlacementAggregate,
		ReturnType:    returnType,
		Native:        true,
		Deterministic: true,
	}
}

func runtimeFixtureScalarFunction(name string, returnType qsbridge.DataType) qsbridge.FunctionDefinition {
	return qsbridge.FunctionDefinition{
		Name:          name,
		Kind:          qsbridge.FunctionScalar,
		Origin:        qsbridge.FunctionOriginQuantaCustom,
		Placement:     qsbridge.FunctionPlacementExpression,
		ReturnType:    returnType,
		Native:        true,
		Deterministic: true,
	}
}

func runtimeFixtureField(name string, dataType qsbridge.DataType, index qsbridge.IndexKind, encoding qsbridge.EncodingKind, primaryKey bool) qsbridge.FieldDefinition {
	return qsbridge.FieldDefinition{
		Name:       name,
		Type:       dataType,
		Index:      index,
		PrimaryKey: primaryKey,
		Storage: qsbridge.StorageProfile{
			Engine: "runtime_fixture",
			Index:  index,
		},
		Encoding: qsbridge.EncodingProfile{Kind: encoding},
	}
}

func runtimeFixtureScaledNumericField(name string, scale int) qsbridge.FieldDefinition {
	field := runtimeFixtureField(name, qsbridge.DataTypeFloat, qsbridge.IndexBSI, qsbridge.EncodingNumericBSI, false)
	field.Encoding = qsbridge.NewNumericBSIProfile(scale, true)
	return field
}

func (s *runtimeFixtureStore) tableRows(table string) ([]runtimeFixtureRow, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, ok := s.rows[strings.ToLower(table)]
	if !ok {
		return nil, false
	}
	return cloneRuntimeFixtureRows(rows), true
}

func (s *runtimeFixtureStore) insertRows(ctx context.Context, request qsruntime.ExecutionRequest) (qsruntime.ExecutionResult, error) {
	if request.Mutation.Kind != qsbridge.MutationInsert {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "runtime fixture only supports INSERT mutations"),
			},
		}, nil
	}
	table := strings.ToLower(request.Mutation.Target.Table)
	if table == "" {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, "insert mutation has no target table"),
			},
		}, nil
	}
	inserted := make([]runtimeFixtureRow, 0, len(request.Mutation.Rows))
	for _, row := range request.Mutation.Rows {
		if err := ctx.Err(); err != nil {
			return qsruntime.ExecutionResult{}, err
		}
		converted, diagnostics, ok := runtimeFixtureInsertRow(table, request.Mutation.Columns, row)
		if !ok {
			return qsruntime.ExecutionResult{Diagnostics: diagnostics}, nil
		}
		inserted = append(inserted, converted)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[table]; !ok {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogTableNotFound, qsbridge.PhaseExecute, "runtime fixture unsupported insert table: "+table),
			},
		}, nil
	}
	s.rows[table] = append(s.rows[table], inserted...)
	affected := uint64(len(inserted))
	return qsruntime.ExecutionResult{
		Statement: qsbridge.StatementResult{
			AffectedRows: affected,
			LastInsertID: uint64(len(s.rows[table])),
			Status:       fmt.Sprintf("Records: %d", affected),
		},
	}, nil
}

func runtimeFixtureInsertRow(table string, columns []qsbridge.FieldRef, row qsbridge.MutationRow) (runtimeFixtureRow, qsbridge.DiagnosticSet, bool) {
	if len(columns) != len(row.Values) {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "insert row value count does not match target column count"),
		}, false
	}
	converted := make(runtimeFixtureRow, len(columns))
	for index, column := range columns {
		cell, diagnostics, ok := runtimeFixtureInsertCell(row.Values[index])
		if !ok {
			return nil, diagnostics, false
		}
		if runtimeFixtureTimeField(column) {
			cell = runtimeFixtureNormalizeTimeCell(cell)
		}
		converted[column.Name] = cell
	}
	runtimeFixtureApplyGeneratedDefaults(table, converted)
	return converted, nil, true
}

func runtimeFixtureTimeField(field qsbridge.FieldRef) bool {
	if field.Type == qsbridge.DataTypeTime {
		return true
	}
	for _, name := range []string{field.Name, field.PhysicalName, field.QualifiedName()} {
		for _, candidate := range []string{"createdAtTimestamp", "timestamp_micro", "timestamp_millis", "birthdate", "order_date", "ship_date", "o_orderdate", "l_shipdate", "l_commitdate", "l_receiptdate"} {
			if strings.EqualFold(name, candidate) || strings.HasSuffix(strings.ToLower(name), "."+strings.ToLower(candidate)) {
				return true
			}
		}
	}
	return false
}

func runtimeFixtureApplyGeneratedDefaults(table string, row runtimeFixtureRow) {
	if !strings.EqualFold(table, "customers_qa") {
		return
	}
	runtimeFixtureEnsureNonNull(row, "createdAtTimestamp", qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: runtimeFixtureReferenceNow().UnixMilli()})
	runtimeFixtureEnsureNonNull(row, "hashedCustId", qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "fixture-hash"})
	runtimeFixtureEnsureNonNull(row, "isActive", qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: true})
	runtimeFixtureEnsureNonNull(row, "isLegalAge", qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: false})
	runtimeFixtureEnsureNonNull(row, "age", qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(0)})
	runtimeFixtureEnsureNonNull(row, "numFamilyMembers", qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(4)})
}

func runtimeFixtureEnsureNonNull(row runtimeFixtureRow, field string, value qsbridge.ResultCell) {
	cell, ok := row[field]
	if !ok || cell.Kind == qsbridge.ValueNull || (cell.Kind == qsbridge.ValueString && cell.Value == "") {
		row[field] = value
	}
}

func runtimeFixtureInsertCell(expr qsbridge.Expr) (qsbridge.ResultCell, qsbridge.DiagnosticSet, bool) {
	literal, ok := runtimeFixtureLiteralExpr(expr)
	if !ok {
		return qsbridge.ResultCell{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "runtime fixture INSERT only supports literal row values"),
		}, false
	}
	return qsbridge.ResultCell{Kind: literal.Kind, Value: literal.Value}, nil, true
}

func runtimeFixtureNormalizeTimeCell(cell qsbridge.ResultCell) qsbridge.ResultCell {
	if cell.Kind == qsbridge.ValueNull {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(0)}
	}
	text, ok := cell.Value.(string)
	if !ok {
		return cell
	}
	if strings.TrimSpace(text) == "" {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
	millis, ok := runtimeFixtureParseTimeMillis(text)
	if !ok {
		return cell
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: millis}
}

func runtimeFixtureParseTimeMillis(text string) (int64, bool) {
	if millis, ok := runtimeFixtureParseRelativeTimeMillis(text); ok {
		return millis, true
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		parsed, err := time.Parse(layout, strings.TrimSpace(text))
		if err == nil {
			return parsed.UTC().UnixMilli(), true
		}
	}
	return 0, false
}

func runtimeFixtureParseRelativeTimeMillis(text string) (int64, bool) {
	trimmed := strings.ToLower(strings.TrimSpace(text))
	if trimmed == "now" {
		return runtimeFixtureReferenceNow().UnixMilli(), true
	}
	if !strings.HasPrefix(trimmed, "now") || len(trimmed) < 6 {
		return 0, false
	}
	sign := trimmed[3]
	if sign != '-' && sign != '+' {
		return 0, false
	}
	amountText := trimmed[4 : len(trimmed)-1]
	unit := trimmed[len(trimmed)-1]
	amount, err := strconv.Atoi(amountText)
	if err != nil || amount < 0 {
		return 0, false
	}
	if sign == '-' {
		amount = -amount
	}
	now := runtimeFixtureReferenceNow()
	switch unit {
	case 'd':
		return now.AddDate(0, 0, amount).UnixMilli(), true
	case 'm':
		return now.AddDate(0, amount, 0).UnixMilli(), true
	case 'y':
		return now.AddDate(amount, 0, 0).UnixMilli(), true
	case 'h':
		return now.Add(time.Duration(amount) * time.Hour).UnixMilli(), true
	default:
		return 0, false
	}
}

func runtimeFixtureReferenceNow() time.Time {
	return time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC)
}

func runtimeFixtureLiteralExpr(expr qsbridge.Expr) (qsbridge.LiteralExpr, bool) {
	switch value := expr.(type) {
	case qsbridge.LiteralExpr:
		return value, true
	case *qsbridge.LiteralExpr:
		if value == nil {
			return qsbridge.LiteralExpr{}, false
		}
		return *value, true
	default:
		return qsbridge.LiteralExpr{}, false
	}
}

func cloneRuntimeFixtureRows(rows []runtimeFixtureRow) []runtimeFixtureRow {
	cloned := make([]runtimeFixtureRow, len(rows))
	for rowIndex, row := range rows {
		cloned[rowIndex] = make(runtimeFixtureRow, len(row))
		for key, value := range row {
			cloned[rowIndex][key] = value
		}
	}
	return cloned
}

func (s *runtimeFixtureStore) ExecuteDirect(ctx context.Context, request qsruntime.ExecutionRequest) (qsruntime.ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return qsruntime.ExecutionResult{}, err
	}
	index, ok := request.RootIndex()
	if !ok {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogTableNotFound, qsbridge.PhaseExecute, "runtime fixture request has no root table"),
			},
		}, nil
	}
	if request.Mutation.Kind != qsbridge.MutationUnknown {
		return s.insertRows(ctx, request)
	}
	candidates, err := s.selectCandidates(ctx, index, request)
	if err != nil {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, err.Error()),
			},
		}, nil
	}
	candidates, err = s.applyMemberships(ctx, request.Memberships, candidates)
	if err != nil {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, err.Error()),
			},
		}, nil
	}
	candidates, err = runtimeFixtureApplyResidualPredicates(request.Predicates, candidates)
	if err != nil {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, err.Error()),
			},
		}, nil
	}
	if len(request.SQLAggregates) > 0 {
		if len(request.GroupBy) > 0 {
			rowSet, err := runtimeFixtureGroupedAggregateRows(request, candidates.Rows)
			if err != nil {
				return qsruntime.ExecutionResult{
					Diagnostics: qsbridge.DiagnosticSet{
						qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, err.Error()),
					},
				}, nil
			}
			return qsruntime.ExecutionResult{
				RowSet: rowSet,
				Count:  uint64(rowSet.CandidateCount()),
			}, nil
		}
		rowSet, err := runtimeFixtureAggregateRows(request.SQLAggregates, candidates.Rows)
		if err != nil {
			return qsruntime.ExecutionResult{
				Diagnostics: qsbridge.DiagnosticSet{
					qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, err.Error()),
				},
			}, nil
		}
		return qsruntime.ExecutionResult{
			RowSet: rowSet,
			Count:  uint64(rowSet.CandidateCount()),
		}, nil
	}
	candidates, err = runtimeFixtureSortCandidates(request.OrderBy, candidates)
	if err != nil {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, err.Error()),
			},
		}, nil
	}
	candidates = runtimeFixtureLimitCandidates(candidates, request.Result.Offset, request.Result.Limit)
	candidates = runtimeFixtureLimitCandidates(candidates, 0, request.Options.MaxRows)
	materialization := candidates.Set.MaterializationRequest(request.Query.ProjectionFields)
	rowSet, err := runtimeFixtureMaterializeRows(materialization, candidates.Rows)
	if err != nil {
		return qsruntime.ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, err.Error()),
			},
		}, nil
	}
	if len(request.Projection) > 0 {
		rowSet, err = runtimeFixtureProjectSQLRows(request.Projection, rowSet)
		if err != nil {
			return qsruntime.ExecutionResult{
				Diagnostics: qsbridge.DiagnosticSet{
					qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, err.Error()),
				},
			}, nil
		}
	}
	if request.Result.Distinct {
		rowSet = runtimeFixtureDistinctRows(rowSet)
	}
	return qsruntime.ExecutionResult{
		RowSet: rowSet,
		Count:  uint64(rowSet.CandidateCount()),
	}, nil
}

func (s *runtimeFixtureStore) applyMemberships(ctx context.Context, memberships []qsbridge.MembershipEdge, candidates runtimeFixtureCandidateRows) (runtimeFixtureCandidateRows, error) {
	for _, membership := range memberships {
		var err error
		candidates, err = s.applyMembership(ctx, membership, candidates)
		if err != nil {
			return runtimeFixtureCandidateRows{}, err
		}
	}
	return candidates, nil
}

func (s *runtimeFixtureStore) applyMembership(ctx context.Context, membership qsbridge.MembershipEdge, candidates runtimeFixtureCandidateRows) (runtimeFixtureCandidateRows, error) {
	rightRows, ok := s.tableRows(membership.Right.Table.Table)
	if !ok {
		return runtimeFixtureCandidateRows{}, fmt.Errorf("runtime fixture unsupported membership table: %s", membership.Right.Table.Table)
	}
	if len(membership.Predicates) > 0 {
		filtered, err := runtimeFixtureApplyResidualPredicates(membership.Predicates, runtimeFixtureCandidateRows{
			Set: qsbridge.QuantaCandidateSet{
				Index:   membership.Right.Table.Table,
				Rownums: runtimeFixtureSequentialRownums(len(rightRows)),
			},
			Rows: rightRows,
		})
		if err != nil {
			return runtimeFixtureCandidateRows{}, err
		}
		rightRows = filtered.Rows
	}
	rightValues := make([]qsbridge.ResultCell, 0, len(rightRows))
	for _, row := range rightRows {
		cell, ok := runtimeFixtureFieldCell(row, membership.Right)
		if ok {
			rightValues = append(rightValues, cell)
		}
	}
	rows := make([]runtimeFixtureRow, 0, len(candidates.Rows))
	rownums := make([]qsbridge.QuantaRownum, 0, len(candidates.Set.Rownums))
	for rowIndex, row := range candidates.Rows {
		if err := ctx.Err(); err != nil {
			return runtimeFixtureCandidateRows{}, err
		}
		leftCell, leftOK := runtimeFixtureFieldCell(row, membership.Left)
		matched := false
		if leftOK {
			for _, rightCell := range rightValues {
				if runtimeFixtureCellEqual(leftCell, rightCell) {
					matched = true
					break
				}
			}
		}
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			rows = append(rows, row)
			rownums = append(rownums, candidates.Set.Rownums[rowIndex])
		}
	}
	candidates.Rows = rows
	candidates.Set.Rownums = rownums
	return candidates, nil
}

func (s *runtimeFixtureStore) selectCandidates(ctx context.Context, index string, request qsruntime.ExecutionRequest) (runtimeFixtureCandidateRows, error) {
	if len(request.Joins) > 0 {
		candidates, err := s.joinCandidates(ctx, index, request)
		if err != nil {
			return runtimeFixtureCandidateRows{}, err
		}
		return runtimeFixtureApplyQueryFilters(ctx, index, request.Query, candidates)
	}
	rows, ok := s.tableRows(index)
	if !ok {
		return runtimeFixtureCandidateRows{}, fmt.Errorf("runtime fixture unsupported table: %s", index)
	}
	candidates := runtimeFixtureCandidateRows{
		Set: qsbridge.QuantaCandidateSet{
			Index:   index,
			Rownums: runtimeFixtureSequentialRownums(len(rows)),
		},
		Rows: rows,
	}
	return runtimeFixtureApplyQueryFilters(ctx, index, request.Query, candidates)
}

func (s *runtimeFixtureStore) joinCandidates(ctx context.Context, index string, request qsruntime.ExecutionRequest) (runtimeFixtureCandidateRows, error) {
	if len(request.Joins) == 0 {
		return runtimeFixtureCandidateRows{}, fmt.Errorf("runtime fixture join request has no join edges")
	}
	if len(request.Sources) == 0 {
		return runtimeFixtureCandidateRows{}, fmt.Errorf("runtime fixture join request has no sources")
	}
	current, ok := s.tableRows(request.Sources[0].Table)
	if !ok {
		return runtimeFixtureCandidateRows{}, fmt.Errorf("runtime fixture unsupported join table: %s", request.Sources[0].Table)
	}
	current = runtimeFixtureQualifiedRows(request.Sources[0], current)
	for _, join := range request.Joins {
		if join.Kind != qsbridge.JoinKindInner && join.Kind != qsbridge.JoinKindLeftOuter {
			return runtimeFixtureCandidateRows{}, fmt.Errorf("runtime fixture supports only inner and left outer joins")
		}
		rows, err := s.applyJoinEdge(ctx, current, join)
		if err != nil {
			return runtimeFixtureCandidateRows{}, err
		}
		current = rows
	}
	return runtimeFixtureCandidateRows{
		Set: qsbridge.QuantaCandidateSet{
			Index:   index,
			Rownums: runtimeFixtureSequentialRownums(len(current)),
		},
		Rows: current,
	}, nil
}

func (s *runtimeFixtureStore) applyJoinEdge(ctx context.Context, current []runtimeFixtureRow, join qsbridge.JoinEdge) ([]runtimeFixtureRow, error) {
	newSide := join.Right.Table
	if len(current) > 0 {
		leftPresent := runtimeFixtureRowHasTable(current[0], join.Left.Table)
		rightPresent := runtimeFixtureRowHasTable(current[0], join.Right.Table)
		switch {
		case leftPresent && !rightPresent:
			newSide = join.Right.Table
		case rightPresent && !leftPresent:
			newSide = join.Left.Table
		case leftPresent && rightPresent:
			return runtimeFixtureFilterJoinedRows(ctx, current, join)
		case !leftPresent && !rightPresent:
			return nil, fmt.Errorf("runtime fixture join edge does not connect to accumulated row set: %s.%s = %s.%s",
				join.Left.Table.RefName(), join.Left.Name, join.Right.Table.RefName(), join.Right.Name)
		}
	}
	newRows, ok := s.tableRows(newSide.Table)
	if !ok {
		return nil, fmt.Errorf("runtime fixture unsupported join table: %s", newSide.Table)
	}
	rows := make([]runtimeFixtureRow, 0)
	for _, currentRow := range current {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		matchedCurrent := false
		for _, newRow := range newRows {
			joined := make(runtimeFixtureRow, len(currentRow)+len(newRow))
			for name, cell := range currentRow {
				joined[name] = cell
			}
			runtimeFixtureCopyQualifiedRow(joined, newSide, newRow)
			leftCell, leftOK := runtimeFixtureQualifiedFieldCell(joined, join.Left)
			rightCell, rightOK := runtimeFixtureQualifiedFieldCell(joined, join.Right)
			if !leftOK || !rightOK || !runtimeFixtureCellEqual(leftCell, rightCell) {
				continue
			}
			matches, err := runtimeFixtureJoinPredicatesMatch(join.On, joined)
			if err != nil {
				return nil, err
			}
			if matches {
				matchedCurrent = true
				rows = append(rows, joined)
			}
		}
		if !matchedCurrent && join.Kind == qsbridge.JoinKindLeftOuter {
			rows = append(rows, currentRow)
		}
	}
	return rows, nil
}

func runtimeFixtureQualifiedRows(source qsbridge.TableInstance, rows []runtimeFixtureRow) []runtimeFixtureRow {
	qualified := make([]runtimeFixtureRow, 0, len(rows))
	for _, row := range rows {
		qualifiedRow := make(runtimeFixtureRow, len(row)*3)
		runtimeFixtureCopyQualifiedRow(qualifiedRow, source, row)
		qualified = append(qualified, qualifiedRow)
	}
	return qualified
}

func runtimeFixtureRowHasTable(row runtimeFixtureRow, table qsbridge.TableInstance) bool {
	for name := range row {
		if strings.HasPrefix(name, table.Table+".") {
			return true
		}
		if ref := table.RefName(); ref != "" && strings.HasPrefix(name, ref+".") {
			return true
		}
	}
	return false
}

func runtimeFixtureFilterJoinedRows(ctx context.Context, current []runtimeFixtureRow, join qsbridge.JoinEdge) ([]runtimeFixtureRow, error) {
	rows := make([]runtimeFixtureRow, 0, len(current))
	for _, row := range current {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		leftCell, leftOK := runtimeFixtureQualifiedFieldCell(row, join.Left)
		rightCell, rightOK := runtimeFixtureQualifiedFieldCell(row, join.Right)
		if !leftOK || !rightOK || !runtimeFixtureCellEqual(leftCell, rightCell) {
			continue
		}
		matches, err := runtimeFixtureJoinPredicatesMatch(join.On, row)
		if err != nil {
			return nil, err
		}
		if matches {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func runtimeFixtureJoinPredicatesMatch(predicates []qsbridge.Predicate, row runtimeFixtureRow) (bool, error) {
	for _, predicate := range predicates {
		matches, err := runtimeFixtureEvalRowPredicate(predicate, row)
		if err != nil {
			return false, err
		}
		if !matches {
			return false, nil
		}
	}
	return true, nil
}

func runtimeFixtureCopyQualifiedRow(target runtimeFixtureRow, source qsbridge.TableInstance, row runtimeFixtureRow) {
	for name, cell := range row {
		if _, exists := target[name]; !exists {
			target[name] = cell
		}
		if source.Table != "" {
			target[source.Table+"."+name] = cell
		}
		if ref := source.RefName(); ref != "" {
			target[ref+"."+name] = cell
		}
	}
}

func runtimeFixtureSelectCandidates(index string, fragments []qsbridge.QuantaQueryFragment, rows []runtimeFixtureRow) (runtimeFixtureCandidateRows, error) {
	return runtimeFixtureFilterCandidateRows(index, fragments, runtimeFixtureCandidateRows{
		Set: qsbridge.QuantaCandidateSet{
			Index:   index,
			Rownums: runtimeFixtureSequentialRownums(len(rows)),
		},
		Rows: rows,
	})
}

func runtimeFixtureSequentialRownums(count int) []qsbridge.QuantaRownum {
	rownums := make([]qsbridge.QuantaRownum, count)
	for index := range rownums {
		rownums[index] = qsbridge.QuantaRownum(index + 1)
	}
	return rownums
}

func runtimeFixtureFilterCandidates(index string, fragments []qsbridge.QuantaQueryFragment, rows []runtimeFixtureRow) (runtimeFixtureCandidateRows, error) {
	return runtimeFixtureSelectCandidates(index, fragments, rows)
}

func runtimeFixtureApplyQueryFilters(ctx context.Context, index string, query qsbridge.QuantaIntermediateQuery, candidates runtimeFixtureCandidateRows) (runtimeFixtureCandidateRows, error) {
	var err error
	if !query.Filter.Empty() {
		candidates, err = runtimeFixtureApplyFilterExpression(ctx, index, query.Filter, candidates)
		if err != nil {
			return runtimeFixtureCandidateRows{}, err
		}
	}
	if len(query.Fragments) > 0 {
		candidates, err = runtimeFixtureFilterCandidateRows(index, query.Fragments, candidates)
		if err != nil {
			return runtimeFixtureCandidateRows{}, err
		}
	}
	return candidates, nil
}

func runtimeFixtureApplyFilterExpression(ctx context.Context, index string, filter qsbridge.QuantaFilterExpression, candidates runtimeFixtureCandidateRows) (runtimeFixtureCandidateRows, error) {
	evaluator := qsruntime.QuantaFilterTreeEvaluator{
		Leaves: runtimeFixtureFilterLeafEvaluator{
			index:      index,
			candidates: candidates,
		},
	}
	set, diagnostics, err := evaluator.EvaluateFilter(ctx, filter)
	if err != nil {
		return runtimeFixtureCandidateRows{}, err
	}
	if diagnostics.BlocksNative() {
		return runtimeFixtureCandidateRows{}, runtimeFixtureDiagnosticsError(diagnostics)
	}
	return runtimeFixtureCandidateRowsForSet(candidates, set), nil
}

type runtimeFixtureFilterLeafEvaluator struct {
	index      string
	candidates runtimeFixtureCandidateRows
}

func (e runtimeFixtureFilterLeafEvaluator) EvaluateFilterLeaf(ctx context.Context, fragment qsbridge.QuantaQueryFragment) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	rownums := make([]qsbridge.QuantaRownum, 0, len(e.candidates.Set.Rownums))
	for rowIndex, row := range e.candidates.Rows {
		if err := ctx.Err(); err != nil {
			return qsbridge.QuantaCandidateSet{}, nil, err
		}
		matched, err := runtimeFixtureMatchFragment(fragment, row)
		if err != nil {
			return qsbridge.QuantaCandidateSet{}, nil, err
		}
		if matched {
			rownums = append(rownums, e.candidates.Set.Rownums[rowIndex])
		}
	}
	return qsbridge.QuantaCandidateSet{
		Index:   e.index,
		Rownums: rownums,
	}, nil, nil
}

func runtimeFixtureCandidateRowsForSet(candidates runtimeFixtureCandidateRows, set qsbridge.QuantaCandidateSet) runtimeFixtureCandidateRows {
	rowByRownum := make(map[qsbridge.QuantaRownum]runtimeFixtureRow, len(candidates.Set.Rownums))
	for rowIndex, rownum := range candidates.Set.Rownums {
		rowByRownum[rownum] = candidates.Rows[rowIndex]
	}
	rows := make([]runtimeFixtureRow, 0, len(set.Rownums))
	rownums := make([]qsbridge.QuantaRownum, 0, len(set.Rownums))
	for _, rownum := range set.Rownums {
		row, ok := rowByRownum[rownum]
		if !ok {
			continue
		}
		rows = append(rows, row)
		rownums = append(rownums, rownum)
	}
	candidates.Set = set
	candidates.Set.Rownums = rownums
	candidates.Rows = rows
	return candidates
}

func runtimeFixtureFilterCandidateRows(index string, fragments []qsbridge.QuantaQueryFragment, candidates runtimeFixtureCandidateRows) (runtimeFixtureCandidateRows, error) {
	filteredRows := make([]runtimeFixtureRow, 0, len(candidates.Rows))
	rownums := make([]qsbridge.QuantaRownum, 0, len(candidates.Set.Rownums))
	for rowIndex, row := range candidates.Rows {
		matches := true
		hasUnion := false
		unionMatches := false
		for _, fragment := range fragments {
			matched, err := runtimeFixtureMatchFragment(fragment, row)
			if err != nil {
				return runtimeFixtureCandidateRows{}, err
			}
			if fragment.Operation == qsbridge.QuantaOperationUnion {
				hasUnion = true
				if matched {
					unionMatches = true
				}
				continue
			}
			if !matched {
				matches = false
				break
			}
		}
		if hasUnion && !unionMatches {
			matches = false
		}
		if matches {
			filteredRows = append(filteredRows, row)
			rownums = append(rownums, candidates.Set.Rownums[rowIndex])
		}
	}
	return runtimeFixtureCandidateRows{
		Set: qsbridge.QuantaCandidateSet{
			Index:   index,
			Rownums: rownums,
		},
		Rows: filteredRows,
	}, nil
}

func runtimeFixtureApplyResidualPredicates(predicates []qsbridge.Predicate, candidates runtimeFixtureCandidateRows) (runtimeFixtureCandidateRows, error) {
	if len(predicates) == 0 || len(candidates.Rows) == 0 {
		return candidates, nil
	}
	rows := make([]runtimeFixtureRow, 0, len(candidates.Rows))
	rownums := make([]qsbridge.QuantaRownum, 0, len(candidates.Set.Rownums))
	for rowIndex, row := range candidates.Rows {
		matches := true
		hasOr := false
		orMatches := false
		for _, predicate := range predicates {
			matched, err := runtimeFixtureEvalRowPredicate(predicate, row)
			if err != nil {
				return runtimeFixtureCandidateRows{}, err
			}
			if predicate.Combinator == qsbridge.PredicateCombinatorOr {
				hasOr = true
				if matched {
					orMatches = true
				}
				continue
			}
			if !matched {
				matches = false
				break
			}
		}
		if hasOr && !orMatches {
			matches = false
		}
		if matches {
			rows = append(rows, row)
			rownums = append(rownums, candidates.Set.Rownums[rowIndex])
		}
	}
	candidates.Rows = rows
	candidates.Set.Rownums = rownums
	return candidates, nil
}

type runtimeFixtureGroup struct {
	key   string
	cells []qsbridge.ResultCell
	rows  []runtimeFixtureRow
}

func runtimeFixtureGroupedAggregateRows(request qsruntime.ExecutionRequest, rows []runtimeFixtureRow) (qsbridge.QuantaProjectedRowSet, error) {
	groups := make([]runtimeFixtureGroup, 0)
	groupIndex := make(map[string]int)
	for _, row := range rows {
		cells := make([]qsbridge.ResultCell, 0, len(request.GroupBy))
		keyParts := make([]string, 0, len(request.GroupBy))
		for _, expr := range request.GroupBy {
			cell, err := runtimeFixtureEvalRowExpr(expr, row)
			if err != nil {
				return qsbridge.QuantaProjectedRowSet{}, err
			}
			cells = append(cells, cell)
			keyParts = append(keyParts, runtimeFixtureDistinctCellKey(cell))
		}
		key := strings.Join(keyParts, "\x00")
		index, ok := groupIndex[key]
		if !ok {
			index = len(groups)
			groupIndex[key] = index
			groups = append(groups, runtimeFixtureGroup{key: key, cells: cells})
		}
		groups[index].rows = append(groups[index].rows, row)
	}

	groupRows := make([]runtimeFixtureRow, 0, len(groups))
	for _, group := range groups {
		row, err := runtimeFixtureGroupedRow(request, group)
		if err != nil {
			return qsbridge.QuantaProjectedRowSet{}, err
		}
		matches, err := runtimeFixtureGroupedRowMatchesHaving(request.Having, row)
		if err != nil {
			return qsbridge.QuantaProjectedRowSet{}, err
		}
		if matches {
			groupRows = append(groupRows, row)
		}
	}
	var err error
	groupRows, err = runtimeFixtureSortRows(request.OrderBy, groupRows)
	if err != nil {
		return qsbridge.QuantaProjectedRowSet{}, err
	}
	groupRows = runtimeFixtureLimitRows(groupRows, request.Result.Offset, request.Result.Limit)
	groupRows = runtimeFixtureLimitRows(groupRows, 0, request.Options.MaxRows)

	rowSet := qsbridge.QuantaProjectedRowSet{
		Rownums: make([]qsbridge.QuantaRownum, len(groupRows)),
	}
	for rowIndex := range groupRows {
		rowSet.Rownums[rowIndex] = qsbridge.QuantaRownum(rowIndex + 1)
	}
	for _, projection := range request.Projection {
		vector := qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Field:   runtimeFixtureProjectionName(projection),
				Type:    runtimeFixtureProjectionType(projection),
				Visible: true,
			},
			Values: make([]qsbridge.ResultCell, len(groupRows)),
		}
		for rowIndex, row := range groupRows {
			cell, err := runtimeFixtureEvalRowExpr(projection.Expr, row)
			if err != nil {
				return qsbridge.QuantaProjectedRowSet{}, err
			}
			vector.Values[rowIndex] = cell
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
	}
	return rowSet, nil
}

func runtimeFixtureGroupedRow(request qsruntime.ExecutionRequest, group runtimeFixtureGroup) (runtimeFixtureRow, error) {
	row := make(runtimeFixtureRow)
	for index, expr := range request.GroupBy {
		cell := group.cells[index]
		row[runtimeFixtureGroupExprKey(expr)] = cell
		if field, ok := runtimeFixtureExprField(expr); ok {
			row[field.Name] = cell
			row[field.QualifiedName()] = cell
		}
	}
	for index, aggregate := range request.SQLAggregates {
		cell, err := runtimeFixtureAggregateCell(aggregate, group.rows)
		if err != nil {
			return nil, err
		}
		alias := aggregate.Alias
		if alias == "" {
			alias = aggregate.Function
		}
		row[alias] = cell
		row[runtimeFixtureAggregateRefKey(index, alias)] = cell
	}
	return row, nil
}

func runtimeFixtureGroupedRowMatchesHaving(predicates []qsbridge.Predicate, row runtimeFixtureRow) (bool, error) {
	for _, predicate := range predicates {
		matched, err := runtimeFixtureEvalRowPredicate(predicate, row)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func runtimeFixtureGroupExprKey(expr qsbridge.Expr) string {
	return "group:" + runtimeFixtureExprKey(expr)
}

func runtimeFixtureAggregateRefKey(index int, alias string) string {
	return fmt.Sprintf("aggregate:%d:%s", index, alias)
}

func runtimeFixtureExprKey(expr qsbridge.Expr) string {
	switch value := expr.(type) {
	case qsbridge.FieldExpr:
		return "field:" + value.Ref.QualifiedName()
	case *qsbridge.FieldExpr:
		if value != nil {
			return "field:" + value.Ref.QualifiedName()
		}
	case qsbridge.CallExpr:
		parts := make([]string, 0, len(value.Args)+1)
		parts = append(parts, "call:"+strings.ToLower(value.Name))
		for _, arg := range value.Args {
			parts = append(parts, runtimeFixtureExprKey(arg))
		}
		return strings.Join(parts, "|")
	case *qsbridge.CallExpr:
		if value != nil {
			return runtimeFixtureExprKey(*value)
		}
	case qsbridge.AggregateRefExpr:
		return runtimeFixtureAggregateRefKey(value.Index, value.Alias)
	case *qsbridge.AggregateRefExpr:
		if value != nil {
			return runtimeFixtureAggregateRefKey(value.Index, value.Alias)
		}
	case qsbridge.SearchedCaseExpr:
		parts := make([]string, 0, len(value.Whens)*2+2)
		parts = append(parts, "case")
		for _, when := range value.Whens {
			parts = append(parts, "when:"+runtimeFixtureExprKey(when.Condition), "then:"+runtimeFixtureExprKey(when.Result))
		}
		if value.Else != nil {
			parts = append(parts, "else:"+runtimeFixtureExprKey(value.Else))
		}
		return strings.Join(parts, "|")
	case *qsbridge.SearchedCaseExpr:
		if value != nil {
			return runtimeFixtureExprKey(*value)
		}
	case qsbridge.LiteralExpr:
		return fmt.Sprintf("literal:%s:%v", value.Kind, value.Value)
	case *qsbridge.LiteralExpr:
		if value != nil {
			return fmt.Sprintf("literal:%s:%v", value.Kind, value.Value)
		}
	}
	return fmt.Sprintf("%T", expr)
}

func runtimeFixtureExprField(expr qsbridge.Expr) (qsbridge.FieldRef, bool) {
	switch value := expr.(type) {
	case qsbridge.FieldExpr:
		return value.Ref, true
	case *qsbridge.FieldExpr:
		if value != nil {
			return value.Ref, true
		}
	}
	return qsbridge.FieldRef{}, false
}

func runtimeFixtureDistinctCellKey(cell qsbridge.ResultCell) string {
	return string(cell.Kind) + ":" + fmt.Sprint(cell.Value)
}

func runtimeFixtureEvalRowPredicate(predicate qsbridge.Predicate, row runtimeFixtureRow) (bool, error) {
	binary, ok := predicate.Expr.(qsbridge.BinaryExpr)
	if !ok {
		if pointer, pointerOK := predicate.Expr.(*qsbridge.BinaryExpr); pointerOK && pointer != nil {
			binary = *pointer
			ok = true
		}
	}
	if !ok {
		return false, fmt.Errorf("runtime fixture predicate requires a binary expression")
	}
	left, err := runtimeFixtureEvalRowExpr(binary.Left, row)
	if err != nil {
		return false, err
	}
	switch binary.Op {
	case qsbridge.BinaryOpIn, qsbridge.BinaryOpNotIn:
		matched, err := runtimeFixtureCellInExprList(left, binary.Right, row)
		if err != nil {
			return false, err
		}
		if binary.Op == qsbridge.BinaryOpNotIn {
			return !matched, nil
		}
		return matched, nil
	case qsbridge.BinaryOpBetween, qsbridge.BinaryOpNotBetween:
		matched, err := runtimeFixtureCellBetweenExprList(left, binary.Right, row)
		if err != nil {
			return false, err
		}
		if binary.Op == qsbridge.BinaryOpNotBetween {
			return !matched, nil
		}
		return matched, nil
	}
	right, err := runtimeFixtureEvalRowExpr(binary.Right, row)
	if err != nil {
		return false, err
	}
	switch binary.Op {
	case qsbridge.BinaryOpEqual:
		return runtimeFixtureCellEqual(left, right), nil
	case qsbridge.BinaryOpNotEqual:
		return !runtimeFixtureCellEqual(left, right), nil
	case qsbridge.BinaryOpLess:
		return runtimeFixtureCellCompare(left, right) < 0, nil
	case qsbridge.BinaryOpLessEqual:
		return runtimeFixtureCellCompare(left, right) <= 0, nil
	case qsbridge.BinaryOpGreater:
		return runtimeFixtureCellCompare(left, right) > 0, nil
	case qsbridge.BinaryOpGreaterEqual:
		return runtimeFixtureCellCompare(left, right) >= 0, nil
	case qsbridge.BinaryOpLike:
		return runtimeFixtureLikeMatch(fmt.Sprint(left.Value), fmt.Sprint(right.Value)), nil
	case qsbridge.BinaryOpNotLike:
		return !runtimeFixtureLikeMatch(fmt.Sprint(left.Value), fmt.Sprint(right.Value)), nil
	case qsbridge.BinaryOpRegexp:
		return runtimeFixtureRegexpMatch(fmt.Sprint(left.Value), fmt.Sprint(right.Value)), nil
	case qsbridge.BinaryOpNotRegexp:
		return !runtimeFixtureRegexpMatch(fmt.Sprint(left.Value), fmt.Sprint(right.Value)), nil
	default:
		return false, fmt.Errorf("runtime fixture unsupported residual predicate op %s", binary.Op)
	}
}

func runtimeFixtureRegexpMatch(value string, pattern string) bool {
	matched, err := regexp.MatchString(pattern, value)
	return err == nil && matched
}

func runtimeFixtureCellInExprList(cell qsbridge.ResultCell, expr qsbridge.Expr, row runtimeFixtureRow) (bool, error) {
	items, err := runtimeFixtureEvalRowExprList(expr, row)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if runtimeFixtureCellEqual(cell, item) {
			return true, nil
		}
	}
	return false, nil
}

func runtimeFixtureCellBetweenExprList(cell qsbridge.ResultCell, expr qsbridge.Expr, row runtimeFixtureRow) (bool, error) {
	items, err := runtimeFixtureEvalRowExprList(expr, row)
	if err != nil {
		return false, err
	}
	if len(items) != 2 {
		return false, fmt.Errorf("runtime fixture BETWEEN requires exactly two bounds")
	}
	return runtimeFixtureCellCompare(cell, items[0]) >= 0 && runtimeFixtureCellCompare(cell, items[1]) <= 0, nil
}

func runtimeFixtureEvalRowExprList(expr qsbridge.Expr, row runtimeFixtureRow) ([]qsbridge.ResultCell, error) {
	switch value := expr.(type) {
	case qsbridge.ListExpr:
		return runtimeFixtureEvalRowListItems(value.Items, row)
	case *qsbridge.ListExpr:
		if value == nil {
			return nil, nil
		}
		return runtimeFixtureEvalRowListItems(value.Items, row)
	default:
		cell, err := runtimeFixtureEvalRowExpr(expr, row)
		if err != nil {
			return nil, err
		}
		return []qsbridge.ResultCell{cell}, nil
	}
}

func runtimeFixtureEvalRowListItems(items []qsbridge.Expr, row runtimeFixtureRow) ([]qsbridge.ResultCell, error) {
	cells := make([]qsbridge.ResultCell, 0, len(items))
	for _, item := range items {
		cell, err := runtimeFixtureEvalRowExpr(item, row)
		if err != nil {
			return nil, err
		}
		cells = append(cells, cell)
	}
	return cells, nil
}

func runtimeFixtureLikeMatch(text string, pattern string) bool {
	return runtimeFixtureLikeMatchAt(text, pattern, 0, 0)
}

func runtimeFixtureLikeMatchAt(text string, pattern string, textIndex int, patternIndex int) bool {
	for patternIndex < len(pattern) {
		switch pattern[patternIndex] {
		case '%':
			for patternIndex < len(pattern) && pattern[patternIndex] == '%' {
				patternIndex++
			}
			if patternIndex == len(pattern) {
				return true
			}
			for candidate := textIndex; candidate <= len(text); candidate++ {
				if runtimeFixtureLikeMatchAt(text, pattern, candidate, patternIndex) {
					return true
				}
			}
			return false
		case '_':
			if textIndex >= len(text) {
				return false
			}
			textIndex++
			patternIndex++
		default:
			if textIndex >= len(text) || text[textIndex] != pattern[patternIndex] {
				return false
			}
			textIndex++
			patternIndex++
		}
	}
	return textIndex == len(text)
}

func runtimeFixtureEvalRowExpr(expr qsbridge.Expr, row runtimeFixtureRow) (qsbridge.ResultCell, error) {
	if cell, ok := row[runtimeFixtureGroupExprKey(expr)]; ok {
		return cell, nil
	}
	switch value := expr.(type) {
	case qsbridge.LiteralExpr:
		return qsbridge.ResultCell{Kind: value.Kind, Value: value.Value}, nil
	case *qsbridge.LiteralExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{Kind: value.Kind, Value: value.Value}, nil
	case qsbridge.FieldExpr:
		cell, ok := runtimeFixtureFieldCell(row, value.Ref)
		if !ok {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return cell, nil
	case *qsbridge.FieldExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		cell, ok := runtimeFixtureFieldCell(row, value.Ref)
		if !ok {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return cell, nil
	case qsbridge.CallExpr:
		return runtimeFixtureEvalRowCall(value, row)
	case *qsbridge.CallExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return runtimeFixtureEvalRowCall(*value, row)
	case qsbridge.AggregateRefExpr:
		return runtimeFixtureAggregateRefCell(value, row), nil
	case *qsbridge.AggregateRefExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return runtimeFixtureAggregateRefCell(*value, row), nil
	case qsbridge.BinaryExpr:
		return runtimeFixtureEvalRowBinary(value, row)
	case *qsbridge.BinaryExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return runtimeFixtureEvalRowBinary(*value, row)
	case qsbridge.SearchedCaseExpr:
		return runtimeFixtureEvalRowSearchedCase(value, row)
	case *qsbridge.SearchedCaseExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return runtimeFixtureEvalRowSearchedCase(*value, row)
	default:
		return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture unsupported residual expression %T", expr)
	}
}

func runtimeFixtureEvalRowSearchedCase(expr qsbridge.SearchedCaseExpr, row runtimeFixtureRow) (qsbridge.ResultCell, error) {
	for _, when := range expr.Whens {
		matched, err := runtimeFixtureEvalCaseCondition(when.Condition, row)
		if err != nil {
			return qsbridge.ResultCell{}, err
		}
		if matched {
			return runtimeFixtureEvalRowExpr(when.Result, row)
		}
	}
	if expr.Else != nil {
		return runtimeFixtureEvalRowExpr(expr.Else, row)
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
}

func runtimeFixtureEvalCaseCondition(expr qsbridge.Expr, row runtimeFixtureRow) (bool, error) {
	switch value := expr.(type) {
	case qsbridge.BinaryExpr:
		return runtimeFixtureEvalRowPredicate(qsbridge.Predicate{Expr: value}, row)
	case *qsbridge.BinaryExpr:
		if value == nil {
			return false, nil
		}
		return runtimeFixtureEvalRowPredicate(qsbridge.Predicate{Expr: *value}, row)
	default:
		cell, err := runtimeFixtureEvalRowExpr(expr, row)
		if err != nil {
			return false, err
		}
		return runtimeFixtureCellTruthy(cell), nil
	}
}

func runtimeFixtureAggregateRefCell(ref qsbridge.AggregateRefExpr, row runtimeFixtureRow) qsbridge.ResultCell {
	if cell, ok := row[runtimeFixtureAggregateRefKey(ref.Index, ref.Alias)]; ok {
		return cell
	}
	if cell, ok := row[ref.Alias]; ok {
		return cell
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
}

func runtimeFixtureEvalRowCall(call qsbridge.CallExpr, row runtimeFixtureRow) (qsbridge.ResultCell, error) {
	args := make([]qsbridge.ResultCell, 0, len(call.Args))
	for _, arg := range call.Args {
		cell, err := runtimeFixtureEvalRowExpr(arg, row)
		if err != nil {
			return qsbridge.ResultCell{}, err
		}
		args = append(args, cell)
	}
	return runtimeFixtureDispatchSQLCall(call.Name, args)
}

func runtimeFixtureEvalRowBinary(expr qsbridge.BinaryExpr, row runtimeFixtureRow) (qsbridge.ResultCell, error) {
	left, err := runtimeFixtureEvalRowExpr(expr.Left, row)
	if err != nil {
		return qsbridge.ResultCell{}, err
	}
	right, err := runtimeFixtureEvalRowExpr(expr.Right, row)
	if err != nil {
		return qsbridge.ResultCell{}, err
	}
	return runtimeFixtureEvalNumericBinary(expr.Op, left, right)
}

func runtimeFixtureAggregateRows(aggregates []qsbridge.Aggregate, rows []runtimeFixtureRow) (qsbridge.QuantaProjectedRowSet, error) {
	if len(aggregates) == 1 && strings.EqualFold(aggregates[0].Function, "topn") {
		return runtimeFixtureTopNRows(aggregates[0], rows)
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []qsbridge.QuantaRownum{1},
	}
	for _, aggregate := range aggregates {
		if aggregate.Mode == qsbridge.AggregateDistinct {
			return qsbridge.QuantaProjectedRowSet{}, fmt.Errorf("runtime fixture distinct aggregates are not supported")
		}
		if aggregate.Filter != nil {
			return qsbridge.QuantaProjectedRowSet{}, fmt.Errorf("runtime fixture aggregate filters are not supported")
		}
		value, err := runtimeFixtureAggregateCell(aggregate, rows)
		if err != nil {
			return qsbridge.QuantaProjectedRowSet{}, err
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Index:   "orders",
				Field:   aggregate.Alias,
				Type:    aggregate.Type,
				Visible: true,
			},
			Values: []qsbridge.ResultCell{value},
		})
	}
	return rowSet, nil
}

func runtimeFixtureTopNRows(aggregate qsbridge.Aggregate, rows []runtimeFixtureRow) (qsbridge.QuantaProjectedRowSet, error) {
	field, ok := aggregate.Input.(qsbridge.FieldExpr)
	if !ok {
		if pointer, pointerOK := aggregate.Input.(*qsbridge.FieldExpr); pointerOK && pointer != nil {
			field = *pointer
			ok = true
		}
	}
	if !ok {
		return qsbridge.QuantaProjectedRowSet{}, fmt.Errorf("runtime fixture topn requires a direct field input")
	}
	counts := make(map[string]int)
	for _, row := range rows {
		cell, ok := runtimeFixtureFieldCell(row, field.Ref)
		if !ok || cell.Kind == qsbridge.ValueNull {
			continue
		}
		counts[fmt.Sprint(cell.Value)]++
	}
	type topNEntry struct {
		Label string
		Count int
	}
	entries := make([]topNEntry, 0, len(counts))
	total := 0
	for label, count := range counts {
		entries = append(entries, topNEntry{Label: label, Count: count})
		total += count
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Count == entries[j].Count {
			return entries[i].Label < entries[j].Label
		}
		return entries[i].Count > entries[j].Count
	})
	rowCount := len(entries) + 1
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   field.Ref.Table.Table,
		Rownums: make([]qsbridge.QuantaRownum, rowCount),
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field:  qsbridge.QuantaProjectionField{Index: field.Ref.Table.Table, Field: "topn_" + field.Ref.Name, Type: qsbridge.DataTypeString, Visible: true},
				Values: make([]qsbridge.ResultCell, rowCount),
			},
			{
				Field:  qsbridge.QuantaProjectionField{Index: field.Ref.Table.Table, Field: "topn_count", Type: qsbridge.DataTypeInt, Visible: true},
				Values: make([]qsbridge.ResultCell, rowCount),
			},
			{
				Field:  qsbridge.QuantaProjectionField{Index: field.Ref.Table.Table, Field: "topn_percent", Type: qsbridge.DataTypeFloat, Visible: true},
				Values: make([]qsbridge.ResultCell, rowCount),
			},
		},
	}
	for index, entry := range entries {
		rowSet.Rownums[index] = qsbridge.QuantaRownum(index + 1)
		rowSet.ProjectionVectors[0].Values[index] = qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: entry.Label}
		rowSet.ProjectionVectors[1].Values[index] = qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(entry.Count)}
		rowSet.ProjectionVectors[2].Values[index] = qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: runtimeFixtureTopNPercent(entry.Count, total)}
	}
	totalIndex := rowCount - 1
	rowSet.Rownums[totalIndex] = qsbridge.QuantaRownum(rowCount)
	rowSet.ProjectionVectors[0].Values[totalIndex] = qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "TOTAL:"}
	rowSet.ProjectionVectors[1].Values[totalIndex] = qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(total)}
	rowSet.ProjectionVectors[2].Values[totalIndex] = qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: 100.0}
	return rowSet, nil
}

func runtimeFixtureTopNPercent(count int, total int) float64 {
	if total == 0 {
		return 0
	}
	return math.Round((float64(count)/float64(total))*10000) / 100
}

func runtimeFixtureAggregateCell(aggregate qsbridge.Aggregate, rows []runtimeFixtureRow) (qsbridge.ResultCell, error) {
	switch {
	case strings.EqualFold(aggregate.Function, "count"):
		if aggregate.Mode == qsbridge.AggregateDistinct {
			seen := make(map[string]struct{})
			for _, row := range rows {
				cell, err := runtimeFixtureEvalRowExpr(aggregate.Input, row)
				if err != nil {
					return qsbridge.ResultCell{}, err
				}
				if cell.Kind == qsbridge.ValueNull {
					continue
				}
				seen[runtimeFixtureDistinctCellKey(cell)] = struct{}{}
			}
			return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(len(seen))}, nil
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(len(rows))}, nil
	case strings.EqualFold(aggregate.Function, "sum"):
		var sum float64
		seen := 0
		for _, row := range rows {
			cell, err := runtimeFixtureEvalRowExpr(aggregate.Input, row)
			if err != nil {
				return qsbridge.ResultCell{}, err
			}
			value, ok := runtimeFixtureFloat64(cell.Value)
			if !ok {
				return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture sum requires numeric input")
			}
			sum += value
			seen++
		}
		if seen == 0 {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: sum}, nil
	case strings.EqualFold(aggregate.Function, "avg"):
		var sum float64
		var intSum int64
		seen := 0
		allInt := true
		for _, row := range rows {
			cell, err := runtimeFixtureEvalRowExpr(aggregate.Input, row)
			if err != nil {
				return qsbridge.ResultCell{}, err
			}
			intValue, intOK := runtimeFixtureInt64(cell)
			if intOK {
				intSum += intValue
			} else {
				allInt = false
			}
			value, ok := runtimeFixtureFloat64(cell.Value)
			if !ok {
				return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture avg requires numeric input")
			}
			sum += value
			seen++
		}
		if seen == 0 {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		if allInt {
			return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: intSum / int64(seen)}, nil
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: sum / float64(seen)}, nil
	case strings.EqualFold(aggregate.Function, "min"):
		cell, ok, err := runtimeFixtureExtremumAggregateCell(aggregate, rows, true)
		if err != nil || ok {
			return cell, err
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	case strings.EqualFold(aggregate.Function, "max"):
		cell, ok, err := runtimeFixtureExtremumAggregateCell(aggregate, rows, false)
		if err != nil || ok {
			return cell, err
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	default:
		return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture aggregate %q is not supported", aggregate.Function)
	}
}

func runtimeFixtureExtremumAggregateCell(aggregate qsbridge.Aggregate, rows []runtimeFixtureRow, minimum bool) (qsbridge.ResultCell, bool, error) {
	var best qsbridge.ResultCell
	seen := false
	for _, row := range rows {
		cell, err := runtimeFixtureEvalRowExpr(aggregate.Input, row)
		if err != nil {
			return qsbridge.ResultCell{}, false, err
		}
		if cell.Kind == qsbridge.ValueNull {
			continue
		}
		if !seen {
			best = cell
			seen = true
			continue
		}
		cmp := runtimeFixtureCellCompare(cell, best)
		if (minimum && cmp < 0) || (!minimum && cmp > 0) {
			best = cell
		}
	}
	return best, seen, nil
}

func runtimeFixtureSortCandidates(sortSpecs []qsbridge.SortSpec, candidates runtimeFixtureCandidateRows) (runtimeFixtureCandidateRows, error) {
	rows := candidates.Rows
	rownums := candidates.Set.Rownums
	if len(sortSpecs) == 0 || len(rows) < 2 {
		return candidates, nil
	}
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	var sortErr error
	sort.SliceStable(order, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		less, err := runtimeFixtureRowsLess(sortSpecs, rows[order[i]], rows[order[j]])
		if err != nil {
			sortErr = err
			return false
		}
		return less
	})
	if sortErr != nil {
		return runtimeFixtureCandidateRows{}, sortErr
	}
	sortedRows := make([]runtimeFixtureRow, len(rows))
	sortedRownums := make([]qsbridge.QuantaRownum, len(rownums))
	for outputIndex, inputIndex := range order {
		sortedRows[outputIndex] = rows[inputIndex]
		sortedRownums[outputIndex] = rownums[inputIndex]
	}
	candidates.Rows = sortedRows
	candidates.Set.Rownums = sortedRownums
	return candidates, nil
}

func runtimeFixtureSortRows(sortSpecs []qsbridge.SortSpec, rows []runtimeFixtureRow) ([]runtimeFixtureRow, error) {
	if len(sortSpecs) == 0 || len(rows) < 2 {
		return rows, nil
	}
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	var sortErr error
	sort.SliceStable(order, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		less, err := runtimeFixtureRowsLess(sortSpecs, rows[order[i]], rows[order[j]])
		if err != nil {
			sortErr = err
			return false
		}
		return less
	})
	if sortErr != nil {
		return nil, sortErr
	}
	sortedRows := make([]runtimeFixtureRow, len(rows))
	for outputIndex, inputIndex := range order {
		sortedRows[outputIndex] = rows[inputIndex]
	}
	return sortedRows, nil
}

func runtimeFixtureRowsLess(sortSpecs []qsbridge.SortSpec, leftRow runtimeFixtureRow, rightRow runtimeFixtureRow) (bool, error) {
	for _, sortSpec := range sortSpecs {
		left, err := runtimeFixtureEvalRowExpr(sortSpec.Expr, leftRow)
		if err != nil {
			return false, err
		}
		right, err := runtimeFixtureEvalRowExpr(sortSpec.Expr, rightRow)
		if err != nil {
			return false, err
		}
		cmp := runtimeFixtureCellCompare(left, right)
		if cmp == 0 {
			continue
		}
		if sortSpec.Direction == qsbridge.SortDescending {
			return cmp > 0, nil
		}
		return cmp < 0, nil
	}
	return false, nil
}

func runtimeFixtureLimitRows(rows []runtimeFixtureRow, offset int, limit int) []runtimeFixtureRow {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(rows) {
		return nil
	}
	rows = rows[offset:]
	if limit > 0 && limit < len(rows) {
		return rows[:limit]
	}
	return rows
}

func runtimeFixtureLimitCandidates(candidates runtimeFixtureCandidateRows, offset int, limit int) runtimeFixtureCandidateRows {
	rows := candidates.Rows
	rownums := candidates.Set.Rownums
	if offset < 0 {
		offset = 0
	}
	if offset >= len(rows) {
		candidates.Rows = nil
		candidates.Set.Rownums = nil
		return candidates
	}
	rows = rows[offset:]
	rownums = rownums[offset:]
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
		rownums = rownums[:limit]
	}
	candidates.Rows = rows
	candidates.Set.Rownums = rownums
	return candidates
}

func runtimeFixtureMatchFragment(fragment qsbridge.QuantaQueryFragment, row runtimeFixtureRow) (bool, error) {
	if fragment.Index != "" && !strings.EqualFold(fragment.Index, "orders") && !strings.EqualFold(fragment.Index, "customers_qa") && !strings.EqualFold(fragment.Index, "orders_qa") && !strings.EqualFold(fragment.Index, "part_qa") && !strings.EqualFold(fragment.Index, "partsupp_qa") && !strings.EqualFold(fragment.Index, "supplier_qa") && !strings.EqualFold(fragment.Index, "customer_tpch_qa") && !strings.EqualFold(fragment.Index, "orders_tpch_qa") && !strings.EqualFold(fragment.Index, "lineitem_tpch_qa") {
		return false, fmt.Errorf("runtime fixture unsupported fragment index: %s", fragment.Index)
	}
	value, ok := row[fragment.Field]
	if !ok {
		value = qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
	if value.Kind == qsbridge.ValueNull && fragment.Negate && fragment.BSIOp == qsbridge.QuantaBSIOpEQ {
		return false, nil
	}
	matched, err := runtimeFixtureMatchCell(fragment, value)
	if err != nil {
		return false, err
	}
	if fragment.Negate || fragment.Operation == qsbridge.QuantaOperationDifference {
		return !matched, nil
	}
	return matched, nil
}

func runtimeFixtureMatchCell(fragment qsbridge.QuantaQueryFragment, cell qsbridge.ResultCell) (bool, error) {
	if fragment.NullCheck {
		return cell.Kind == qsbridge.ValueNull, nil
	}
	if fragment.HasLiteral {
		return runtimeFixtureCellEqual(cell, qsbridge.ResultCell{Kind: fragment.Literal.Kind, Value: fragment.Literal.Value}), nil
	}
	if fragment.HasLiteralRange {
		return runtimeFixtureCellCompare(cell, qsbridge.ResultCell{Kind: fragment.BeginLiteral.Kind, Value: fragment.BeginLiteral.Value}) >= 0 &&
			runtimeFixtureCellCompare(cell, qsbridge.ResultCell{Kind: fragment.EndLiteral.Kind, Value: fragment.EndLiteral.Value}) <= 0, nil
	}
	if len(fragment.Literals) > 0 {
		for _, literal := range fragment.Literals {
			if runtimeFixtureCellEqual(cell, qsbridge.ResultCell{Kind: literal.Kind, Value: literal.Value}) {
				return true, nil
			}
		}
		return false, nil
	}
	return runtimeFixtureMatchBSI(fragment, cell)
}

func runtimeFixtureMatchBSI(fragment qsbridge.QuantaQueryFragment, cell qsbridge.ResultCell) (bool, error) {
	if cell.Kind == qsbridge.ValueNull {
		return false, nil
	}
	if _, ok := cell.Value.(string); ok {
		cell = runtimeFixtureNormalizeTimeCell(cell)
		if cell.Kind == qsbridge.ValueNull {
			return false, nil
		}
	}
	if scaled, ok := runtimeFixtureScaledNumericCell(fragment.Field, cell); ok {
		cell = scaled
	}
	value, ok := runtimeFixtureInt64(cell)
	if !ok {
		return false, fmt.Errorf("runtime fixture supports BSI predicates only over integer cells: %s kind=%s type=%T value=%#v", fragment.Field, cell.Kind, cell.Value, cell.Value)
	}
	switch fragment.BSIOp {
	case qsbridge.QuantaBSIOpEQ:
		return runtimeFixtureCompareValue(value, fragment.Value, func(cmp int) bool { return cmp == 0 })
	case qsbridge.QuantaBSIOpGE:
		return runtimeFixtureCompareValue(value, fragment.Value, func(cmp int) bool { return cmp >= 0 })
	case qsbridge.QuantaBSIOpGT:
		return runtimeFixtureCompareValue(value, fragment.Value, func(cmp int) bool { return cmp > 0 })
	case qsbridge.QuantaBSIOpLE:
		return runtimeFixtureCompareValue(value, fragment.Value, func(cmp int) bool { return cmp <= 0 })
	case qsbridge.QuantaBSIOpLT:
		return runtimeFixtureCompareValue(value, fragment.Value, func(cmp int) bool { return cmp < 0 })
	case qsbridge.QuantaBSIOpRange:
		if fragment.Begin == nil || fragment.End == nil {
			return false, fmt.Errorf("runtime fixture range predicate missing bound: %s", fragment.Field)
		}
		return big.NewInt(value).Cmp(fragment.Begin) >= 0 && big.NewInt(value).Cmp(fragment.End) <= 0, nil
	case qsbridge.QuantaBSIOpBatchEQ:
		for _, candidate := range fragment.Values {
			if candidate != nil && big.NewInt(value).Cmp(candidate) == 0 {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("runtime fixture unsupported BSI op %q for %s", fragment.BSIOp, fragment.Field)
	}
}

func runtimeFixtureCompareValue(value int64, target *big.Int, compare func(int) bool) (bool, error) {
	if target == nil {
		return false, fmt.Errorf("runtime fixture predicate missing value")
	}
	return compare(big.NewInt(value).Cmp(target)), nil
}

func runtimeFixtureScaledNumericCell(field string, cell qsbridge.ResultCell) (qsbridge.ResultCell, bool) {
	scale, ok := runtimeFixtureScaledNumericFieldScale(field)
	if !ok {
		return qsbridge.ResultCell{}, false
	}
	number, ok := runtimeFixtureFloat64(cell.Value)
	if !ok {
		return qsbridge.ResultCell{}, false
	}
	factor := math.Pow10(scale)
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(math.Round(number * factor))}, true
}

func runtimeFixtureScaledNumericFieldScale(field string) (int, bool) {
	lower := strings.ToLower(field)
	if strings.EqualFold(field, "height") || strings.HasSuffix(lower, ".height") ||
		strings.EqualFold(field, "c_acctbal") || strings.HasSuffix(lower, ".c_acctbal") ||
		strings.EqualFold(field, "ps_supplycost") || strings.HasSuffix(lower, ".ps_supplycost") ||
		strings.EqualFold(field, "l_extendedprice") || strings.HasSuffix(lower, ".l_extendedprice") ||
		strings.EqualFold(field, "l_discount") || strings.HasSuffix(lower, ".l_discount") {
		return 2, true
	}
	return 0, false
}

func runtimeFixtureFieldCell(row runtimeFixtureRow, field qsbridge.FieldRef) (qsbridge.ResultCell, bool) {
	for _, name := range []string{
		field.QualifiedName(),
		field.Table.Table + "." + field.Name,
		field.Table.Table + "." + field.PhysicalName,
		field.PhysicalName,
		field.Name,
	} {
		if name == "" {
			continue
		}
		if cell, ok := row[name]; ok {
			return cell, true
		}
	}
	return qsbridge.ResultCell{}, false
}

func runtimeFixtureQualifiedFieldCell(row runtimeFixtureRow, field qsbridge.FieldRef) (qsbridge.ResultCell, bool) {
	if field.Table.Table == "" && field.Table.RefName() == "" {
		return runtimeFixtureFieldCell(row, field)
	}
	for _, name := range []string{
		field.QualifiedName(),
		field.Table.Table + "." + field.Name,
		field.Table.Table + "." + field.PhysicalName,
	} {
		if name == "" || strings.HasSuffix(name, ".") {
			continue
		}
		if cell, ok := row[name]; ok {
			return cell, true
		}
	}
	return qsbridge.ResultCell{}, false
}

func runtimeFixtureCellLess(left qsbridge.ResultCell, right qsbridge.ResultCell) bool {
	return runtimeFixtureCellCompare(left, right) < 0
}

func runtimeFixtureCellCompare(left qsbridge.ResultCell, right qsbridge.ResultCell) int {
	left = runtimeFixtureComparableCell(left)
	right = runtimeFixtureComparableCell(right)
	if runtimeFixtureNumericKind(left.Kind) || runtimeFixtureNumericKind(right.Kind) {
		leftNumber, leftOK := runtimeFixtureFloat64(left.Value)
		rightNumber, rightOK := runtimeFixtureFloat64(right.Value)
		if leftOK && rightOK {
			switch {
			case leftNumber < rightNumber:
				return -1
			case leftNumber > rightNumber:
				return 1
			default:
				return 0
			}
		}
	}
	leftText := fmt.Sprint(left.Value)
	rightText := fmt.Sprint(right.Value)
	switch {
	case leftText < rightText:
		return -1
	case leftText > rightText:
		return 1
	default:
		return 0
	}
}

func runtimeFixtureCellTruthy(cell qsbridge.ResultCell) bool {
	switch cell.Kind {
	case qsbridge.ValueBool:
		value, ok := cell.Value.(bool)
		return ok && value
	case qsbridge.ValueInt, qsbridge.ValueFloat:
		value, ok := runtimeFixtureFloat64(cell.Value)
		return ok && value != 0
	case qsbridge.ValueString:
		return strings.TrimSpace(fmt.Sprint(cell.Value)) != ""
	default:
		return false
	}
}

func runtimeFixtureCellEqual(left qsbridge.ResultCell, right qsbridge.ResultCell) bool {
	left = runtimeFixtureComparableCell(left)
	right = runtimeFixtureComparableCell(right)
	if leftNumber, leftOK := runtimeFixtureBoolComparableNumber(left); leftOK {
		if rightNumber, rightOK := runtimeFixtureBoolComparableNumber(right); rightOK {
			return leftNumber == rightNumber
		}
	}
	if runtimeFixtureNumericKind(left.Kind) || runtimeFixtureNumericKind(right.Kind) {
		leftNumber, leftOK := runtimeFixtureFloat64(left.Value)
		rightNumber, rightOK := runtimeFixtureFloat64(right.Value)
		if leftOK && rightOK {
			return leftNumber == rightNumber
		}
	}
	if runtimeFixtureStringSetContains(left, right) || runtimeFixtureStringSetContains(right, left) {
		return true
	}
	return fmt.Sprint(left.Value) == fmt.Sprint(right.Value)
}

func runtimeFixtureComparableCell(cell qsbridge.ResultCell) qsbridge.ResultCell {
	text, ok := cell.Value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return cell
	}
	millis, ok := runtimeFixtureParseTimeMillis(text)
	if !ok {
		return cell
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: millis}
}

func runtimeFixtureBoolComparableNumber(cell qsbridge.ResultCell) (float64, bool) {
	if value, ok := cell.Value.(bool); ok {
		if value {
			return 1, true
		}
		return 0, true
	}
	if runtimeFixtureNumericKind(cell.Kind) {
		return runtimeFixtureFloat64(cell.Value)
	}
	return 0, false
}

func runtimeFixtureStringSetContains(setCell qsbridge.ResultCell, valueCell qsbridge.ResultCell) bool {
	if setCell.Kind != qsbridge.ValueString || valueCell.Kind != qsbridge.ValueString {
		return false
	}
	set, setOK := setCell.Value.(string)
	value, valueOK := valueCell.Value.(string)
	if !setOK || !valueOK || !strings.Contains(set, ";") || strings.Contains(value, ";") {
		return false
	}
	for _, token := range strings.Split(set, ";") {
		if strings.TrimSpace(token) == value {
			return true
		}
	}
	return false
}

func runtimeFixtureNumericKind(kind qsbridge.ValueKind) bool {
	return kind == qsbridge.ValueInt || kind == qsbridge.ValueFloat
}

func runtimeFixtureFloat64(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func runtimeFixtureInt64(cell qsbridge.ResultCell) (int64, bool) {
	switch value := cell.Value.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	case time.Time:
		return value.UTC().UnixMilli(), true
	default:
		return 0, false
	}
}

func runtimeFixtureMaterializeRows(request qsbridge.QuantaMaterializationRequest, rows []runtimeFixtureRow) (qsbridge.QuantaProjectedRowSet, error) {
	if len(request.Rownums) != len(rows) {
		return qsbridge.QuantaProjectedRowSet{}, fmt.Errorf("runtime fixture materialization rownum count %d does not match row count %d", len(request.Rownums), len(rows))
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:        request.Index,
		LogicalShard: request.LogicalShard,
		Replica:      request.Replica,
		Rownums:      append([]qsbridge.QuantaRownum(nil), request.Rownums...),
	}
	for _, field := range request.ProjectionFields {
		vector := qsbridge.QuantaProjectionVector{
			Field:  field,
			Values: make([]qsbridge.ResultCell, len(rows)),
		}
		for rowIndex, row := range rows {
			vector.Values[rowIndex] = runtimeFixtureProjectionVectorCell(row, field)
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
	}
	return rowSet, nil
}

func runtimeFixtureProjectionVectorCell(row runtimeFixtureRow, field qsbridge.QuantaProjectionField) qsbridge.ResultCell {
	for _, name := range []string{field.Index + "." + field.Field, field.Index + "." + field.PhysicalName, field.Field, field.PhysicalName} {
		if name == "" || strings.HasSuffix(name, ".") {
			continue
		}
		if value, ok := row[name]; ok {
			return value
		}
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
}

func runtimeFixtureProjectSQLRows(projections []qsbridge.ProjectionColumn, raw qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, error) {
	projected := raw
	projected.ProjectionVectors = make([]qsbridge.QuantaProjectionVector, 0, len(projections))
	for _, projection := range projections {
		vector := qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Index:   raw.Index,
				Field:   runtimeFixtureProjectionName(projection),
				Type:    runtimeFixtureProjectionType(projection),
				Visible: true,
			},
			Values: make([]qsbridge.ResultCell, raw.CandidateCount()),
		}
		for rowIndex := range raw.Rownums {
			cell, err := runtimeFixtureEvalSQLExpr(projection.Expr, raw, rowIndex)
			if err != nil {
				return qsbridge.QuantaProjectedRowSet{}, err
			}
			vector.Values[rowIndex] = cell
		}
		projected.ProjectionVectors = append(projected.ProjectionVectors, vector)
	}
	return projected, nil
}

func runtimeFixtureProjectionName(projection qsbridge.ProjectionColumn) string {
	if projection.Alias != "" {
		return projection.Alias
	}
	switch expr := projection.Expr.(type) {
	case qsbridge.FieldExpr:
		return expr.Ref.Name
	case *qsbridge.FieldExpr:
		if expr != nil {
			return expr.Ref.Name
		}
	case qsbridge.CallExpr:
		return expr.Name
	case *qsbridge.CallExpr:
		if expr != nil {
			return expr.Name
		}
	}
	return "expr"
}

func runtimeFixtureProjectionType(projection qsbridge.ProjectionColumn) qsbridge.DataType {
	if projection.Type != qsbridge.DataTypeUnknown {
		return projection.Type
	}
	return qsbridge.ExprDataType(projection.Expr)
}

func runtimeFixtureEvalSQLExpr(expr qsbridge.Expr, rowSet qsbridge.QuantaProjectedRowSet, rowIndex int) (qsbridge.ResultCell, error) {
	switch value := expr.(type) {
	case qsbridge.LiteralExpr:
		return qsbridge.ResultCell{Kind: value.Kind, Value: value.Value}, nil
	case *qsbridge.LiteralExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{Kind: value.Kind, Value: value.Value}, nil
	case qsbridge.FieldExpr:
		return runtimeFixtureProjectedFieldCell(rowSet, value.Ref, rowIndex)
	case *qsbridge.FieldExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return runtimeFixtureProjectedFieldCell(rowSet, value.Ref, rowIndex)
	case qsbridge.CallExpr:
		return runtimeFixtureEvalSQLCall(value, rowSet, rowIndex)
	case *qsbridge.CallExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return runtimeFixtureEvalSQLCall(*value, rowSet, rowIndex)
	case qsbridge.BinaryExpr:
		return runtimeFixtureEvalSQLBinary(value, rowSet, rowIndex)
	case *qsbridge.BinaryExpr:
		if value == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return runtimeFixtureEvalSQLBinary(*value, rowSet, rowIndex)
	default:
		return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture unsupported projection expression %T", expr)
	}
}

func runtimeFixtureProjectedFieldCell(rowSet qsbridge.QuantaProjectedRowSet, field qsbridge.FieldRef, rowIndex int) (qsbridge.ResultCell, error) {
	for _, vector := range rowSet.ProjectionVectors {
		if vector.Field.Index != field.Table.Table || vector.Field.Field != runtimeFixtureProjectionFieldName(field) {
			continue
		}
		if rowIndex >= len(vector.Values) {
			return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture projection vector %s has no row %d", vector.Field.Field, rowIndex)
		}
		return vector.Values[rowIndex], nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
}

func runtimeFixtureProjectionFieldName(field qsbridge.FieldRef) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Name
}

func runtimeFixtureEvalSQLBinary(expr qsbridge.BinaryExpr, rowSet qsbridge.QuantaProjectedRowSet, rowIndex int) (qsbridge.ResultCell, error) {
	left, err := runtimeFixtureEvalSQLExpr(expr.Left, rowSet, rowIndex)
	if err != nil {
		return qsbridge.ResultCell{}, err
	}
	right, err := runtimeFixtureEvalSQLExpr(expr.Right, rowSet, rowIndex)
	if err != nil {
		return qsbridge.ResultCell{}, err
	}
	return runtimeFixtureEvalNumericBinary(expr.Op, left, right)
}

func runtimeFixtureEvalNumericBinary(op qsbridge.BinaryOp, left qsbridge.ResultCell, right qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	leftNumber, leftOK := runtimeFixtureFloat64(left.Value)
	rightNumber, rightOK := runtimeFixtureFloat64(right.Value)
	if !leftOK || !rightOK {
		return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture arithmetic projection requires numeric inputs")
	}
	switch op {
	case qsbridge.BinaryOpAdd:
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber + rightNumber}, nil
	case qsbridge.BinaryOpSubtract:
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber - rightNumber}, nil
	case qsbridge.BinaryOpMultiply:
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber * rightNumber}, nil
	case qsbridge.BinaryOpDivide:
		if rightNumber == 0 {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber / rightNumber}, nil
	default:
		return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture unsupported binary projection op %s", op)
	}
}

func runtimeFixtureEvalSQLCall(call qsbridge.CallExpr, rowSet qsbridge.QuantaProjectedRowSet, rowIndex int) (qsbridge.ResultCell, error) {
	args := make([]qsbridge.ResultCell, 0, len(call.Args))
	for _, arg := range call.Args {
		cell, err := runtimeFixtureEvalSQLExpr(arg, rowSet, rowIndex)
		if err != nil {
			return qsbridge.ResultCell{}, err
		}
		args = append(args, cell)
	}
	return runtimeFixtureDispatchSQLCall(call.Name, args)
}

func runtimeFixtureDispatchSQLCall(name string, args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	switch strings.ToLower(name) {
	case "tostring":
		return runtimeFixtureCallToString(args)
	case "toint":
		return runtimeFixtureCallToInt(args)
	case "tonumber":
		return runtimeFixtureCallToNumber(args)
	case "lower", "lcase":
		return runtimeFixtureCallStringUnary(args, strings.ToLower)
	case "upper", "ucase":
		return runtimeFixtureCallStringUnary(args, strings.ToUpper)
	case "length", "char_length":
		return runtimeFixtureCallLength(args)
	case "substr", "substring", "mid":
		return runtimeFixtureCallSubstr(args)
	case "todate":
		return runtimeFixtureCallToDate(args)
	case "timediff":
		return runtimeFixtureCallTimeDiff(args)
	case "hash.sha256":
		return runtimeFixtureCallSHA256(args)
	case "year", "yy", "mm", "monthofyear", "dayofweek", "hourofday", "hourofweek", "seconds":
		return runtimeFixtureCallDatePart(name, args)
	default:
		return qsbridge.ResultCell{}, fmt.Errorf("runtime fixture unsupported scalar function %q", name)
	}
}

func runtimeFixtureCallToString(args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, fmt.Errorf("tostring requires one argument")
	}
	if args[0].Kind == qsbridge.ValueNull {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(args[0].Value)}, nil
}

func runtimeFixtureCallSHA256(args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, fmt.Errorf("hash.sha256 requires one argument")
	}
	if args[0].Kind == qsbridge.ValueNull {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	sum := sha256.Sum256([]byte(fmt.Sprint(args[0].Value)))
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: hex.EncodeToString(sum[:])}, nil
}

func runtimeFixtureCallToInt(args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, fmt.Errorf("toint requires one argument")
	}
	value, ok := runtimeFixtureInt64(args[0])
	if !ok {
		number, numberOK := runtimeFixtureFloat64(args[0].Value)
		if !numberOK {
			return qsbridge.ResultCell{}, fmt.Errorf("toint requires numeric input")
		}
		value = int64(number)
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value}, nil
}

func runtimeFixtureCallToNumber(args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, fmt.Errorf("tonumber requires one argument")
	}
	value, ok := runtimeFixtureFloat64(args[0].Value)
	if !ok {
		return qsbridge.ResultCell{}, fmt.Errorf("tonumber requires numeric input")
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: value}, nil
}

func runtimeFixtureCallStringUnary(args []qsbridge.ResultCell, fn func(string) string) (qsbridge.ResultCell, error) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, fmt.Errorf("string function requires one argument")
	}
	if args[0].Kind == qsbridge.ValueNull {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fn(fmt.Sprint(args[0].Value))}, nil
}

func runtimeFixtureCallLength(args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, fmt.Errorf("length requires one argument")
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(len(fmt.Sprint(args[0].Value)))}, nil
}

func runtimeFixtureCallSubstr(args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 2 && len(args) != 3 {
		return qsbridge.ResultCell{}, fmt.Errorf("substr requires two or three arguments")
	}
	text := fmt.Sprint(args[0].Value)
	start, ok := runtimeFixtureInt64(args[1])
	if !ok {
		return qsbridge.ResultCell{}, fmt.Errorf("substr start requires integer input")
	}
	length := int64(len(text))
	if len(args) == 3 {
		var lengthOK bool
		length, lengthOK = runtimeFixtureInt64(args[2])
		if !lengthOK {
			return qsbridge.ResultCell{}, fmt.Errorf("substr length requires integer input")
		}
	}
	start-- // SQL string positions are 1-based.
	if start < 0 {
		start = 0
	}
	if start >= int64(len(text)) || length <= 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""}, nil
	}
	end := start + length
	if end > int64(len(text)) {
		end = int64(len(text))
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: text[start:end]}, nil
}

func runtimeFixtureCallToDate(args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, fmt.Errorf("todate requires one argument")
	}
	return runtimeFixtureNormalizeTimeCell(args[0]), nil
}

func runtimeFixtureCallTimeDiff(args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 3 {
		return qsbridge.ResultCell{}, fmt.Errorf("timediff requires end, start, and unit arguments")
	}
	end, endOK := runtimeFixtureTimeMillis(args[0])
	start, startOK := runtimeFixtureTimeMillis(args[1])
	if !endOK || !startOK {
		return qsbridge.ResultCell{}, fmt.Errorf("timediff requires time inputs")
	}
	unit := strings.ToLower(fmt.Sprint(args[2].Value))
	diff := end - start
	switch unit {
	case "millisecond", "milliseconds", "millis":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(diff)}, nil
	case "second", "seconds":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(diff) / float64(time.Second/time.Millisecond)}, nil
	case "minute", "minutes":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(diff) / float64(time.Minute/time.Millisecond)}, nil
	case "hour", "hours":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(diff) / float64(time.Hour/time.Millisecond)}, nil
	case "day", "days":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(diff) / float64((24*time.Hour)/time.Millisecond)}, nil
	default:
		return qsbridge.ResultCell{}, fmt.Errorf("timediff unit %q is not supported", unit)
	}
}

func runtimeFixtureCallDatePart(name string, args []qsbridge.ResultCell) (qsbridge.ResultCell, error) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, fmt.Errorf("%s requires one argument", name)
	}
	millis, ok := runtimeFixtureTimeMillis(args[0])
	if !ok {
		return qsbridge.ResultCell{}, fmt.Errorf("%s requires a time input", name)
	}
	t := time.UnixMilli(millis).UTC()
	switch strings.ToLower(name) {
	case "year", "yy":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(t.Year())}, nil
	case "mm", "monthofyear":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(t.Month())}, nil
	case "dayofweek":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(t.Weekday())}, nil
	case "hourofday":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(t.Hour())}, nil
	case "hourofweek":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(t.Weekday())*24 + int64(t.Hour())}, nil
	case "seconds":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(t.Second())}, nil
	default:
		return qsbridge.ResultCell{}, fmt.Errorf("date part function %q is not supported", name)
	}
}

func runtimeFixtureTimeMillis(cell qsbridge.ResultCell) (int64, bool) {
	if value, ok := runtimeFixtureInt64(cell); ok {
		return value, true
	}
	normalized := runtimeFixtureNormalizeTimeCell(cell)
	if normalized.Kind == qsbridge.ValueInt {
		value, ok := normalized.Value.(int64)
		return value, ok
	}
	return 0, false
}

func runtimeFixtureDistinctRows(rowSet qsbridge.QuantaProjectedRowSet) qsbridge.QuantaProjectedRowSet {
	if rowSet.CandidateCount() < 2 {
		return rowSet
	}
	seen := make(map[string]struct{}, rowSet.CandidateCount())
	keep := make([]int, 0, rowSet.CandidateCount())
	for rowIndex := 0; rowIndex < rowSet.CandidateCount(); rowIndex++ {
		key := runtimeFixtureDistinctRowKey(rowSet, rowIndex)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keep = append(keep, rowIndex)
	}
	if len(keep) == rowSet.CandidateCount() {
		return rowSet
	}
	distinct := rowSet
	distinct.Rownums = make([]qsbridge.QuantaRownum, len(keep))
	for outputIndex, rowIndex := range keep {
		distinct.Rownums[outputIndex] = rowSet.Rownums[rowIndex]
	}
	distinct.ProjectionVectors = make([]qsbridge.QuantaProjectionVector, len(rowSet.ProjectionVectors))
	for vectorIndex, vector := range rowSet.ProjectionVectors {
		copied := vector
		copied.Values = make([]qsbridge.ResultCell, len(keep))
		for outputIndex, rowIndex := range keep {
			copied.Values[outputIndex] = vector.Values[rowIndex]
		}
		distinct.ProjectionVectors[vectorIndex] = copied
	}
	return distinct
}

func runtimeFixtureDistinctRowKey(rowSet qsbridge.QuantaProjectedRowSet, rowIndex int) string {
	var b strings.Builder
	for _, vector := range rowSet.ProjectionVectors {
		if !vector.Field.Visible {
			continue
		}
		cell := vector.Values[rowIndex]
		b.WriteString(string(cell.Kind))
		b.WriteByte('=')
		b.WriteString(fmt.Sprint(cell.Value))
		b.WriteByte('\x00')
	}
	return b.String()
}

func runtimeFixtureDiagnosticsError(diagnostics qsbridge.DiagnosticSet) error {
	if len(diagnostics) == 0 {
		return fmt.Errorf("runtime fixture diagnostics blocked execution")
	}
	messages := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic.BlocksNative() {
			messages = append(messages, diagnostic.Error())
		}
	}
	if len(messages) == 0 {
		return fmt.Errorf("runtime fixture diagnostics blocked execution")
	}
	return errors.New(strings.Join(messages, "; "))
}
