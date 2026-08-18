# qsbridge

`qsbridge` is the native SQL planning vocabulary for QuantaStream. It is the
active home for SQL planning contracts, query classification, and native
execution vocabulary.

The package defines typed SQL engine concepts and tests their behavior. It
includes the parser and planning surfaces used by SQLRunner, inabox-direct, and
inabox-standard. Storage execution still lives behind runtime and protocol
boundaries rather than inside `qsbridge`.

`architecture_test.go` enforces this boundary by failing if `qsbridge` imports
runtime or storage packages such as `source`, `server`, `shared`, `grpc`,
`core`, or `qsruntime`. The package should describe handoff contracts to those
layers, not silently grow a runtime dependency on them.

## Goals

The first goal is to make Quanta's SQL processing explicit:

1. Bind SQL concepts to table and field identities.
2. Represent scalar expressions without depending on parser internals.
3. Preserve query shape before choosing an executor.
4. Classify native capabilities and blockers with stable diagnostics.
5. Lower supported query shapes into logical plan nodes.
6. Keep physical execution and legacy compatibility boundaries clean.

This gives us room to modernize the SQL engine without treating the old package
layout as sacred or rewriting everything at once.

## Non-Goals For The Current Scaffold

`qsbridge` does not currently:

- provide general SQL parsing beyond the tiny one-table direct SELECT scaffold
- execute logical plans
- replace `qlbridge`
- talk to bitmap, BSI, Consul, KV, or storage APIs
- own MySQL wire protocol behavior
- implement optimizer rules
- create runtime feature flags

Those pieces come later, after the vocabulary and boundaries are stable.

## Runtime And Compatibility Boundaries

`qsruntime` is the transitional runtime adapter boundary outside `qsbridge`.
It owns the neutral `ExecutionRequest`/`ExecutionResult` envelope, direct QIAB
executor hooks, route selection, and the current mixed staging surface while
native execution grows. `qsbridge` may describe requests for runtime execution,
but it must not import runtime packages or choose storage transports directly.

`qscompat` is the intended future quarantine for legacy compatibility code:
legacy gRPC/proxy adapters, old source/core/shared bridges, direct-session
migration shims, and legacy projector adapters. Those pieces may live in
`qsruntime` while the split would create more churn than clarity, but the
target boundary is `qscompat` and it should disappear once direct QIAB
execution covers the required SQL surface.

Compatibility code must not become the place where SQL meaning or bitmap
algebra is corrected. Query semantics belong in `qsbridge`; durable runtime
contracts belong in the runtime/executor layer; compatibility adapters only
translate between the new contracts and temporary compatibility surfaces. For
example,
negated bitmap predicates should be lowered as explicit bitmap `DIFFERENCE`
operations before they reach compatibility adapters, rather than relying on
adapter rewrites or historical `Negate` side flags. Once temporary
compatibility surfaces become unused, they should be deleted rather than
treated as long-lived infrastructure.

## Current Flow

The intended shape is:

```text
SQL text
  -> parser bridge
  -> catalog-backed binding
  -> QueryIR
  -> diagnostics
  -> native capability classification
  -> optimizer audit records
  -> logical plan
  -> physical plan
  -> executor
  -> MySQL-compatible result
```

Only the middle vocabulary exists today:

```text
catalog metadata
  -> table/field binding
  -> expression IR
  -> query shape IR
  -> diagnostics
  -> capability classification
  -> logical plan scaffold
```

## Result Delivery Notes

Quanta's first priority remains a first-class analytical engine, not
row-at-a-time OLTP behavior. The result-delivery contracts should still preserve
good time-to-first-byte behavior for interactive clients where it does not
conflict with analytical execution, late materialization, or HTAP priorities.

`LIMIT`, `OFFSET`, forward-only cursor metadata, and future projector iteration
should therefore be treated as one connected boundary. `OFFSET` should not force
early full-row materialization by itself: execution can skip candidate rownums
or projected batches before shaping visible result chunks. Cursor fetch and
`Projector.Next()` style iteration should expose progress over result chunks
while keeping predicate evaluation, candidate rownum production, and projection
materialization separable. This leaves room for interactive client latency
improvements without making the core planner optimize primarily for OLTP-style
pagination.

## Package Layers

The long-term package split keeps frequently changed planner and executor code
away from colder adapter surfaces. `client` is the stable logical
adapter-facing surface for rowsets, summaries, commands, sessions, and
compatibility views. `protocol` is narrower and colder: wire vocabulary,
handshake metadata, response descriptors, and protocol error mapping.
`plan` owns semantically correct query shaping; `opt` owns optional rewrites,
cost records, and plan-improvement policy that must not be required for
correctness.

`catalog.go` contains table, field, relationship, function, and
storage-profile metadata vocabulary plus a small in-memory catalog and cached
catalog wrapper for tests and scaffolding. Function placement defaults from
function kind when catalog entries do not specify it, while explicit predicate
placement is preserved for special cases such as sampling.

`catalog_expression.go` contains schema-owned expression metadata for
blind-column INSERT defaults and streaming ingest table selectors. It records
raw expression text, purpose, and row or payload dependencies without importing
the historical qlbridge expression VM.

`catalog_view.go` contains node-facing and query-facing projections of the
catalog. Node views carry lean physical storage, BSI, bitmap, and relationship
metadata; query views preserve semantic planner/runtime metadata such as
dictionary, rehydration, multiplicity, function, and relationship definitions.
This keeps node metadata needs expressible without making
`core.BasicTable` or `shared.Table` authoritative in the refactor.

`information_schema.go` defines the small virtual MySQL metadata tables exposed
to binding for `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS`.
Runtime supplies the rows from catalog metadata rather than backing these names
with physical Quanta tables.

`bridge.go` contains parser-neutral unbound statement and expression shapes that
can be populated by SQL parsers before binding, including
projection, inner/outer join, semi/anti membership, predicate, aggregate,
grouping, having, and ordering shapes. Predicate scopes preserve `ON`, `WHERE`,
and `HAVING` boundaries for later outer-join-safe planning.

`ir.go` contains table, field, predicate, join, aggregate, and result-shape
vocabulary.

`encoding.go` contains physical encoding, rehydration, value multiplicity, and
relationship traversal capability vocabulary. These types describe what Quanta
can evaluate natively without importing runtime bitmap or BSI packages. It also
provides canonical constructors for StringLexBSI, time-granularity BSI, and
scaled numeric BSI profiles so catalog builders do not hand-roll those rules.

`legacy_encoding.go` maps legacy Quanta index/storage labels into the new
encoding vocabulary so catalog migration can preserve existing schema meaning
while moving toward representation capabilities. Numeric and time legacy labels
flow through the canonical BSI profile helpers.
`legacy_dependency_inventory.go` records proxy-retirement dependency subjects
such as legacy session, table, projector, join, and helper surfaces. It
classifies each as preserve-behind-interface, move-to-compat, delete-after-proxy,
or research-needed so the compatibility burn-down can be tested and updated
without importing legacy packages.

`topology.go` records the high-level Quanta scaling contract: data and storage
I/O scale by adding data nodes, query processing scales by adding query
proxies, and metadata/cache consistency must account for multiple proxies
sharing the same underlying node set. It also names topology generation source
and invalidation semantics so placement and cache decisions can detect stale
topology metadata without implementing runtime watchers in qsbridge.

`placement.go` explains physical shard, replica, placement-policy, cache, and
topology implications for a `PhysicalScope`. It also records deterministic
data-shard addressability through rendezvous hashing: a stable node set, shard
key, cluster-wide Consul-backed replication factor, and topology generation
produce an ordered owner set. This gives future plan explain output and cache
keys a stable vocabulary for why work is routed to particular data nodes and
why proxy-local state may require cross-proxy consistency.

`expr.go` contains scalar expression nodes such as literals, fields, prepared
statement parameters, calls, binary operators, and aggregate references. Bound
function calls carry catalog provenance such as MySQL-compatible vs
Quanta-custom origin plus expression, aggregate, predicate, or table placement
so planning diagnostics can distinguish standard SQL from intentional
extensions and keep predicate-only functions such as sampling out of normal
projection semantics.
`relationship_aggregate_expr.go` contains neutral storage-expression tokens
for relationship aligned aggregates that runtime adapters and storage nodes can
share without importing each other.

`function_registry.go` centralizes built-in function metadata for SQL scalar
functions, SQL aggregates, catalog default expressions, and streaming selector
expressions. Parser, binder, runtime registration, and catalog-expression
validation should consume this registry instead of maintaining separate
hard-coded function lists.

`function_evaluator.go` defines the protocol-neutral function-call contract used
by context-specific evaluators. It carries bound registry metadata, binding
context, and `ResultCell` arguments so SQL execution, catalog defaults,
streaming selectors, and future UDF adapters can share one call shape while
keeping separate evaluator implementations.

`function_usage.go` derives a query-local function usage inventory from
`QueryIR`. It records scalar calls, aggregate slots, function name, origin,
placement, context, return type, and determinism in first-seen order so
diagnostics, explain, and future validators can reason about custom SQL without
re-walking the query tree.

`simple_parser.go` contains the first qsbridge-owned parser bridge. It accepts a
narrow one-table SELECT surface with direct-field projections, simple arithmetic
expressions, aggregate calls, optional aliases, one optional flat AND list of
field-to-literal or field-to-parameter comparison predicates, one single-field
GROUP BY, one direct-field or aggregate-alias ORDER BY, LIMIT, and OFFSET,
returning parser-boundary diagnostics for unsupported SQL. This keeps the
vertical database/sql slice honest without committing to the final parser
architecture.
`simple_lexer.go` contains the first qsbridge-owned lexical scaffold. It
tokenizes the narrow simple-parser SQL surface into keyword, identifier,
literal, operator, punctuation, placeholder, wildcard, EOF, and error tokens
with byte spans, but it is not yet wired into the active parser.

`view_expansion.go` rewrites supported logical view references before binding.
The first cut only inlines single-source, non-materialized views whose
definitions remain simple SELECTs, preserving ordinary table planning after the
rewrite.

`dictionary.go` contains the planner-facing StringEnum dictionary boundary,
including field identity, encoded values, versions, capabilities, update mode,
consistency mode, and a small cached resolver wrapper for tests and
scaffolding.

`catalog_cache.go` provides a small in-memory catalog cache wrapper for table,
view, relationship, and function lookups. It caches successful lookups and
diagnostics while returning copies so planner metadata callers do not alias
cached state.

`store.go` records metadata persistence domains and backend families for
catalog, dictionary, access policy, session, and prepared-plan metadata. It
keeps Consul, KVStore, adapter-owned stores, cache scope, and invalidation
modes visible as planning metadata without importing any backend APIs. User and
role metadata are represented as cached Consul/Consul-like metadata, while
access-policy enforcement remains adapter-owned. See `DESIGN_DECISIONS.md` for
the StringEnum dictionary persistence and propagation decision.

`query.go` contains `QueryIR`, projection and sort shapes, query support checks,
required-field discovery, and client-visible result-column metadata. Result
metadata keeps column name, type, nullability, and direct source-field identity
available for MySQL-compatible result handling without coupling planning to the
wire protocol.

`access.go` contains planner-derived authorization requirements. It records the
tables and fields that require SELECT, INSERT, UPDATE, or DELETE privileges so
future MySQL authentication/RBAC adapters have a clean hook. It does not
enforce access decisions.
`client_access_requirements.go` exposes those prepared-plan authorization
requirements as adapter-visible rows. It reports privilege, table identity,
alias, and field list without consulting or implementing an RBAC policy engine.
`client_access_requirement_summary.go` exposes aggregate authorization
requirement counts as adapter-visible rows. It reports privilege, table, field,
and mutation totals without enforcing policy decisions.
`authentication.go` contains the protocol-neutral authentication boundary. It
records login metadata, authentication methods/plugins, authenticated
principals, roles, and session creation metadata while leaving password
exchange, JWT/OAuth validation, and enterprise identity integration to adapters.
It names MySQL-compatible plugins such as `caching_sha2_password` and
`mysql_native_password`, plus disabled JWT/OAuth hooks, so future adapters have
stable metadata without qsbridge owning credential exchange.
`client_authentication_methods.go` exposes adapter-supplied authentication
method and plugin metadata as rows. It includes a built-in method inventory for
MySQL-compatible password flows, JWT/OAuth token flows, and external identity
hooks without implementing the wire handshake or identity provider integration.
`client_authentication_method_summary.go` exposes aggregate authentication
method counts as adapter-visible rows. It reports enabled, disabled, password,
token, external identity, and method-family totals for compatibility dashboards.
`client_authentication_summary.go` exposes one authentication request and
decision as adapter-visible rows. It reports method, requested user, default
schema, client address, authenticated principal, roles, attribute counts, and
diagnostics without storing credentials or implementing a wire handshake.
`connection.go` captures connection-level metadata for protocol adapters:
protocol profile, advertised client capabilities, authentication request,
attributes, capability negotiation policy, and the authenticated planning
session context. It does not implement a wire handshake, TLS negotiation, socket
lifecycle, or password exchange. It can also build a `PlanRequest` from the
authenticated connection plus adapter-owned catalog version, physical scope, and
optimization metadata.
`client_sql_driver.go` adapts qsbridge planning and execution contracts to
Go's `database/sql/driver` connector surface. It is connector-based rather
than globally registered, so tests and future adapters can exercise the path
without importing the historical qlbridge SQL driver.
`inmemory_native_executor.go` provides a storage-neutral native executor for
direct SELECT projections and flat AND-combined field-to-literal or
field-to-parameter comparison predicates.
It also applies direct-field ORDER BY plus LIMIT/OFFSET result shaping. It lets
connector and result-envelope tests scan real projected rows while bitmap, BSI,
and broader expression execution remain outside the qsbridge scaffold.
`inmemory_join.go` adds fixture-grade inner, left, and right join row assembly
for parser and driver compatibility tests, including null-extended rows for
outer joins.
`inmemory_projection.go` contains the executor-local expression projection
helpers for arithmetic projection evaluation and result-row chunking.
`inmemory_aggregate.go` contains executor-local global aggregate and grouped
count helpers for the in-memory native executor.
`projector_kernel.go` contains the executor-side projector lifecycle vocabulary
that replaces the old conceptual placement of join/materialization logic inside
`core.Projector`. It names candidate seeding, relationship-vector loading,
batching, late materialization, rehydration, row assembly, aggregation, and
ranking as separate kernel stages without importing bitmap, BSI, session, or
legacy runtime packages. Materialization dependencies have stable ids and probe
names so execution can explain field hydration before calling a runtime adapter.
Candidate batches and batch-aware materialization plans make `Projector.Next(n)`
style bounded work visible without tying the contract to the legacy projector.
The execution-plan sketch now turns those primitives into ordered handoffs for
seed, relationship-vector loading, batching, materialization, rehydration,
assembly, aggregation, and ranking, while relationship-vector projection results
can be folded into merged rownum-domain sets for downstream executor stages. The
executable projector-kernel runner can now call neutral relationship-vector and
materialization kernels, then pass materialized rowsets into result assembly
without importing runtime or legacy projector packages. Relationship-vector
projection kernel requests group the FK/vector BSI reads needed by a projector
plan before any runtime adapter touches storage.
`materialization.go` contains the native projection/materialization contract
for fetching columnar values from candidate rownums. Runtime implementations
may call native kernels, node adapters, or temporary legacy bridges, but the
request, rowset, diagnostic, batch, probe, and bitmap-result candidate-set
shapes remain owned by qsbridge. The grouped materialization-kernel request and
result surface is the native projector handoff for batch-aware field hydration.
The assembly helper merges materialized rowsets into a native rowset and result
chunks while keeping probes and diagnostics visible.
Runtime adapters can now route both direct projection and relationship
materialization through this grouped kernel seam, allowing native field readers
to be preferred while temporary compatibility materializers remain explicit
fallbacks.
`topn_rank.go` contains the executor-neutral top-N ranking row assembly
formerly embedded in the legacy projector path. Runtime adapters can feed it
bitmap value/cardinality pairs without importing `core.Projector`, while the
planner can continue to model `topn()` as a native aggregate handoff.
`relationship_execution.go` contains stable relationship-vector join planning,
adapter request/result, joined-row, rownum-domain translation, relationship
vector projection read/result, and low-level kernel dispatch contracts. It owns
the execution vocabulary for reduce, expand, semi, anti, and null-extension
primitives while runtime and compatibility packages provide implementations.
`relationship_vector_reader.go` contains the executor-neutral relationship
vector reader contract used by filter-domain normalization plus a deterministic
in-memory reader for native tests; inabox-direct vector-field derivation stays
outside this package.
`relationship_tuple_rowset.go` contains role-qualified tuple rows, tuple
rowsets, relationship-vector graph expansion, row filtering by result indexes,
and late-materialization projection into `QuantaProjectedRowSet`. These are
engine data-flow contracts rather than legacy runtime mechanics.
`relationship_tuple_values.go` contains role-qualified tuple value-key and
value-store contracts used when relationship-vector graph rows are late
materialized. The keys live in qsbridge because role-qualified materialization
is engine vocabulary; runtime packages still own residual filter execution and
aggregate execution while those paths depend on runtime mechanics.
`same_row_comparison.go` contains the native same-row field comparison contract
for BSI-backed predicates such as `l_receiptdate > l_commitdate`. It names the
request/result/kernel and planner detection vocabulary so future executors can
return rownum bitmaps without hydrating both fields through `core.Projector`.
`quanta_execution_result.go` contains the native bitmap/runtime result envelope
used between executor kernels and runtime adapters. It is lower-level than the
client-facing `ExecutionResult` and carries projected rowsets, diagnostics,
probes, counts, and statement metadata without implying a wire response shape.

`quanta_bitmap_result.go` contains the native bitmap-query result shape. It
captures candidate rownums, counts, and bitmap predicate error metadata before
runtime adapters turn those candidates into projected rows or protocol-visible
responses.

`inspection_result.go` contains the protocol-neutral inspection row shape,
stable inspection result-column metadata, and row-to-result-chunk conversion
used by explain/inspection surfaces without tying those rows to a particular
runtime adapter.
`quanta_intermediate.go` lowers supported execution requests from `QueryIR`
into dependency-light Quanta bitmap-query and native aggregate request shapes.
It is the first physical execution-dialect adapter: `QueryIR` remains the
planner-owned representation while this layer keeps legacy bitmap, projector,
and gRPC payloads behind a later runtime adapter boundary.
Native aggregate requests are declarative handoff contracts only; `qsruntime`
is the package boundary that may later translate them into legacy in-process or
network runtime calls.
`client_handshake.go` prepares pre-authentication greeting metadata for protocol
adapters: server version, auth plugin, charset/collation, connection status
flags, and client capability policy. It does not serialize handshake packets,
exchange credentials, or open sockets.
`client_handshake_summary.go` exposes one handshake greeting exchange as
adapter-visible rows. It reports session id, protocol/driver, greeting strings,
status flags, accepted capabilities, support, and diagnostics without
serializing packets or exposing credentials.
`client_charset.go` exposes adapter-supplied character set and collation rows
for MySQL-compatible discovery paths. It filters and shapes metadata without
owning the server's full charset registry or handshake packet serialization.
`client_charset_summary.go` exposes aggregate character set and collation
metadata. It summarizes filtered character set and collation counts, default
and compiled collations, multi-byte charsets, and zero-sortlen collations
without owning the charset registry.
`client_storage_engines.go` exposes adapter-supplied SHOW ENGINES-style storage
engine metadata. It reports support flags and capability traits without importing
or probing runtime storage implementations.
`client_storage_engines_summary.go` exposes aggregate SHOW ENGINES-style storage
engine metadata. It summarizes filtered engine counts, support buckets, and
transaction/XA/savepoint capability counts without importing or probing runtime
storage implementations.
`client_connection.go` composes connection authentication, optional
adapter-owned session registration, and connection-close/session-cleanup
metadata. It gives future MySQL/gRPC handshake and disconnect code a stable
exchange while leaving password exchange, packet flow, TLS, and socket lifecycle
outside qsbridge.
`client_connection_summary.go` exposes connection accept and close exchanges as
adapter-visible rows. It reports session identity, user/schema, accepted
capabilities, registration/cleanup flags, response status, support, and
diagnostics without implementing the wire handshake.
`client_capabilities.go` exposes the active protocol and accepted client
capabilities for a connection as protocol-neutral rows. It is intended for
adapter diagnostics and compatibility metadata, not executor capability
selection. Structured explain bundles and plan-cache policy metadata are
separate protocol capabilities so adapters can advertise management surfaces
without implying different execution semantics.
`client_capability_summary.go` exposes aggregate protocol and client capability
counts as adapter-visible rows. It separates protocol support from accepted
client flags and reports execution, management, transport, and session families.
`compatibility.go` describes the qsbridge scaffold's own compatibility
manifest: which capabilities are native-planning vocabulary, metadata-only
surfaces, adapter-owned boundaries, runtime-owned boundaries, audit-only
records, or intentionally deferred work. It is descriptive, not a runtime
feature-flag system. Structured explain and plan-cache policy are listed as
client metadata capabilities so adapters can discover those surfaces through
the same compatibility/readiness path as broader statement and catalog
metadata.
`client_compatibility.go` exposes that manifest as adapter-visible rows for
diagnostics, management tooling, and release-readiness checks without probing
storage or executing plans.
`sql_feature.go` describes the higher-level SQL compatibility matrix for the
scaffold. It maps user-visible SQL areas such as projections, predicates,
joins, membership, aggregates, mutations, prepared execution, cursors, and
known blockers to native-planning, metadata-only, boundary-only, audit-only, or
deferred status. Function compatibility has its own category so
MySQL-compatible built-ins and Quanta-custom functions can be reported without
pretending they have identical compatibility semantics.
Protocol metadata surfaces such as structured explain, profile, compatibility,
readiness, and plan-cache policy are tracked as metadata-only SQL features.
`client_sql_feature.go` exposes that SQL feature matrix as adapter-visible rows
for diagnostics, roadmap reporting, and compatibility checks without invoking a
parser, optimizer, executor, or legacy fallback.
`sql_feature_coverage.go` compares a prepared plan against the SQL feature
matrix, marking which features are present in that statement, whether the
feature is supported by the scaffold, and which capabilities or diagnostics
serve as evidence.
`client_sql_feature_coverage.go` exposes that per-prepared-plan coverage as
adapter-visible rows so explain/diagnostic tools can report statement-specific
readiness without re-planning or executing.
`readiness.go` derives a compact scaffold readiness summary from the
compatibility manifest and SQL feature matrix. It counts native-planning,
metadata-only, boundary-only, audit-only, and deferred items without inventing
runtime state.
`client_readiness.go` exposes that summary as adapter-visible rows for
management dashboards, release checks, and diagnostic tooling.
`client_readiness_detail.go` exposes normalized compatibility and SQL feature
items as adapter-visible rows so dashboards can inspect the concrete readiness
surface behind the aggregate counts.
`client_change_user.go` models protocol-level reauthentication on an existing
connection. It preserves the connection/session id, authenticates replacement
principal metadata, negotiates accepted capabilities, previews the session
transition, and can optionally replace adapter-owned session registry metadata
without implementing password exchange or socket behavior.
`client_change_user_summary.go` exposes one change-user exchange as
adapter-visible rows. It reports previous/next user and schema, accepted
capabilities, session application, response status, support, and diagnostics
without exposing credentials or token material.
`connection_handoff.go` composes authenticated connection metadata with the
planning service and protocol-aware final handoff. It gives adapters a
connection-aware metadata envelope for single and batch requests while still
avoiding execution, session mutation, or network ownership.
`client_statement.go` represents adapter-provided SQL text bundles for
single-statement and multi-statement client requests. It validates
multi-statement capability and builds ordered `PlanRequest` values, but it does
not split SQL text, parse statements, or own protocol buffering.
`client_parse_preview.go` runs the configured parser bridge over a client
statement bundle and exposes parser-only unbound statement shape counts. It is a
compatibility checkpoint before catalog binding, planning, legacy fallback, or
execution.
`client_parse_preview_summary.go` exposes aggregate parser-preview shape counts
for statement kinds, unbound tables, projections, joins, memberships,
predicates, grouping, ordering, blockers, and parse diagnostics.
`client_plan.go` prepares each statement in a client statement bundle through
the planning service and returns ordered prepared-plan metadata. It preserves
multi-statement request ordering and diagnostics without executing statements or
owning protocol buffering.
`client_plan_summary.go` exposes those prepared client-plan bundles as
adapter-visible rows. It reports statement order, schema/catalog/user context,
logical and physical root kinds, parameter/result/access counts, scope hints,
and statement diagnostics without executing or carrying full SQL text as row
data.
`client_plan_lifecycle.go` centralizes adapter-facing SELECT vs mutation lifecycle
classification and step counts so plan, prepare, native request, explain, and
handoff summaries use the same lifecycle vocabulary.
`client_handoff.go` carries ordered client statement plans through the existing
protocol-aware authorization and routing handoff. It lets adapters inspect one
native, fallback, protocol rejection, route rejection, or access-denial decision
per statement without executing or buffering results.
`client_handoff_summary.go` exposes aggregate handoff counts as adapter-visible
rows. It reports statement, native, legacy fallback, policy rejection, access
denial, protocol rejection, read/write, SELECT/mutation lifecycle, and
diagnostic totals before result preview or response shaping.
`client_route_decision.go` exposes those final route and handoff decisions as
adapter-visible rows. It reports native eligibility, route reason, protocol
support, authorization status, read/write intent, SELECT/mutation lifecycle,
and diagnostics for management and debugging surfaces without executing native
or legacy plans.
`client_route_policy.go` exposes named routing-policy profiles as
adapter-visible rows. It reports compatibility, native-only, legacy-only, and
native-disabled policy behavior without changing how route decisions are made.
`client_route_policy_summary.go` exposes aggregate routing-policy counts as
adapter-visible rows. It reports default, native-allowed, fallback-allowed,
rejecting, and native-routing gate counts for compact management dashboards.
`client_dispatch_preview.go` exposes those dispatch previews as adapter-visible
rows so management and debugging surfaces can see whether a request would reach
native execution, legacy fallback, or no executor, with read/write intent and
SELECT/mutation lifecycle metadata, without calling either path.
`client_dispatch_preview_summary.go` exposes aggregate dispatch-preview counts
as adapter-visible rows. It reports native, legacy, no-target, configured,
missing-executor, and will-dispatch totals for deployment checks before any
executor is called.
`client_dispatch_targets.go` exposes dispatch target boundary profiles as
adapter-visible rows. It names the native executor, legacy executor, and
terminal no-dispatch targets without configuring or invoking any executor.
`client_dispatch_target_summary.go` exposes aggregate dispatch-target counts as
adapter-visible rows. It reports total, runtime-owned, executor-required,
configurable, and terminal target counts for compact deployment checks.
`client_result.go` derives ordered protocol-facing result previews from client
handoff metadata. It exposes query schemas, statement/OK response metadata, and
protocol errors for adapters without calling native or legacy executors.
`client_exchange.go` composes the client statement bundle, final handoff, and
result preview into one adapter-facing exchange envelope. It is the highest
level compatibility scaffold so far, and it still does not execute statements,
call fallback, buffer rows, mutate sessions, or own protocol state.
`client_exchange_summary.go` exposes aggregate metadata for that full client
exchange. It reports request, handoff, preview, response, route, and response
kind counts, read/write and SELECT/mutation lifecycle counts, plus diagnostics,
giving adapters a compact health row without duplicating per-statement response
detail.
`client_response.go` maps a client exchange to an ordered response sequence for
protocol adapters. Each item identifies query, statement, or error response
metadata plus `MoreResults`/`Final` flags, without serializing packets or
executing plans. Response items also carry protocol-neutral flags for query,
statement, error, streaming, completion, and cursor state so adapters can map
the sequence to MySQL-style server status metadata later.
`client_response_summary.go` exposes those response sequence descriptors as
adapter-visible rows. It summarizes response kind, route outcome, result status,
read/write intent, SELECT/mutation lifecycle, schema size, row/OK metadata,
error counts, flags, and SQL text without executing or serializing the
response.
`client_describe.go` exposes SQL and prepared-handle metadata for adapters that
need to describe parameters, result schemas, or OK/status shapes without
executing the statement or mutating session state.
`client_describe_summary.go` exposes one SQL or prepared-handle describe
exchange as adapter-visible rows. It reports source kind, handle identity,
query kind, read/write intent, SELECT/mutation lifecycle, parameter/result
counts, result/statement response flags, support, SQL length, and diagnostics
without executing the statement.
`client_prepared_metadata.go` exposes prepare response details as rows: one row
per prepared parameter and result column, including logical type, protocol type,
wire type, nullability, source identity, and flags without executing or
serializing the statement.
`client_prepared_metadata_summary.go` exposes aggregate prepared metadata counts
for parameters, result columns, nullable rows, source-backed rows, and flagged
protocol rows.
`client_catalog.go` exposes schemas, tables, columns, and table field-list
metadata for protocol adapters. Field-list requests can apply simple
MySQL-style wildcards to logical or physical column names without executing a
statement.
`catalog_metadata.go` defines enumerable catalog metadata interfaces for
schemas, tables, columns, relationships, dictionaries, and registered
functions used by SHOW and information-schema style adapters without requiring
the core catalog to be fully enumerable. The in-memory scaffold can represent
explicit schemas even when they do not yet contain tables, which is required for
MySQL-compatible current-database selection semantics.
`client_catalog_summary.go` exposes aggregate catalog shape by schema. It
reports table, column, relationship, dictionary-backed field, StringEnum, and
searchable-field counts without probing runtime storage or metadata backends.
`client_metadata_store_summary.go` exposes metadata-store boundary profiles as
adapter-visible rows. It reports domain, backend, cache requirement, cache
scope, invalidation mode, mutability, distribution, ownership, and consistency
notes for refactor diagnostics.
`client_catalog_functions.go` exposes registered SQL function metadata for
adapter discovery paths. It reports name, kind, argument/return types, aliases,
origin, placement, native support, and deterministic status without invoking the
parser or executor. This keeps MySQL-compatible functions distinct from
supported Quanta-custom SQL such as `topn()` and from legacy custom functions
that need review. Predicate-only custom functions such as
`sample_stratified()` should remain catalog-visible so sampling capability can
be retained and redesigned without treating it as an ordinary projection scalar.
`client_function_usage.go` exposes the function calls used by one prepared plan
as adapter-visible rows. It records origin, placement, query context, return
type, and determinism so explain and diagnostics can show why a query depends on
custom SQL, predicate-only functions, or ordinary MySQL-compatible scalar calls.
`client_table_status.go` exposes SHOW TABLE STATUS-style table traits from
catalog metadata. It reports storage profile flags and table shape counts
without querying runtime storage statistics.
`client_table_status_summary.go` exposes aggregate SHOW TABLE STATUS-style
table trait counts from catalog metadata. It summarizes filtered table counts,
storage flags, field/relationship totals, and distinct engine/index shapes
without querying runtime storage statistics.
`client_catalog_indexes.go` exposes primary-key, encoded-field-index, and
relationship-backed key metadata for one table. It gives adapters enough shape
for SHOW INDEX/driver metadata paths without querying storage or building
runtime relation vectors.
`client_catalog_encodings.go` exposes field representation metadata for one
table. It reports encoding kind, legacy storage name, multiplicity,
rehydration, predicate/projection capabilities, time granularity, numeric scale,
StringLexBSI prefix/remainder settings, and search flags without touching
runtime storage.
`client_catalog_dictionaries.go` exposes dictionary-backed field metadata for
one table. It reports dictionary identity, version, cardinality, advertised
matching/mutation capabilities, update mode, consistency mode, and
cache-invalidation needs without choosing or probing the future dictionary
persistence backend.
`client_catalog_constraints.go` exposes primary-key and relationship-backed
constraint metadata for information-schema style adapters. It reports
constraint names, types, columns, ordinals, and referenced table fields without
probing runtime relation vectors.
`client_catalog_relationships.go` exposes table-local relationship metadata for
foreign-key and information-schema style adapters. It reports source/target
fields, direction, cardinality, relationship encoding, and traversal
capabilities without touching runtime relation vectors.
`client_command.go` models non-SQL protocol commands such as ping, quit,
connection reset, and init-schema. It gives adapters OK/session/close metadata
without forcing command packets through SQL planning or owning network state.
`client_schema.go` prepares current-schema changes for `USE` and init-schema
commands. It can optionally validate the requested schema against enumerable
catalog metadata before previewing or applying session state, but it does not
mutate sessions unless an adapter-owned registry is supplied.
`client_schema_summary.go` exposes one schema-selection exchange as
adapter-visible rows. It reports requested/previous/next schema names, session
application, response status, support, and diagnostics for database-selection
compatibility work.
`client_command_capabilities.go` exposes the recognized non-SQL command surface
as rows. It reports command payload/session/close behavior and required
protocol capabilities without executing the command.
`client_command_capability_summary.go` exposes aggregate non-SQL command
capability counts as adapter-visible rows for compact protocol-readiness checks.
`client_command_summary.go` exposes one prepared non-SQL command exchange as
adapter-visible rows. It reports payload shape, close/session effects, response
status, support, and diagnostics without exposing arbitrary command payload
text.
`client_prepared.go` gives prepared-statement adapters a matching metadata path:
prepare/register one SQL statement, execute a registered handle through the
same protocol-aware handoff and response-preview scaffolds, and report missing
handles as protocol errors without reparsing or executing.
`client_prepare_summary.go` exposes one prepare/register exchange as
adapter-visible rows. It reports handle identity, query kind, registration and
support flags, parameter/result-column counts, SQL length, and diagnostics
without executing or repeating the full SQL text.
`client_prepared_execution_summary.go` exposes single and batch prepared
execution exchanges as adapter-visible rows. It reports handle identity, query
kind, handoff path, read/write intent, SELECT/mutation lifecycle,
response/result status, binding and batch-set counts, support, and diagnostics
without exposing parameter values.
`client_prepared_close_summary.go` exposes one prepared close exchange as
adapter-visible rows. It reports handle identity, close flag, response
kind/status, support, and diagnostics without executing or deallocating anything
outside the adapter-owned close exchange.
`client_prepared_reset.go` models prepared-statement reset metadata. It
validates a registered handle, returns OK/status response metadata, and clears
adapter-owned long-parameter state without closing the prepared statement or
executing SQL.
`client_prepared_reset_summary.go` exposes one prepared reset exchange as
adapter-visible rows. It reports handle identity, reset/long-data clearing
flags, response kind/status, support, and diagnostics without executing or
closing the prepared statement.
`prepared_long_data.go` defines the adapter-owned long-parameter registry
contract and in-memory implementation used to accumulate prepared statement
parameter fragments without executing the statement.
`client_prepared_long_data.go` models COM_STMT_SEND_LONG_DATA-style fragment
storage over an adapter-owned registry. It records fragment sizes and
accumulated parameter state without retaining or exposing payload bytes in
summaries.
`client_prepared_long_data_status.go` exposes accumulated prepared long-data
parameter metadata as rows. It reports handle identity, parameter label, value
kind, chunk count, total bytes, and final-fragment state without retaining or
displaying payload bytes.
`client_prepared_long_data_status_summary.go` exposes aggregate prepared
long-data inventory counts. It summarizes accumulated states, named statements,
final states, string-kind parameters, total chunks, total bytes, largest state,
and distinct prepared handles without retaining or displaying payload bytes.
`client_prepared_long_data_summary.go` exposes one prepared long-data store
exchange as adapter-visible rows. It reports handle/parameter identity,
fragment byte counts, accumulated state, support, and diagnostics while keeping
payload bytes outside qsbridge.
`client_prepared_statement_status.go` exposes registered prepared-statement
handles as adapter-visible rows. It reports handle identity, schema/catalog/user
scope, query kind, support flag, parameter/result counts, physical scope, and
diagnostics without reading the prepared-plan cache or executing statements.
`client_prepared_statement_status_summary.go` exposes aggregate prepared-handle
inventory counts as adapter-visible rows. It reports named, supported,
unsupported, parameter, result-column, diagnostic, placement, and cache totals.
`authorization.go` contains the adapter-facing authorization request and
decision boundary. qsbridge derives requirements and can carry allow/deny
diagnostics, but authentication, role loading, policy evaluation, and session
mutation stay outside the package.

`access_policy.go` provides a small in-memory policy authorizer for adapters
and tests. It supports user and role grants at table or field scope, but it is
not a persistent RBAC engine, GRANT/REVOKE implementation, or enterprise policy
store.
`client_grants.go` exposes SHOW GRANTS-style metadata for the current session
over an adapter-owned access policy. It returns grant rows and formatted grant
text for matching user and role principals without becoming the persistent RBAC
engine.
`client_grant_summary.go` exposes aggregate SHOW GRANTS-style counts for the
current session. It reports user, role, privilege, table, and field-scope totals
without becoming the persistent RBAC engine.

Prepared-statement placeholders are represented as parameter expressions with
stable index/name, type, and nullability metadata. `QueryIR` can report required
parameters in first-seen order, and inspection exposes that list for future
MySQL prepared-statement adapters and planning diagnostics.
`parameters.go` adds an execute-time binding scaffold that validates supplied
positional or named values against required placeholders without executing a
plan.
`client_parameter_bindings.go` exposes execute-time parameter binding metadata
as rows. It reports required placeholders, supplied value kinds, missing/extra
or mismatched values, and bound status without executing or displaying supplied
values.
`client_parameter_binding_summary.go` exposes aggregate execute-time parameter
binding counts as adapter-visible rows. It reports required, named, positional,
present, bound, missing, extra, mismatch, and nullability diagnostics without
displaying supplied values.

`prepared.go` contains the protocol-facing prepared-plan snapshot used by
future MySQL-style prepare/execute adapters. It keeps SQL text, parameter
metadata, result columns, statement metadata, plans, inspection, and diagnostics
together, and can bind execute-time values without invoking an executor.
`plan_invariant.go` validates prepared-plan snapshots against their source
`QueryIR` metadata. It reports consistency checks for support status, query
kind, parameters, access requirements, and result columns without re-planning or
executing.
`client_plan_invariant.go` exposes those prepared-plan invariant checks as
adapter-visible rows for diagnostics and test harnesses.
`prepare_metadata.go` adds adapter-owned prepared-statement identity and a
protocol-neutral prepare description envelope. qsbridge can describe parameters
and result columns for a prepared statement, but it does not allocate statement
ids, own a statement registry, or close/deallocate statements itself.
`prepared_registry.go` provides an optional in-memory prepared-statement
registry for adapters that want qsbridge-shaped handle, lookup, and close
metadata during refactor work. It is separate from deterministic plan caching
and does not execute statements or own protocol sessions.
`prepared_cache.go` defines the process-local prepared-plan cache boundary and
a small sharded in-memory implementation for adapters and tests. It does not
own invalidation policy, session storage, or execution.
`client_prepared_cache.go` exposes optional prepared-plan cache inspection rows
for diagnostics and management tooling. It reports cache keys, statement
identity, schema/catalog/user scope, read/write intent, SELECT/mutation
lifecycle, result shape counts, and physical cache scope without exposing
cached plan internals or executing statements.
`client_prepared_cache_summary.go` exposes aggregate prepared-plan cache counts
for diagnostics and management tooling. It summarizes filtered cache entries,
support state, read/write and SELECT/mutation lifecycle counts, result shape
counts, placement/cache policy buckets, and distinct schema/user scope without
exposing cached plan internals or executing statements.

`execution.go` contains the future executor handoff contract. It combines a
bound prepared plan with execution options such as request id, max rows, batch
size, streaming, cursor mode, cancellation intent, and explain/profile flags.
It validates options and carries result/access/session metadata, but it does
not execute plans.
`client_native_request_summary.go` exposes single and batch native execution
request descriptors as adapter-visible rows. It reports request id, SQL, schema,
user, support status, result shape, session/access counts, execution options,
parameter counts, and diagnostics without calling a native executor.
`executor.go` contains interface-only native and legacy executor boundaries plus
a small dispatcher over final handoff decisions. Implementations live outside
qsbridge; the package still imports no bitmap, BSI, storage, or legacy runtime
packages.
`client_executor_status.go` exposes configured native and legacy executor
boundaries as adapter-visible rows. It reports whether each boundary is present
for single and batch requests without calling either executor.
`client_executor_status_summary.go` exposes aggregate executor-boundary status
as adapter-visible rows. It reports configured, missing, single-request, and
batch-request counts for deployment-readiness checks without calling executors.
`dispatch_preview.go` describes the final executor boundary a handoff would
use, including native executor, legacy fallback executor, rejected handoff, and
missing-executor diagnostics. It also defines dispatch target profiles for
adapter and deployment inspection. It is non-executing metadata only.
`cursor.go` contains protocol-neutral forward-only cursor metadata for streaming
or cursor-oriented result delivery. It records cursor intent and result progress
without implementing cursor storage, fetch, or close mechanics.
`cursor_registry.go` provides an optional in-memory cursor metadata registry for
adapters that need to track open, exhausted, or closed cursor descriptors. It
does not store result rows or implement fetch behavior.
`client_cursor.go` models cursor open, advance, and close exchanges over
adapter-owned cursor registries. It tracks cursor state and result metadata
without implementing wire-level cursor commands or runtime row fetching.
`client_cursor_status.go` exposes current adapter-owned cursor descriptors as
rows for management and diagnostics. It inventories cursor id, request id,
mode, state, batch sizing, max rows, and position without reading result rows.
`client_cursor_status_summary.go` exposes aggregate cursor inventory counts for
management and diagnostics. It tracks cursor lifecycle buckets, mode buckets,
batch sizing, max-row settings, and consumed row positions without reading
result rows.
`client_cursor_fetch.go` validates cursor fetch requests against registered
forward-only cursor metadata and computes requested row counts, batch defaults,
and max-row clipping without advancing cursor position or reading rows.
`client_cursor_fetch_summary.go` exposes one cursor fetch validation exchange
as adapter-visible rows. It reports cursor identity, position, batch/max-row
settings, requested row count, final-fetch state, support flag, and diagnostics
without advancing or reading the cursor.
`client_cursor_lifecycle_summary.go` exposes cursor open, advance, and close
exchanges as adapter-visible rows. It reports operation, cursor identity, state,
position, sizing, applied/supported flags, and diagnostics without storing or
moving result rows.
`batch.go` extends that contract to multiple execute-time parameter sets for
prepared batch execution. It validates each set independently and keeps the
request metadata protocol-neutral; it does not loop over a mutator or call an
executor.
`client_batch_result_summary.go` exposes batch execution result envelopes as
adapter-visible rows. It reports one row per batch item with status, result
kind, read/write intent, SELECT/mutation lifecycle, row/affected counts,
diagnostics, and session-action counts without executing batch items.
`client_batch_result_chunk_summary.go` exposes one row per batch item result
chunk. It reports item ordinal, batch/request ids, chunk sequence, row count,
final state, cumulative item result status, cursor state, and diagnostics
without reading row payloads.
`client_batch_result_payload_summary.go` exposes payload shape for each batch
item result. It reuses the single-result payload summary vocabulary per item so
batch adapters can inspect row width, value-kind, missing-cell, and null-cell
metadata without reading result values.

`result.go` contains the protocol-neutral execution response envelope: result
columns, row chunks, OK metadata, diagnostics, completion state, returned-row
count, and session actions. It also defines a batch response envelope that
groups one result per parameter set and aggregates rows, affected rows,
diagnostics, cancellation, and session actions. These shapes define what a
future executor should produce without implementing execution.
`client_execution_result_summary.go` exposes one execution result envelope as
adapter-visible rows. It reports request id, result kind/status, completion,
read/write intent, SELECT/mutation lifecycle, row and affected-row counts,
result shape counts, cursor/profile/cancellation state, session actions, and
diagnostics without serializing result data.
`client_result_chunk_summary.go` exposes one row per execution result chunk. It
reports sequence, row count, final-chunk state, cumulative result status,
cursor state, and diagnostics without retaining or displaying row payloads.
`client_result_payload_summary.go` exposes result payload shape by ordinal. It
reports declared column name/type, observed cell counts, missing/null counts,
and value-kind sets without exposing result values or serializing rows.
`result_schema.go` maps logical result columns to conservative protocol-facing
column descriptors for MySQL, gRPC, Go, HTTP, or generic adapters. It preserves
logical type/source/nullability metadata plus derived column flags without
implementing wire encoding or value serialization.
`client_result_schema_summary.go` exposes result-preview schema columns as
adapter-visible rows. It reports statement ordinal, column ordinal, name,
source, read/write intent, SELECT/mutation lifecycle, logical type, protocol
type, wire type, nullability, flags, and SQL text for query results without
serializing field packets or executing statements.
`statement_response.go` maps non-row statement metadata to protocol-facing
OK/status response descriptors, including affected rows, last insert id,
warning counts, optional warning/note detail, status text, requested session
actions, and status flags. It does not build MySQL packets or apply session
mutations.
`client_statement_response_status.go` exposes one statement response's OK/status
metadata as rows. It reports affected rows, last insert id, warnings, status
text, session-action counts, transaction markers, status flags, and diagnostics
without serializing an OK packet.
`client_batch_statement_response_status.go` exposes OK/status metadata for each
batch item result. It reports item identity, affected rows, last insert id,
warnings, status text, session-action counts, transaction markers, protocol
status flags, and diagnostics without serializing OK packets or applying
session mutations.
`client_batch_statement_response_status_summary.go` exposes aggregate OK/status
metadata for batch item results. It summarizes item counts, affected rows,
warnings, session actions, transaction markers, diagnostics, and protocol status
flag buckets without serializing OK packets or applying session mutations.
`client_transaction_summary.go` exposes transaction response metadata as rows.
It reports BEGIN/COMMIT/ROLLBACK action, status text, support/applied flags,
session-action count, warnings, status flags, and diagnostics without owning
transaction state or storage semantics.
`client_warnings.go` exposes warning/note detail as protocol-neutral rows for
SHOW WARNINGS-style adapter behavior. It uses statement response metadata and
does not query runtime state or serialize packets.
`client_warnings_summary.go` exposes aggregate warning/note counts for
SHOW WARNINGS-style adapter behavior. It summarizes warning count, notice rows,
level buckets, coded rows, and SQLState rows without querying runtime state or
serializing packets.
`client_batch_warnings.go` exposes warning/note detail for batch item results.
It preserves per-item request identity and aggregate warning count while only
returning rows for notices that have detail metadata.
`client_batch_warnings_summary.go` exposes aggregate warning/note counts for
batch item results. It preserves count-only warnings while summarizing detailed
warning rows, level buckets, coded rows, SQLState rows, and item/request
coverage without querying runtime state or serializing packets.
`lifecycle.go` contains request lifecycle and cancellation metadata. It gives
MySQL and future gRPC adapters a typed way to identify cancelable requests,
validate cancellation handoffs, and record canceled result envelopes without
wiring qsbridge to goroutines, contexts, or runtime interruption mechanics.
`client_lifecycle.go` records cancellation exchanges against adapter-owned
execution registries. It can mark single or batch results as canceled but does
not interrupt goroutines, sockets, or storage work itself.
`execution_registry.go` provides an optional in-memory registry for adapter
owned in-flight request metadata. It can map request ids to single or batch
execution descriptors and create cancellation metadata, but adapters still own
actual interruption, timeout, and connection behavior.
`client_process.go` exposes process-list style metadata over the execution
registry. It returns request id, kind, status, user, schema, cancelability, and
SQL text as protocol-neutral rows without owning goroutines or executor state.
`client_process_summary.go` exposes aggregate process-list metadata, including
single versus batch requests, status buckets, and cancelable work counts for
compact operational views.
`client_cancellation_summary.go` exposes one cancellation exchange as
adapter-visible rows. It reports request identity, prior status, cancellation
reason, recorded/supported flags, resulting single or batch status, diagnostics,
and message without interrupting runtime work itself.
`client_cancellation_profiles.go` exposes static cancellation capability
profiles as adapter-visible rows. It documents client-request, timeout, and
shutdown cancellation requirements without installing runtime cancellation
mechanics.
`client_cancellation_profile_summary.go` exposes aggregate cancellation
capability counts as adapter-visible rows for compact readiness and management
surfaces.
`client_topology.go` exposes adapter-supplied shard and replica placement
metadata. It gives future diagnostics, management commands, and plan
explanations stable shard/replica identifiers without making qsbridge discover
cluster state or probe runtime storage.
`client_rendezvous_placement.go` exposes rendezvous shard placement decisions
as adapter-visible rows. It reports shard key, topology generation,
replication factor, owner rank, node id, completeness, and placement cache key
without consulting runtime topology services.
`client_status_variables.go` exposes SHOW STATUS-style adapter-supplied name
and value metadata with wildcard filtering. qsbridge shapes the result rows but
does not own server counters or runtime metric collection.
`client_status_variables_summary.go` exposes aggregate SHOW STATUS-style
metadata over adapter-supplied values. It summarizes filtered variable counts,
empty values, numeric-looking values, and common command/thread/connection
status prefixes without owning server counters or runtime metric collection.
`client_statistics.go` formats adapter-supplied status values for lightweight
protocol statistics commands. It provides a stable summary string without
collecting metrics or touching runtime state.
`client_statistics_summary.go` exposes aggregate protocol statistics metadata.
It summarizes sorted status variable counts, summary text length, numeric/empty
values, and command/thread/connection status families without collecting
runtime metrics.
`profile.go` contains protocol-neutral explain/profile metadata. It can expose
logical and physical explain text when requested by execution options and leaves
runtime timing/counter population to future executors.
`client_profile.go` shapes execution profile metadata into adapter-visible rows
for explain text, timings, counters, and diagnostics. It does not collect
runtime counters or execute profiled statements.
`client_profile_summary.go` exposes aggregate single-execution profile metadata,
including row totals, read/write intent, SELECT/mutation lifecycle,
explain/timing/counter/diagnostic section counts, and profile request flags.
`client_batch_profile.go` shapes per-item batch execution profile metadata into
adapter-visible rows. It preserves item/request identity while reusing the same
read/write intent, SELECT/mutation lifecycle, explain, timing, counter, and
diagnostic vocabulary as single-result profiles.
`client_batch_profile_summary.go` exposes aggregate batch explain/profile
metadata. It summarizes item coverage, read/write and SELECT/mutation lifecycle
item counts, plus explain, timing, counter, and diagnostic row counts without
executing profiled statements.
`native_plan_executor.go` provides a plan-only native executor scaffold that
completes query and batch result envelopes without reading bitmap, BSI, or
storage data.

Non-row statements have an explicit statement-result shape for OK metadata such
as affected rows, last insert id, warnings, status text, and requested session
actions. Logical and physical plans represent these as statement nodes instead
of forcing INSERT, UPDATE, DELETE, session-affecting statements, or DDL through
SELECT scan planning. `session_statement.go` represents actions such as `USE`
or `SET` as metadata for protocol/session owners; qsbridge does not mutate
session state.
`transaction.go` adds metadata-only transaction actions for begin, commit, and
rollback statements. These actions are visible to protocol/session owners, but
qsbridge does not implement transaction state or storage semantics.
`client_transaction.go` turns begin, commit, and rollback actions into
statement-response and session-action exchange metadata, optionally applying
session state through the adapter-owned session registry without owning
transaction storage semantics.
`client_session.go` previews and optionally applies requested session actions
against an adapter-owned session registry. It is the exchange boundary for
schema, variable, SQL mode, time-zone, reset, and transaction session metadata.
`client_session_state.go` maps statement response session actions into
protocol-neutral session-state change rows such as schema changes, system
variables, transaction state, reset markers, and change-user markers. It does
not apply the changes or serialize MySQL session tracking packets.
`client_session_state_summary.go` exposes aggregate session-state metadata for
statement responses. It summarizes schema, variable, transaction, reset, and
change-user action counts without applying session mutations.
`client_session_summary.go` exposes one session action exchange as
adapter-visible rows. It reports before/after schema, SQL mode and variable
counts, action-family counts, applied/support flags, and diagnostics without
mutating session state itself.
`client_batch_session_state.go` maps batch item session actions into
protocol-neutral session-state rows with item and request identity. It gives
adapters enough metadata to reason about per-item schema, variable,
transaction, reset, or change-user changes without applying them.
`catalog_planning_trace.go` exposes catalog-backed field, encoding, dictionary, and capability evidence from a planned statement.
`predicate_planning_trace.go` exposes predicate placement, scope, operator, field evidence, and native capability traces from a planned statement.
`execution_handoff_trace.go` exposes the native executor-facing request boundary, including options, physical root, strategies, fields, result shape, and diagnostics.
`select_lifecycle.go` exposes a runtime-neutral simple SELECT planning lifecycle trace from parse through diagnostics.
`select_execution.go` turns a planned simple SELECT into a completed empty
result envelope with schema, diagnostics, profile, and final-chunk metadata
before storage-backed row production exists.
`client_select_lifecycle_summary.go` exposes simple SELECT planning lifecycle
stages as adapter-visible rows so protocol adapters can inspect parse, bind,
QueryIR, logical, physical, result-schema, and diagnostic progress.
`subquery_ir.go` records planner-neutral subquery request, result, and binding
shapes so scalar and membership subqueries can move through planning without
being represented as SQL text.
`subquery_helper_plan.go` describes temporary helper execution plans for
subquery shapes that still need compatibility execution while the native
planner/executor path is filled in. In qsbridge vocabulary, helper-shaped
subquery execution is intentionally transitional: durable behavior should move
into named planner rules, native subquery steps, or executor kernels as the
compatibility layer is retired.
`native_subquery_step.go` exposes native subquery execution step descriptors
that let the executor represent subquery work as explicit operations instead
of hidden rewrites.
`mutation_lifecycle.go` exposes a runtime-neutral INSERT/UPDATE/DELETE planning lifecycle trace from parse through statement-result diagnostics.
`mutation_validation.go` contains planner-facing mutation legality checks for
target table, assignment, predicate, broad-write, and parameter-shape rules.
`client_mutation_lifecycle_summary.go` exposes mutation planning lifecycle stages
as adapter-visible rows so protocol adapters can inspect parse, bind, QueryIR,
logical, physical, statement-result, and diagnostic progress.
`client_batch_session_state_summary.go` exposes aggregate session-state
metadata for batch results. It summarizes item coverage, changed items, schema
changes, variable changes, transaction markers, resets, and change-user actions
without applying session mutations.

Mutation statements also carry executor-neutral mutation shape metadata. INSERT
binding records the target table, resolved target columns, and one or more
parser-neutral rows of value expressions so batch inserts and prepared
statement parameters have a clean planning contract before any runtime mutator
is wired in. UPDATE binding records target assignments and WHERE predicates,
while DELETE binding records the target table and WHERE predicates. These
shapes are planning contracts only; they do not execute mutations.

`client_mutation_summary.go` exposes prepared mutation shape as adapter-visible
rows. It reports write kind, target table, insert row count, assignment count,
predicate count, parameter count, target columns, predicate scopes, support,
and diagnostics without invoking mutators or storage.

`diagnostic.go` contains stable reason codes, severities, phases, source spans,
and conversions from blockers, predicates, joins, and catalog/schema failures.
`protocol_error.go` maps those internal diagnostics into protocol-facing
SQLSTATE/vendor-code style errors so MySQL and future network adapters can
produce compatible error responses without scraping diagnostic strings.
`client_diagnostics.go` exposes diagnostics as adapter-visible rows for
SHOW ERRORS-style inspection. Supplied diagnostics are returned as data, while
connection-level diagnostics still block the metadata exchange itself.
Rows preserve source-span coordinates when diagnostics provide them, giving
future parser and bridge errors a stable way to point client tooling at the
relevant SQL region.
`client_diagnostics_summary.go` exposes aggregate diagnostic metadata. It
summarizes severity mix, field-linked diagnostics, and source-span coverage for
SHOW ERRORS-style inspection without querying runtime state.
`client_batch_diagnostics.go` exposes diagnostics from a batch result envelope
and each batch item as adapter-visible rows. It preserves batch/item scope,
request identity, SQLSTATE/vendor-code mapping, source spans, and field
metadata while keeping connection diagnostics as exchange blockers.
`client_batch_diagnostics_summary.go` exposes aggregate diagnostic metadata for
batch result envelopes. It summarizes deduplicated batch/item rows, severity
mix, field-linked diagnostics, and source-span coverage without querying
runtime state.
`protocol.go` records protocol/driver capability profiles and validates
requested execution shapes such as prepared execution, batch execution,
streaming, cursors, cancellation, explain, and profile metadata. It does not
implement any wire protocol.
`wire_adapter.go` records the boundary between packet/RPC adapters, qsbridge
metadata contracts, deployment-owned auth/session integration, and executor
invocation. It makes MySQL and future gRPC wire ownership explicit without
implementing packet framing, command decoding, result serialization, sockets, or
RPC services.
`transport.go` records transport and execution-placement boundaries separately
from protocol semantics. It distinguishes MySQL wire clients, future gRPC API
surfaces, internal proxy-to-node traffic, and embedded QIAB direct calls so
shared port numbers or deployment topology do not leak into SQL planning
contracts.
`adapter_surface.go` names the public and internal adapter lanes that sit above
protocol and transport details: MySQL server, gRPC API/control plane, embedded
QIAB, and internal execution transport. It keeps these surfaces explicit without
turning qsbridge into a packet router or runtime transport implementation.
`adapter_contract.go` records the implementation contract checklist for those
surfaces. It ties each adapter lane to qsbridge metadata contracts, adapter or
runtime ownership, compatibility status, and implementation notes without
implementing protocol handlers or executors.
`adapter_rollout.go` records ordered rollout phases for each adapter surface:
metadata inventory, adapter shell, shadow validation, compatibility route, and
runtime enablement. These are planning milestones, not runtime feature flags.
`adapter_readiness.go` combines adapter surface, contract summary, and rollout
summary metadata into one readiness report per surface. It records whether a
surface is metadata-ready, whether runtime enablement is still blocked, and
which phase comes next.
`driver.go` records intended client-driver compatibility targets for generic
MySQL clients, Node.js, Python, Java/JDBC, Go `database/sql`, future native Go
APIs, and future gRPC APIs. These profiles include protocol capabilities and
auth plugin targets, including structured explain and plan-cache policy metadata
for native Go and gRPC, but remain metadata only; protocol adapters still own
actual driver compatibility testing and wire behavior.
`client_protocol_negotiation.go` exposes one protocol execution negotiation as
adapter-visible rows. It reports requested mode/options, advertised protocol
capabilities, supported status, and diagnostics without executing or opening a
network protocol path.
`client_protocol_negotiation_summary.go` exposes aggregate protocol negotiation
metadata. It summarizes support, advertised capabilities, diagnostics, and
requested execution-option counts without executing or opening a network path.
`client_wire_adapter_boundary.go` exposes wire/server ownership boundaries as
adapter-visible rows. It reports concern, protocol, owner, permanence,
metadata-only status, and detail so the future MySQL server adapter can remain a
thin packet translation layer over qsbridge contracts.
`client_transport_boundary.go` exposes transport and placement boundaries as
adapter-visible rows. It reports role, kind, protocol, owner, execution
placement, networked status, port independence, metadata-only status, and detail
without implementing routing or runtime transport.
`client_adapter_surface.go` exposes named adapter surfaces as adapter-visible
rows. It reports audience, protocol, transport, placement, ownership, client or
control-plane visibility, embedded/internal flags, and qsbridge metadata usage
without opening sockets or invoking executors.
`client_adapter_surface_summary.go` exposes aggregate adapter-surface counts by
audience, placement, protocol, transport, and qsbridge metadata usage for
compact compatibility dashboards.
`client_adapter_contract.go` exposes adapter-surface implementation contracts as
adapter-visible rows. It reports surface, concern, layer, status, owner,
required flag, adapter/runtime ownership, metadata contract name,
implementation note, and detail for readiness planning.
`client_adapter_contract_summary.go` exposes aggregate adapter-contract
readiness counts by surface. It reports total, required, metadata-only,
boundary-only, deferred, adapter-owned, runtime-owned, and qsbridge-owned
contract counts for compact management views.
`client_adapter_rollout.go` exposes ordered adapter rollout phases as
adapter-visible rows. It reports surface, phase, order, status, owner, required
contract concerns, runtime-blocking flag, and detail without enabling any
runtime routing path.
`client_adapter_rollout_summary.go` exposes aggregate rollout readiness counts
by adapter surface. It reports phase totals, metadata-only, boundary-only,
deferred, runtime-blocking, qsbridge-owned, adapter-owned, and runtime-owned
counts for compact planning dashboards.
`client_adapter_readiness.go` exposes combined adapter readiness reports as
adapter-visible rows. It joins surface identity, protocol/transport/placement,
contract counts, rollout counts, metadata-ready/runtime-ready flags, and next
phase into one compact management view.
`client_adapter_readiness_gates.go` exposes adapter release-readiness gates as
adapter-visible rows. It combines contract readiness and rollout phases into a
checklist with ready, runtime-blocking, blocker-count, and next-gate flags.
`client_adapter_readiness_gate_summary.go` exposes aggregate release-gate
counts by adapter surface. It reports total gates, ready gates,
runtime-blocking gates, blocker totals, next gate, and contract/metadata/runtime
readiness flags for compact dashboard views.
`client_adapter_readiness_next_actions.go` exposes the next release-readiness
action per adapter surface. It reports the next gate, order, owner, status,
runtime-blocking flag, blocker count, and detail without requiring clients to
filter the full gate checklist.
`client_adapter_readiness_blockers.go` exposes the concrete blockers behind
adapter readiness as adapter-visible rows. It reports whether each blocker came
from a deferred contract or runtime-blocking rollout phase, preserving phase,
concern, owner, status, and detail for drill-down diagnostics.
`client_adapter_readiness_blocker_summary.go` exposes aggregate blocker counts
by adapter surface. It reports contract versus rollout blockers, deferred and
boundary-only work, runtime-blocking totals, and ownership counts for compact
release-readiness dashboards.
`client_adapter_readiness_summary.go` exposes one aggregate adapter readiness
row across all surfaces. It reports surface counts, metadata-ready/runtime-ready
totals, surface-type counts, contract totals, deferred work, and runtime-blocking
phase counts for high-level release planning.
`client_driver_compatibility.go` exposes the client-driver target matrix as
adapter-visible rows. It reports ecosystem, protocol, status, representative
drivers, capabilities, auth plugins, and notes for diagnostics and
release-readiness checks.
`client_driver_compatibility_summary.go` exposes aggregate driver-target counts
as adapter-visible rows. It reports protocol, typed API, capability, and
authentication-family totals for compact compatibility dashboards.

`classifier.go` summarizes whether a bound query can run natively and which
capabilities or diagnostics are present.

`bind.go` contains catalog-backed table, field, and function name resolution for
future parser bridge work.

`planner.go` contains the runtime-neutral planning facade that composes a parser
bridge, catalog-backed binding, diagnostics, logical planning, physical
planning, and inspection into one stable envelope without executing anything.

`service.go` contains the adapter-facing planning service facade. It composes
the planner, prepared-plan snapshots, optional prepared-plan caching, parameter
binding, single and batch execution-request construction, route decisions,
protocol capability negotiation, and optional authorization delegation while
still leaving execution, authentication, policy storage, wire behavior, and
session mutation outside qsbridge.

`route.go` contains the adapter-facing native-vs-legacy route decision
scaffold. Compatibility mode chooses native only when supported and otherwise
returns an explicit legacy-fallback decision; native-only mode rejects
unsupported plans; legacy-only mode forces fallback. Native routing can also be
disabled explicitly as a feature gate, causing compatibility mode to fall back
and native-only mode to reject. Named route policy profiles make those choices
visible as metadata for adapters and dashboards. qsbridge records the decision
but does not call either runtime path.

`fallback.go` contains the metadata-only legacy fallback handoff. It carries SQL
text, effective schema, session context, execution options, bound parameters,
route decision, and diagnostics for protocol adapters that must invoke the
legacy compatibility path. Batch fallback uses the same boundary while
preserving one bound parameter set per batch item. It does not call the legacy
runtime.
`client_fallback_summary.go` exposes single and batch legacy fallback handoff
descriptors as adapter-visible rows. It reports request id, SQL, schema, user,
roles, route reason, support flags, execution options, parameter counts, and
diagnostics without invoking the legacy runtime.
`handoff.go` combines route decisions and authorization decisions into one final
adapter-facing handoff classification for single and batch execution: native
execution, legacy fallback, policy rejection, or access denial. Authorization
denial takes precedence over routing so adapters do not accidentally fall back
to legacy after access has been rejected. The handoff outcome also exposes final
diagnostics and protocol-error mappings so MySQL and future adapters can choose
an error response without rebuilding qsbridge decision logic.
Protocol-aware handoff variants add capability negotiation to the same final
decision surface, preserving precedence as authorization denial, protocol
rejection, then route selection.

`session.go` contains the planner-facing session metadata boundary: session id,
user identity, effective roles, current schema, SQL modes, time zone, and
session variables. It does not implement authentication, authorization,
transactions, or runtime session storage.
`session_catalog.go` overlays connection-local temporary table metadata on top
of the durable catalog for binding and metadata reads. Temporary tables shadow
durable tables or views within one session, but qsbridge still only describes
the catalog view; runtime owns applying session actions.
`temporary_table_rows.go` contains clone helpers for connection-local temporary
table row payloads carried by session actions. Runtime owns INSERT/SELECT
execution for those rows; qsbridge only keeps the session payload contract
copy-safe.
`session_registry.go` provides an optional in-memory session metadata registry
for adapters that need to store authenticated sessions or apply session
transitions during refactor work. It does not own network connections,
transactions, or session lifetimes.
`client_session_status.go` exposes adapter-owned session registry metadata as
rows for diagnostics and management surfaces. It reports session id, user,
schema, time zone, roles, SQL modes, and variable counts without owning auth,
network, or transaction state.
`client_session_status_summary.go` exposes aggregate session inventory counts
for diagnostics and management surfaces. It summarizes selected schemas, time
zones, roles, SQL modes, variables, distinct users, and distinct schemas without
owning auth, network, or transaction state.
`session_transition.go` can preview the before/after session state implied by
metadata actions such as `USE`, `SET`, SQL mode changes, and time-zone changes.
It does not mutate live sessions or invent transaction state.
`client_session_variables.go` exposes SHOW VARIABLES-style session metadata as
protocol-neutral rows. It includes selected database, SQL mode, time zone, and
adapter/user session variables with optional wildcard filtering, without
querying runtime state.
`client_session_variables_summary.go` exposes aggregate SHOW VARIABLES-style
session metadata. It summarizes filtered variable counts, built-in and adapter
variable buckets, empty/numeric values, and selected schema, SQL mode, and
time-zone presence without querying runtime state.
`client_sql_modes.go` exposes adapter-supplied SQL compatibility mode metadata
as rows, including supported/default flags and whether each mode is enabled for
the current session.
`client_sql_modes_summary.go` exposes aggregate SQL mode metadata. It
summarizes filtered mode counts, supported/default/enabled buckets, and enabled
default or supported combinations without querying runtime state.

`cachekey.go` contains conservative, deterministic plan-cache identity
scaffolding. SQL text, effective schema, session identity, roles, SQL modes,
time zone, variables, catalog version, and physical scope participate so future
prepared-plan or plan-cache layers do not accidentally reuse plans across
compatibility, authorization, or metadata-version boundaries. It also records a
typed cache-key policy that keeps display-only explain/profile options and
execute-only parameter, batching, and cursor choices out of prepare-time cache
identity.
`client_plan_cache_key.go` exposes a deterministic plan-cache key breakdown as
adapter-visible rows. It shows which SQL, schema, catalog version, user/session,
variable, shard, replica, routing, placement, and cache-scope components feed a
digest without reading or mutating the cache.
`client_plan_cache_policy.go` exposes the prepared-plan cache identity policy
as adapter-visible rows so tools can explain why each factor is included,
display-only, audit-only, or execute-only.
`sharded_cache.go` contains a small lock-sharded, bounded LRU shared-memory
utility intended for catalog, dictionary, and prepared-plan metadata caches
where contention should stay local to a shard. It reports hit, miss, eviction,
occupancy, and configured-limit stats so management surfaces and future dynamic
shared-memory sizing controllers can observe and adjust cache pressure without
embedding tuning policy in the primitive.
`shared_memory_tuner.go` contains a deterministic shared-memory sizing
controller. It observes bounded shared-memory stats, calculates interval hit
ratios and eviction pressure, and proposes hold/grow/shrink decisions without
running timers, spawning goroutines, or mutating caches directly.

`plan.go` contains executor-independent logical plan nodes and traversal helpers.

`physical.go` contains executor-facing physical plan nodes and shard, replica,
placement, and cache scope vocabulary. `explain.go` contains structured and
human-readable logical and physical plan explanations plus section-selection
options for logical, physical, optimizer detail, optimizer summary, diagnostic,
function, and native blocker output.
`client_explain.go` exposes section-selected explain bundles as adapter-visible
rows. It reports selected sections, logical and physical node summaries,
optimizer records and aggregate optimizer counts, diagnostics, function usage,
and native blockers without executing the plan.
`client_explain_summary.go` exposes aggregate explain row counts by selected
section, plan node section, optimizer section, diagnostics, functions, native
blockers, and support state for compact management surfaces.

`optimizer.go` contains optimizer audit records for applied, advisory, and
blocked rewrite decisions. Records include rule id, status, category, impact,
reason, before/after summaries, diagnostics, and affected fields so explain and
management tooling do not have to infer query-tuning meaning from free text. It
also contains read-only advisory detectors, such as the top-N grouped-count
shape that could later lower to native `topn()`. The planner merges these
detected advisories with caller-supplied optimizer traces before building
inspection and explain metadata. It does not implement query rewrites yet.
`client_optimizer.go` exposes optimizer audit records as adapter-visible rows
for explain, diagnostics, and future query-tuning tools. It reports rewrite
category and impact without applying or inventing optimizer rewrites.
`client_optimizer_summary.go` exposes aggregate optimizer trace counts by
rewrite status, category, impact, diagnostics, and supported state for
management dashboards.

`inspection.go` contains a management-facing inspection report that bundles
classification, optimizer audit records, logical explain, and physical explain.
It also carries query-local function usage and native blocker inventory so
explain output can show standard, custom, predicate-only, and blocked constructs
without rewalking the IR.
`client_inspection.go` prepares one statement for explain/profile inspection,
preserving the prepared plan, inspection report, profile text, and diagnostics
without executing the plan.
`client_inspection_report.go` exposes that inspection report as compact
adapter-visible rows covering summaries, sources, fields, access, encodings,
relationships, mutations, and diagnostics without making adapters walk nested
planning structs directly.
`client_inspection_summary.go` exposes one explain/profile planning exchange as
adapter-visible rows. It reports request id, query kind, support,
explain/profile flags, plan node counts, capability/parameter/result counts,
profile row counts, SQL length, and diagnostics without executing the plan.

## Future Package Boundaries

The scaffold currently keeps qsbridge in one Go package while the vocabulary is
still changing quickly. That avoids premature exported helper churn and import
cycles while types such as result metadata, diagnostics, protocol negotiation,
handoff decisions, and client rowsets are still being shaped together.

`package_boundary.go` records the intended future package split as typed
metadata and classifies current qsbridge files into core, client, protocol,
catalog, planning, execution, cache, and testkit destinations without moving
files yet.
`client_package_boundary_summary.go` exposes that intended package split as
adapter-visible rows for diagnostics and refactor tooling. It reports boundary
name, split phase/order, responsibility counts, file prefixes, exact file names,
and descriptive responsibility text without changing package layout. It also
reports intended dependency direction so future package moves can preserve the rule that
foundational vocabulary does not import planner, executor, or adapter-facing
rowsets.

Once these interfaces harden, the package should split along stable dependency
directions:

- `qsbridge`: foundational SQL value and expression vocabulary, diagnostics,
  authentication/authorization concepts, connection identity, and package-map
  metadata.
- `qsbridge/catalog`: schema, table, field, relationship, encoding,
  dictionary, and metadata-store boundaries.
- `qsbridge/plan`: parser-neutral binding, query IR shaping, function usage,
  capability classification, optimizer audit records, logical plans, physical
  plans, and native blocker inspection.
- `qsbridge/execution`: executor-neutral execution contracts, fallback
  decisions, prepared statements, cursors, sessions, transactions, result
  envelopes, profiles, dispatch metadata, and service facade state.
- `qsbridge/client`: adapter-facing client and metadata rowsets such as
  statement bundles, catalog discovery, prepared metadata, route decisions,
  warnings, diagnostics, and response previews. This layer may depend on core,
  catalog, planning, execution, protocol, and cache boundaries.
- `qsbridge/protocol`: protocol profiles, result schemas, protocol errors,
  statement response descriptors, and protocol capability negotiation. This
  layer may depend on core vocabulary but not client rowsets.
- `qsbridge/cache`: lock-sharded and plan-cache utilities that can be reused by
  catalog, dictionary, prepared-plan, and adapter metadata layers. Cache-key
  helpers may reference core vocabulary but should stay independent from client
  and protocol adapters.
- `qsbridge/testkit`: shared builders and fixtures once package splits would
  otherwise force repetitive cross-package tests.

The intended import direction is from adapter/client layers toward core
vocabulary, with cache utilities kept standalone. The split should happen when
it clarifies ownership rather than simply moving files for tidiness.

## Protocol Surfaces

Quanta should keep protocol responsibilities explicit as the SQL engine is
refactored. The engine core should be expressed as in-process Go interfaces
first. Network protocols adapt to those interfaces rather than owning planner or
executor semantics directly.

The intended split is:

- **MySQL protocol**: user-facing SQL compatibility, existing client drivers,
  prepared statements, batching, cancellation behavior, and wire-level result
  semantics.
- **gRPC control plane**: typed admin, catalog, inspection, planning, cluster,
  and eventually management APIs. gRPC should be the canonical structured
  network surface for operational tools.
- **Go interfaces**: the engine contracts underneath both MySQL and gRPC. These
  interfaces should remain testable without networking and should define the
  behavior that protocol adapters expose.
- **HTTP gateway**: optional convenience layer for dashboards, lightweight
  tooling, or browser-based clients. HTTP should adapt the gRPC/control-plane
  contracts rather than becoming a separate source of truth.

## Catalog Persistence

Quanta currently persists schema and metadata in Consul and keeps an in-memory
schema cache in the running processes. The refactor should preserve the useful
shape of that design while avoiding a hard engine dependency on Consul APIs.

The intended boundary is:

- **Catalog interface**: engine-facing contract used by binding, planning, and
  inspection.
- **Cached catalog**: process-local read-through cache, backed by lock-sharded
  maps, for table, field, relationship, function, role, and other stable catalog
  metadata.
- **Catalog store adapter**: persistence backend implementation. Consul can be
  the default adapter, but it should not leak into SQL planning code.

This leaves room to keep Consul where it works well while retaining an escape
hatch if HashiCorp licensing, operational requirements, or deployment strategy
make a different backend preferable. A future adapter could persist the same
catalog model in another KV store, database table, file, or embedded store
without changing the SQL engine contracts.

## StringEnum Dictionaries

StringEnum dictionaries are a Quanta-wide storage, ingest, catalog, and query
planning concern. They should not be treated as a `qlbridge`-only detail.

For batch-oriented loads such as TPC-H, low-cardinality string values are mostly
known during ingest. The dictionary can often be built early and treated as a
stable field-level mapping for planning and execution. Streaming use cases are
harder. Heterogeneous live feeds can introduce new values over time, which means
dictionary mutation, cache invalidation, replication, and query consistency must
be designed explicitly.

The current engine persists StringEnum labels through KVStore special handling
and keeps exact replica copies on every node on a per-table/per-field basis. It
also uses contention management such as Go `singleflight` around dictionary
population. That shape has worked well enough operationally, but it is closer to
a distributed cache than a clean metadata service and may contain race
conditions.

The refactor should model dictionaries behind explicit interfaces:

- **Dictionary resolver**: maps SQL string values to encoded StringEnum ids and
  maps ids back to labels for result projection.
- **Cached dictionary**: process-local cache with explicit field identity,
  version, and invalidation behavior.
- **Dictionary update mode**: distinguishes static batch-loaded dictionaries
  from append-only streaming dictionaries and adapter-owned mutation policies.
- **Dictionary consistency mode**: distinguishes immutable snapshots from
  versioned distributed dictionaries that require cache invalidation.
- **Dictionary store adapter**: persistence backend for dictionary labels and
  encoded ids. The adapter may use Consul, KVStore, another database, or an
  embedded store without leaking into the planner.
- **Ingest coordination**: mutation path for new streaming values, including
  contention handling, replication, and version publication.

Planner code should reason about StringEnum capabilities, dictionary versions,
and encoded ids. It should not own dictionary persistence, replication, or
contention semantics directly.

## String Storage And Predicate Capabilities

SQL-facing string type is not enough to decide whether a predicate is native.
The planner must classify string predicates by the field's physical storage and
index strategy.

The intended distinction is:

- **StringEnum** fields use a dictionary-backed encoded id space. Equality,
  membership, prefix `LIKE`, and eventually contains `LIKE` may be native only
  when the dictionary advertises the needed capability.
- **Backing string** fields are ordinary string payloads that may need residual
  expression evaluation unless a more specific string index capability is
  present.
- **StringLexBSI** should be treated as a legacy/equality-oriented strategy, not
  as evidence that prefix or contains predicates are native.
- **StringLexBSI** is the preferred future direction for replacing
  `StringLexBSI` if lexical ordering, range-like string predicates, or cleaner
  prefix planning become important.
- **Text/search indexes** should be modeled as a separate capability family.
  Full-text search, substring matching, and searchable-schema behavior should
  not be conflated with SQL `LIKE` or StringEnum dictionary matching.

This keeps optimizer decisions honest: `DataTypeString` describes values, while
`IndexKind`, dictionary metadata, and future storage-specific capability flags
describe what can run natively.

Native classification should consume encoding capabilities directly. If a bound
field's encoding profile advertises equality, membership, range, prefix, or
contains support, classification and explain output can report that predicate
shape as natively understood. This is still one layer above executor selection:
an encoding-derived range capability does not by itself choose a bitmap, BSI,
dictionary, or residual primitive. It only records that the catalog/storage
contract says the predicate shape is available for native planning.

Physical planning can then translate plan capabilities into executor-facing
strategy families such as bitmap pushdown, BSI pushdown, residual scan, or
encoding-derived equality/range/prefix/contains. Strategy metadata is
explainable scaffolding; it is not an executor implementation and should not
reach into runtime packages.

Semi/anti membership is represented as a first-class logical and physical plan
node rather than being hidden inside generic predicates. That keeps `IN`,
`EXISTS`, `NOT IN`, and `NOT EXISTS` visible to classification, optimizer audit
records, explain output, and eventual executor lowering.
When membership is backed by relationship storage, diagnostics and strategy
metadata should preserve the relationship-specific capability as planning
evidence instead of collapsing it to a generic membership label.

## Relationship Encodings

Logical relationships and physical relationship storage should remain separate
concepts. A catalog relationship describes which tables and fields are related,
the join direction, and cardinality. A relationship encoding profile describes
how Quanta can use stored relation state to traverse or reduce found sets.

Legacy `ParentRelation` fields are currently the bridge into this model. They
represent relation vectors used for child-to-parent lookup, parent-to-child
expansion, join reduction, semi-join membership, and anti-join exclusion. In the
refactored engine, that physical behavior should live behind relationship
encoding capabilities rather than being treated as an ordinary SQL field type.

This distinction leaves room for multiple physical implementations, including
vector, bitmap, or BSI-backed relation storage. It also keeps outer-join behavior
explicit: preserving unmatched rows is a null-extension capability, while
anti-joins and `NOT EXISTS` can often be expressed as bitmap difference.

Binding should preserve relationship encoding metadata when it turns catalog
relationships into query-local join or membership edges. Classification can then
report relationship parent lookup, child expansion, join reduction, and
anti-join difference as native planning evidence. Physical planning can expose
matching strategy metadata on join nodes without committing to a concrete
executor yet. Semi-joins and anti-joins should use the same relationship
capability vocabulary because `IN`, `EXISTS`, `NOT IN`, and `NOT EXISTS` are
often the SQL surface for relation-backed bitmap set operations.

## Boundaries

`qsbridge` owns SQL semantics, planning vocabulary, and planner diagnostics.
Runtime packages own execution, storage adapters, and protocol boundaries. The
package should describe QuantaStream's native model first, then adapt parser
and runtime details at package boundaries.

New `qsbridge` code should use standard Go APIs. If logging becomes necessary,
use `log/slog`; do not introduce new `gou` usage in this package.

## Code Style

All exported types, constants, and functions need documentation comments. Add
short comments around counter-intuitive planner behavior, especially when a rule
preserves current Quanta semantics or records a known limitation.

Tests should assert typed behavior, stable diagnostic codes, and plan shape. They
should not depend on log output or legacy error string formatting.
