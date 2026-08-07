# qsbridge Design Decisions

## StringEnum Dictionary Persistence And Propagation

Status: current near-term decision.

StringEnum dictionaries map labels to numeric ids so low-cardinality strings can
be represented as bitmap-friendly values and rehydrated for projection,
grouping, ordering, and diagnostics. The mapping has to remain stable across
query proxies and data nodes, but the workload has two very different shapes:
batch loads such as TPC-H discover most labels early, while streaming ingest
can introduce new labels after the cluster has been running for a long time.

For now, StringEnum dictionary data remains in KVStore rather than moving into
Consul. Consul is still the right fit for catalog-style metadata that changes
infrequently and benefits from broad cluster/locality semantics, but it is not
the right hot path for mutable per-field dictionaries that may grow during
ingest. The current runtime keeps node-local Pogreb copies through
dictionary-specific propagation, while the query proxy keeps a process-local
dictionary cache. qsbridge records that shape explicitly with a KVStore-backed,
runtime-owned, distributed dictionary profile that requires process cache,
versioned invalidation, and node-local copies.

The architecture must treat multiple query proxies sharing the same underlying
data-node set as a first-class deployment topology. Dictionary consistency
therefore cannot depend on a single proxy-local cache, a single writer process,
or process-local coordination alone; every proxy must observe a compatible
label/id mapping for the same table and field while targeting the same node
set.

On a query-proxy cache miss, the runtime may create a missing label and fan the
new key out to all nodes through the dictionary propagation path. The existing
singleflight usage is a mitigation for concurrent proxies discovering the same
label at the same time; it helps avoid duplicate ids for one label, but it
should not be treated as the final consistency model. Dictionary versions,
cache invalidation, and failure behavior need to stay visible in the planning
and metadata boundary.

We are not moving the dictionary into Consul in this slice, and qsbridge should
not import KVStore, Pogreb, or propagation APIs. It only names the contract so a
future resolver can be implemented behind a clean interface.

A deterministic value-derived id strategy may eventually eliminate the
sequential-id race entirely. That design still needs collision handling,
reverse lookup, prefix/LIKE planning semantics, migration from existing ids,
and operational tooling for dictionary inspection. Until those questions are
settled, deterministic ids are a future option rather than the current
implementation plan.

Implications:

- StringEnum dictionary metadata remains runtime-owned, not catalog-owned.
- Query planning should treat dictionary version and resolver capability as
  explicit inputs.
- Node-local dictionary copies are part of the supported runtime shape.
- Process-local caches are required, but must not hide dictionary version
  changes.
- Prefix and LIKE support must continue to reason through dictionary semantics,
  not raw string scans alone.

## Projector Kernel And Foundset Transfer Strategy

Status: current refactor direction.

The legacy `core.Projector` is a reference for behavior, not a long-lived
adapter target. It contains important semantics for foundset-driven projection,
relationship-vector lookup, late materialization, string rehydration,
aggregation, and ranking, but those concerns conceptually belong in the
qsbridge executor/kernel layer rather than in `core`. qsbridge should extract
the semantic primitives and deprecate `core.Projector` over time instead of
building bridge code that makes the new engine depend on it.

The legacy Quanta intermediate layer also needs to evolve. A query fragment can
start from a foundset, but projection and join execution often need several
follow-up artifacts: relationship/vector BSIs, BSI-backed projection fields,
bitmap fields, and string or dictionary rehydration data. Passing the same
foundset repeatedly to retrieve each artifact is wasteful, especially across a
network boundary. The future IL/runtime contract should allow a caller to
request a bundle of required data for one foundset and shard window when that is
the best physical plan.

The current Q15 supplier-revenue kernel exposes the concrete near-term
workaround. The planner can first evaluate the selective `lineitem.l_shipdate`
predicate and receive a `lineitem` foundset. Relationship-vector reduction then
needs a follow-up artifact request for `lineitem.l_suppkey` constrained by that
foundset, without treating the original time predicate as a second logical
filter on the relationship vector. That request shape is legal and should become
first-class IL vocabulary: "given this table foundset, return these physical
vectors/materialization artifacts." The inabox-direct path may issue this as a
follow-up request for now, but the longer-term IL should make it explicit rather
than relying on projector-specific behavior or repeated ad hoc projection calls.

This is not only a Q15 concern. Many useful query shapes start with a selective
predicate on a time-sharded table such as `lineitem` or `orders`, then need
relationship vectors, BSI projection fields, grouping keys, or residual
materialization from the same candidate set. If the executor loses that foundset
between stages, a query may still return correct results while quietly falling
back to a full scan of a time-sharded table. The optimizer should recognize this
as a reusable "foundset-follow-up" pattern: once a shard/time predicate has
produced candidate rownums, subsequent physical artifact reads should be
constrained by those rownums whenever that preserves SQL semantics.

Explain and inspection output should eventually make this visible. A plan that
uses the pattern should show the first predicate/artifact that produced the
foundset, the follow-up vector or materialization requests that reused it, and
whether any step fell back to a full artifact scan. That will make slow but
correct TPC-H cases diagnosable without rediscovering the same boundary by
profiling every query shape.

Bundling is not always safe or optimal. Large foundsets plus many requested
fields can create oversized responses and may exceed gRPC or future transport
message limits. A planner/optimizer must therefore choose among several
physical strategies:

- Pass a very small foundset and retrieve only the specific follow-up data
  needed.
- Retrieve a larger bundle of relationship vectors and projection data in one
  shot when the foundset, field count, and transport budget make that cheaper
  than repeated calls.
- Push shard-range-aware filtering to the node when the predicate aligns with
  the table's physical time/shard boundary, allowing the node to read a single
  shard window and return a narrower artifact set.

That last option is an intelligent storage-native filter, not a general move
toward pushing arbitrary SQL evaluation into storage nodes. The rule is: SQL
semantics, relational algebra, residual predicates, and bitmap operation
planning stay in qsbridge; data nodes may still perform physical pruning and
artifact selection that are native to how data is stored. Shard-window filtering
is one of those allowed optimizations because the node already owns shard
boundaries, date-partitioned storage, and the local artifact reads needed to
answer that narrowed request.

This distinction matters. A node-side shard-window request should not evaluate
user-visible SQL in a new way, reorder boolean semantics, or decide join shape.
It should only answer a physically narrower request that the planner already
proved equivalent to the requested shard range. This also keeps future
proxy-side shard caches viable: if the optimizer knows a date predicate maps
exactly to shard ranges, it can decide whether cached shard fragments are usable
or whether the node should narrow the response.

## Sibling Relationship Graph Expansion

Status: near-term design boundary.

Some TPC-H shapes cannot be represented by a single child sink rowset after
relationship-vector reduction. Q9 is the concrete example: `part` has sibling
children `lineitem` and `partsupp`, and the formal profit expression only
becomes a valid row stream after expanding those siblings under the same parent
and applying the residual equality `ps_suppkey = l_suppkey`.

The current inabox-direct graph executor intentionally requires one sink table.
That is correct for chain and converged-graph shapes such as
`region -> nation -> supplier -> lineitem`, where every participating role can
be aligned one-to-one from the chosen sink row. It is not sufficient for sibling
children because one parent row may produce multiple lineitem rows and multiple
partsupp rows. The executor must represent the expanded logical row as a tuple
of role rownums, not as one table's rownum plus ancestor lookups.

The future native executor should model this as a relationship graph rowset:

- each candidate row carries rownums by role, such as `p`, `l`, and `ps`
- relationship-vector edges expand or constrain role rownums
- residual join predicates such as `ps.ps_suppkey = l.l_suppkey` filter the
  expanded rows after both sides are materialized or vector-derived
- aggregate inputs and grouping expressions read from role-qualified rowsets
  rather than assuming the sink table owns the row identity

This is not compatibility glue. It is relational algebra over bitmap-backed
relationship vectors, and it belongs in the native execution/kernel layer. The
legacy single-sink blocker should remain explicit until the tuple-rowset shape
exists, rather than silently choosing one sibling as the sink and risking wrong
multiplicity.

Implications:

- `core.Projector` should be deprecated by extraction, not wrapped.
- Projector kernel primitives belong in the qsbridge execution boundary.
- Runtime adapters should support bundled materialization requests when
  transport limits and foundset cardinality allow it.
- Nodes may apply planner-approved storage-native pruning such as shard-window
  filtering without becoming SQL execution engines.
- Explain/inspection output should eventually expose the selected foundset
  transfer strategy and whether shard-range-aware node filtering was used.
- Transport message limits are part of physical planning metadata, not an
  afterthought inside legacy compatibility code.

## Legacy Compatibility And Core/Shared Sunset Boundary

Status: active guardrail.

`qscompat` is a quarantine, not a second execution engine. Any wrapper code in
`qscompat` that starts to understand join orchestration, projector semantics,
relationship-vector reduction, materialization policy, aggregation semantics, or
optimizer choices is a red flag. Those concepts belong in qsbridge planner,
optimizer, and executor/kernel vocabulary. Compatibility adapters may translate
new contracts to temporary legacy calls, but they must not become the place
where SQL meaning is repaired or extended.

The current Quanta intermediate layer is the temporary exception. Some IL
connective tissue still has to cross core/shared/runtime boundaries while the
legacy `qlbridge`-based query proxy exists. That exception should be narrow and
have an explicit sunset plan: once the native qsbridge execution path covers the
required TPC-H and SQLRunner surfaces, the old proxy-facing IL adapters
should be retired or rewritten as direct native runtime contracts.

After that transition, the code that remains in `core` and `shared` should be
the pieces that are not SQL-proxy execution semantics: client-side IL connective
tissue where still needed, node-local execution/storage code, clustering and
placement support, catalog/admin APIs, and operational infrastructure. Join
planning, foundset reduction policy, projector kernels, result assembly, and
optimizer decisions should not live there.

Implications:

- Treat qscompat code that touches join/projector behavior as transitional debt
  unless it only translates already-planned qsbridge contracts.
- Keep new SQL semantics out of core/shared even when legacy code already has
  a nearby helper.
- Track IL compatibility as a short-lived bridge to deprecating the legacy
  `qlbridge` proxy path, not as a durable architecture layer.
- Move semantics into qsbridge first, then let runtime adapters become thin and
  replaceable.

Retiring the old proxy does not mean removing the proxy role from Quanta.
The proxy remains the query coordination tier for SQL parsing, planning,
optimization, cache coordination, result assembly, protocol adapters, and
management surfaces. The target is to replace the legacy qlbridge-based proxy
implementation with the qsbridge-based proxy/runtime path.

## Runtime Sessions And Node Catalog Metadata

Status: active guardrail.

`core/session.go` and `core/session_pool.go` are not just legacy SQL proxy
artifacts. They are also used by batch load and streaming ingest paths, so they
should be refactored as runtime/session infrastructure rather than deleted as
part of killing the old proxy. The long-term owner may not be `core`, but
the capability itself survives because ingestion still needs session lifecycle,
table access, and metadata-aware write coordination.

The table and catalog-shaped code in `shared` also needs a careful migration.
Data nodes legitimately need catalog and encoding metadata: they own local
storage artifacts, relationship vectors, BSI layouts, StringEnum/StringLex
state, shard windows, and physical index reads. That node visibility should
come from the same Consul-backed metadata source as other components, ideally
through the new catalog vocabulary or a node-side adapter that can translate it
without importing SQL planning semantics.

The distinction is:

- qsbridge owns SQL semantics, planning, optimizer policy, and executor kernel
  contracts.
- Nodes own local storage execution and need enough catalog metadata to
  interpret physical artifacts.
- Runtime/session infrastructure supports ingestion and operational execution
  paths, not just SQL query proxy behavior.
- core/shared should shrink toward runtime, node, clustering, admin, and
  connective infrastructure, not disappear wholesale.

## Protocol Bindings And gRPC Artifacts

Status: active guardrail.

Existing gRPC/protobuf artifacts should be reviewed as protocol bindings over
stable qsbridge contracts, not as the source of those contracts. Protobuf
schemas, MySQL wire packets, direct QIAB calls, and future management APIs
should all serialize or invoke the same underlying planning, execution, result,
diagnostic, and metadata vocabulary.

The target shape is:

- qsbridge/protocol owns protocol-neutral request/response vocabulary,
  capability metadata, errors, handoff descriptors, and result descriptors.
- gRPC/protobuf code lives at a transport adapter edge that mirrors qsbridge
  contracts.
- MySQL wire support lives at another adapter edge over the same contracts.
- QIAB/direct mode can bypass transport entirely and call the same contracts in
  process.

Generated protobuf code should not live in the main qsbridge package or shape
the core engine. As old proxy code is retired, each gRPC artifact should
either be deleted as proxy-only scaffolding or migrated into an adapter package
that serializes qsbridge-native contracts.
