# TPC-H Roadmap

TPC-H is the near-term analytical validation target for QuantaStream. The goal is to
use it as a correctness and capability roadmap before treating it as a formal
performance benchmark.

Formal performance comparisons belong in the Benchmark Lab methodology described
in [`BENCHMARK_LAB.md`](BENCHMARK_LAB.md).

## Current Checkpoint

As of 2026-07-19, the SF 0.01 `tpch_queries.yaml` roadmap suite is green in
both `inabox-direct` and `inabox-standard`.

The latest `inabox-standard` run through the MySQL protocol produced:

- 83/83 passing cases
- 33.790 seconds suite elapsed
- about 11.52 seconds mount/startup elapsed

These timings are useful readiness and profiling signals, not formal benchmark
claims. Fine-grained performance attribution should use dedicated profiling
runs, runtime probes, and the Benchmark Lab reporting path.

## Existing Schema Model

The repository already contains TPC-H table schemas under
`tpc-h-benchmark/config`. General schema design guidance is documented in
[`SCHEMA_DESIGN.md`](SCHEMA_DESIGN.md); this section explains the TPC-H-specific
choices.

These schemas intentionally use TPC-H integer keys as Quanta column IDs:

- `customer.c_custkey`
- `orders.o_orderkey`
- `part.p_partkey`
- `supplier.s_suppkey`
- `nation.n_nationkey`
- `region.r_regionkey`

Those fields are marked with `columnID: true`. Relationship fields then use
`ParentRelation` and point directly to the parent table:

- `orders.o_custkey -> customer`
- `lineitem.l_orderkey -> orders`
- `lineitem.l_partkey -> part`
- `lineitem.l_suppkey -> supplier`
- `partsupp.ps_partkey -> part`
- `partsupp.ps_suppkey -> supplier`
- `customer.c_nationkey -> nation`
- `supplier.s_nationkey -> nation`
- `nation.n_regionkey -> region`

This avoids the extra primary-key lookup path required by string-key schemas.
For TPC-H, foreign-key values are already the parent row identifiers Quanta
needs for efficient relationship traversal.

Selected parent-to-child expansion paths also opt into relationship artifacts so
the optimizer can avoid repeatedly rebuilding broad child-domain candidates.
This includes the fact-table edges from `lineitem`, the customer-to-orders edge,
the nation-to-customer/supplier edges, and the part/supplier-to-partsupp edges.
The tiny `region -> nation` dimension hop is intentionally left on the regular
relationship-vector path until probes show it matters.

## Existing Load Artifacts

The older benchmark directory includes:

- `tpc-h-benchmark/create-tpch.sh`
- `tpc-h-benchmark/drop-tpch.sh`
- `tpc-h-benchmark/tpch.sh`
- `tpc-h-benchmark/tpc-h-kinesis-producer.go`

These files should be treated as recoverable prior work. The next step is to
validate the load path against the current local cluster and SQLRunner roadmap
format, not to redesign the schemas.

## Validation Strategy

TPC-H roadmap suites live under `tpc-h-benchmark/sqltests`. Start with the
smoke suite before formal query coverage:

- table existence and count checks
- static dimension checks for `region` and `nation`
- relationship counts across `customer -> orders -> lineitem`
- bridge checks for `part/supplier -> partsupp`
- basic multi-table projections across the core TPC-H relationships

The smoke suite is `tpc-h-benchmark/sqltests/tpch_smoke.yaml`.

Once the smoke suite is passing, use
`tpc-h-benchmark/sqltests/tpch_queries.yaml` for formal TPC-H query goals.
Broad SQL syntax gaps that block formal TPC-H queries should be documented in
[`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md) unless they are near-term executable
roadmap targets.
The first useful targets are:

- Q1: single-table grouping and multiple aggregates over `lineitem`
- Q3: `customer -> orders -> lineitem` join with filters, grouping, ordering,
  and limit
- Q5: multi-table join with grouping through `region`, `nation`, `customer`,
  `orders`, `lineitem`, and `supplier`

Q3 currently has a supported formal roadmap shape. Q5 has supported staged
relationship probes, the same-nation six-table grouped count shape, the
six-table regional count shape, simple regional revenue, revenue ordering, and
formal discounted revenue using `l_extendedprice * (1 - l_discount)`. The
same-nation and six-table revenue cases use longer SQLRunner timeouts because
they remain performance-sensitive.

As TPC-H work uncovers general SQL behavior, add or strengthen corresponding
coverage in the broad suites under `sqlrunner/sqltests`. The benchmark suite
should remain the TPC-H narrative; the broad suites should preserve reusable SQL
engine contracts.

For inabox-direct refactor work, keep the broad TPC-H kernel suite as a cross-query signal and place heavier query-family probes in focused suites such as `sqlrunner/sqltests/inabox_direct_tpch_q9.yaml`.

## Query Coverage Matrix

This matrix tracks the current Q1-Q22 picture. `Supported` means an executable
suite case exists for a staged or formal-compatible shape. `Probed` means the
query was investigated and intentionally kept out of the executable suite.
`Blocked` means the formal query is primarily waiting on known unsupported SQL
syntax or planner capability.

| Query | Status | Current Notes |
| --- | --- | --- |
| Q1 | Supported | Inabox-direct covers the single-table `lineitem` grouped count shape with StringEnum group materialization. |
| Q2 | Supported | Inabox-direct covers Europe/BRASS count, mixed-table graph projection, and fixed supply-cost filtering over the region/nation/supplier/partsupp graph; formal query still needs correlated scalar minimum supply-cost filtering. |
| Q3 | Supported | Staged/formal-compatible customer-orders-lineitem revenue, date filters, grouping, ordering, and limit are covered. |
| Q4 | Supported | Inabox-direct covers staged order-date priority grouping; formal correlated `EXISTS` remains blocked. |
| Q5 | Supported | Regional revenue path is covered through staged probes and formal discounted revenue. |
| Q6 | Supported | Single-table discounted revenue is covered in inabox-direct with time-range, numeric-range, and arithmetic aggregate predicates. |
| Q7 | Supported | Staged single-role nation filters, one-role supplier/customer join paths, repeated `nation` aliases, France/Germany shipdate-window count, and one-direction revenue/year grouping are covered; formal query's bidirectional nation-pair OR is tracked as a grouped-boolean relationship-graph blocker. |
| Q8 | Supported | Staged part-type filtering, America customer/order/lineitem count, repeated customer/supplier `nation` alias count, market-share aggregate inputs grouped by year, and final aggregate-ratio projection are covered. |
| Q9 | Supported | Focused inabox-direct coverage includes `p_name LIKE '%green%'`, line/order graph counts, revenue arithmetic, supplier/nation grouped staging, sibling `lineitem`/`partsupp` profit tuples, and formal profit grouped by nation/year. |
| Q10 | Supported | Formal returned-item revenue by customer is covered, including customer account balance, nation, address, phone, and comment projection. |
| Q11 | Supported | Staged German supplier value aggregation, ordering, alias-based and aggregate-expression fixed-threshold `HAVING` are covered; formal scalar subquery thresholds remain a parser/planner boundary. |
| Q12 | Supported | Staged shipmode, receipt-date, joined date-comparison grouping, joined priority conditional aggregates, and the combined formal kernel are covered; formal query still needs parser cleanup for `OR`/`AND` inside `CASE`. |
| Q13 | Supported | Base customer count, grouped customer-orders inner join counts, contains-style comment filtering, and left-outer join count are covered; formal query still needs projected null-extension and derived-table second-stage grouping. |
| Q14 | Supported | Conditional promo and total revenue components plus final aggregate-ratio projection are covered in inabox-direct. |
| Q15 | Supported | Supplier-key revenue grouping, supplier projection, and max-revenue supplier selection over the lineitem date window are covered; formal query still needs view/temp-table materialization. |
| Q16 | Supported | Staged part filters, part-partsupp filtered join count/projection/grouped count/grouped value/count-distinct, and supplier semi/anti subquery counts are covered; formal single-query shape is blocked by compound `WHERE` subquery parsing/planning. |
| Q17 | Supported | Staged populated Brand/Container kernels cover part count, part-lineitem count, and joined yearly extended-price sum; formal constants are empty in this data load and formal correlated scalar aggregate remains a parser/planner boundary. |
| Q18 | Supported | Direct child-FK and joined large-order quantity threshold grouping plus formal customer/order projection are covered. |
| Q19 | Supported | Formal mixed-table OR discounted revenue is covered in inabox-direct with grouped boolean lowering and constrained child-domain branch evaluation. |
| Q20 | Supported | Staged `forest%` part filtering, forest `part -> partsupp` join count, equivalent `IN (SELECT ...)` membership count, Canada supplier join, 1994 shipped-quantity grouping, and fixed-threshold shipped-quantity `HAVING` are covered; formal query remains blocked by scalar aggregate comparison, interval syntax, and compound subquery composition. |
| Q21 | Supported | Inabox-direct covers Saudi supplier, F-status order, late-receipt line counts, same-order other-supplier `EXISTS`, late-line plus sibling `EXISTS`, joined/grouped Saudi supplier wait with sibling `EXISTS`, and the formal sibling `EXISTS` plus `NOT EXISTS` semi/anti shape over repeated `lineitem` aliases. |
| Q22 | Supported | Inabox-direct covers seeded customer anti-membership and seeded phone-prefix function-OR count; formal scalar-threshold grouping remains blocked. |

The inabox-direct TPCH kernel suite is now represented across Q1-Q22. As of the
2026-07-04 checkpoint, `go run . -engine inabox-direct -suite_file
sqltests/inabox_direct_tpch_kernels.yaml` from `sqlrunner/` completes with 84
supported PASS cases and 1 intentional XFAIL case. The XFAIL set is the
current refactor punch list rather than a suite failure.

The first TPC-H performance optimization target was reducing repeated BSI
projection work during multi-table join projection. A targeted per-projector
parent projection cache now avoids re-reading stable parent-side fields across
`Projector.Next` batches while keeping child-table projection streaming.

TPC-H Q2 now has inabox-direct coverage for the Europe/BRASS five-table graph
count, mixed-table graph projection, and the fixed low-supply-cost count over
`part -> partsupp -> supplier -> nation -> region`. The graph predicate path
supports suffix `LIKE` over `p_type`, numeric part-size and supply-cost filters,
and region filtering. The projection path now aligns sink rows back to ancestor
roles, materializes visible and residual-only fields, and applies projected
residual predicates before producing visible output.

TPC-H Q1 and Q4 now have inabox-direct coverage for single-table grouped
count materialization. Q1 exercises full-window materialization on time-sharded
`lineitem` plus StringEnum group rehydration, while Q4 exercises the aligned
`orders` date-window priority grouping path. Count-only grouped aggregates must
bypass the global bitmap-count fast path so they can materialize group fields
before reducing rows.

TPC-H Q21 now has inabox-direct coverage for the Saudi supplier dimension join,
F-status order count, raw late-receipt same-table date comparison, same-order
other-supplier `EXISTS`, late-line plus sibling `EXISTS`, the grouped
Saudi/F/late supplier wait kernel with sibling `EXISTS`, and the formal sibling
`EXISTS` plus `NOT EXISTS` semi/anti shape. The late-receipt and sibling
membership counts are correct but still performance canaries.

TPC-H Q22 now has inabox-direct coverage for seeded customer anti-membership
and seeded phone-prefix function `OR` filtering.
`customer.c_custkey NOT IN (SELECT o_custkey FROM orders)` returns the expected
500 customers when paired with a full-domain seed, and the seeded
`substr(c_phone, 1, 2) = ... OR ...` count returns the expected 429 customers
through grouped residual boolean evaluation. The formal scalar average threshold
remains scalar-subquery work.

TPC-H Q6 is supported as a single-table `lineitem` query with date predicates,
numeric predicates, and discounted revenue arithmetic. It is also the first
explicit time-shard pruning checkpoint: `lineitem` is now configured with
`timeQuantumField: l_shipdate`, and the SQL planner derives query shard windows
only when a Date/DateTime BSI predicate targets the table's configured
`timeQuantumField`. With that layout, the SF 0.01 Q6 discounted revenue path
improved from roughly 15-18 seconds per query to about 1 second while preserving
the expected revenue result.

The pruning rule is intentionally conservative. Query-global `FromTime` /
`ToTime` windows are currently safe only for single-table plans because the
bitmap query representation does not yet carry independent time windows per
table or per fragment. Multi-table date predicates therefore do not use this
optimization yet, even when one participating table has an aligned
`timeQuantumField`. Per-table shard windows are a future planner/optimizer
milestone.

This also creates a useful future advisor signal. If workload telemetry sees
frequent selective Date/DateTime predicates on a field that is not the table's
`timeQuantumField`, Quanta can record a missed shard-pruning opportunity and
eventually suggest a different shard field, an alternate materialized layout, or
a secondary time-oriented access path. That recommendation should be driven by
observed predicate frequency, estimated selectivity, and elapsed cost rather
than by field type alone.

TPC-H Q2 has staged executable coverage for the Europe/BRASS filter kernel over
the `part -> partsupp -> supplier -> nation -> region` join graph. This covers
the `p_size = 15` numeric predicate, `p_type LIKE '%BRASS'` suffix matching over
`StringEnum`, region filtering by `EUROPE`, and a fixed `ps_supplycost < 300`
threshold. Formal Q2 still depends on the
correlated scalar minimum supply-cost comparison and formal output ordering.

TPC-H Q11 now has staged executable coverage for the Germany supplier filter,
`partsupp -> supplier -> nation` joins, grouped supplier value aggregation,
ordering, alias-based fixed-threshold `HAVING` such as `having part_value >
100000`, and direct aggregate-expression `HAVING` such as `having sum(...) >
100000`. The formal scalar aggregate subquery threshold remains parser/planner
refactor work because HAVING currently accepts aggregate alias/literal
comparisons, not subquery thresholds.

The inabox-direct Q11 kernel is also a useful grouped-aggregate performance
checkpoint. Phase probes showed that bounded heap top-N ordering was not the
active bottleneck: ordering was sub-millisecond while grouped aggregate
construction took several seconds. Precomputing aggregate input vectors once
per aggregate moved that grouped phase from seconds to milliseconds and moved
the Q11 kernel into the sub-second class. Keep this as an execution invariant:
grouped aggregate reducers should not repeatedly re-evaluate the same aggregate
input expression once per group.

TPC-H Q4 now has staged executable coverage for the `orders` date-window
priority grouping. Same-table lineitem date field comparisons are covered by
the Q12 work, but the formal Q4 correlated `EXISTS` predicate remains blocked
when embedded inside the compound `WHERE`. This is the same parser/planner
refactor boundary as Q16: subqueries need to survive as expression-tree nodes
instead of only the top-level `SqlWhere.Source` side channel.

TPC-H Q12 now has staged executable coverage for `l_shipmode IN
('MAIL', 'SHIP')`, the `l_receiptdate` range predicate, same-table lineitem date
field comparisons (`l_commitdate < l_receiptdate` and
`l_shipdate < l_commitdate`), unqualified grouped counts by ship mode, joined
`orders -> lineitem` priority conditional aggregates, and the combined formal
kernel using date comparisons plus priority conditional counts. The staged
conditional aggregate uses searched `CASE` with `IN`/`NOT IN`; parser cleanup is
still needed for the exact formal `OR`/`AND` spelling inside `CASE` predicates.
Those remaining parser/planner boundaries are tracked in
[`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md) rather than as noisy TPC-H XFAIL
cases.

The inabox-direct TPCH kernel suite now tracks the same boundary explicitly.
The Q12 shipmode `IN`, receipt-date range, and combined shipmode/date count now
match the proxy-path expected counts. The runtime adapter handles time-sharded
single-table predicates by adding a synthetic full shard-key window when the SQL
predicate does not constrain the shard key itself; non-shard time predicates
such as `l_receiptdate` remain ordinary BSI predicates inside that shard scan.
The suite also covers `orders -> lineitem` joined `count(*)` for the shipmode
and receipt-date predicates, returning 2,764. The joined grouped Q12 case now
applies the formal date-comparison residual predicates after relationship-vector
reduction, yielding `MAIL=150` and `SHIP=157`. The suite now also covers joined
priority conditional aggregation with `MAIL=547/813` and `SHIP=559/845` for
high/low priority line counts, plus the combined formal kernel with date
comparisons and priority conditional counts yielding `MAIL=64/86` and
`SHIP=61/96`. The remaining Q12 promotion gap is parser cleanup for the formal
`OR`/`AND` spelling inside `CASE` predicates.

TPC-H Q3 and Q5 are represented in the inabox-direct kernel suite as small
frontier probes. Q3 currently supports the `customer` market-segment filter and
the first `customer -> orders` projection after adding synthetic full-shard
windows for unfiltered time-sharded child tables. The next Q3 hop,
`orders -> lineitem` with date predicates on both time-sharded tables, is also
supported with a count of `1464`. The obsolete qlbridge proxy result was
`32289` for this shape, but that value is the child-side `lineitem` date
predicate cardinality, not the FK-reduced SQL intersection after applying the
parent `orders` predicate. Q5 has green dimension coverage for the `ASIA`
region filter and `region -> nation` relationship-vector join. The
inabox-direct suite also now tracks the next one-edge Q5 fanouts,
`nation -> supplier` and `nation -> customer`, with row counts of 27 and 309
respectively. Two-edge
dimension chains are supported for `count(*)` as well:
`region -> nation -> supplier` returns 27 and
`region -> nation -> customer` returns 309 for the `ASIA` probe. The
three-edge `region -> nation -> supplier -> lineitem` count chain is also
covered and returns 16464, proving dimension filtering can flow into the
`lineitem` fact table through relationship-vector reduction. The customer-side
linear chain is covered too: `region -> nation -> customer -> orders` returns
2959 and `region -> nation -> customer -> orders -> lineitem` returns 11708. A
direct `supplier -> lineitem` fact-side count is also supported, returning 2447
for the single-nation supplier probe. The inabox-direct suite now has a first
relationship-graph reduction kernel as well: the Q5 supplier existence edge can
converge on the customer/orders/lineitem chain and returns 11708. A follow-up
graph projection kernel materializes the sink-table `lineitem` revenue inputs
needed by Q5 revenue aggregation, and an ungrouped graph aggregate now computes
total discounted revenue as `3.97022970406798e+08`. The next graph aggregate
kernel reconstructs ancestor rownums from the `lineitem` sink through
relationship vectors and supports grouped revenue by `n_name`; the simplified
regional graph produces five ASIA nation groups ordered by discounted revenue.
The next kernel applies the same-nation supplier/customer residual equality
(`s.s_nationkey = c.c_nationkey`) over the converged graph and returns five
same-nation ASIA revenue groups. The formal-shaped Q5 kernel now also folds in
the `orders.o_orderdate` 1994 window, reducing the converged `lineitem` sink to
1503 rows and the same-nation aggregate input to 90 rows before grouping. The
simple parser also supports the more canonical Q5 shape where the same-nation
predicate is an additional residual `JOIN ON` conjunct on the `supplier` edge;
that variant returns the same five ordered ASIA revenue groups.
The `supplier -> lineitem` hop currently scans the full time-sharded `lineitem`
child side when there is no local child predicate, so fact-side Q5 chains are
tracked as a performance frontier rather than a correctness blocker.

TPC-H-shaped SQL function coverage now includes scalar projection and residual
grouping for `year(date_field)` and `substr(string_field, start, length)`.
These probes keep function evaluation honest in both select-list projection and
group-key evaluation without promoting the repeated-alias-heavy formal Q7/Q8/Q9
queries prematurely.

Conditional expression coverage note: searched `CASE WHEN ... THEN ... ELSE ... END` is supported in the aggregate expression shape needed by formal Q14, e.g. `sum(case when p_type like 'PROMO%' then ... else 0 end)`. This should not yet be treated as complete CASE support: projection, WHERE, GROUP BY, ORDER BY, nested CASE, simple CASE, and string-valued CASE outputs need separate coverage. MySQL-style `IF(condition, true_expr, false_expr)` is not currently supported in aggregate expressions; a direct probe inside `sum(if(...))` fails during parse/planning. Prefer standard SQL `CASE` for roadmap coverage until `IF` is explicitly added as a compatible expression function.

String pattern coverage note: StringEnum fields now support SQL-style `%` and `_` `LIKE` / `NOT LIKE` through enum-label matching plus bitmap batch predicates. This covers Q14-style `p_type LIKE 'PROMO%'`, Q16-style prefix exclusion such as `p_type NOT LIKE 'MEDIUM POLISHED%'`, and Q2-style suffix probes such as `p_type LIKE '%BRASS'`. These predicates are resolved by scanning the compact enum dictionary, not by hydrating every row.

Backing-store string fields have a different boundary. Exact equality over
`p_name` works for both count and projection, and `substr(p_name, ...)`
grouping works. Residual string-function predicates such as
`substr(p_name, 1, 5) = 'green'` or `substr(p_name, 11, 8) = 'lavender'` now
drive both `count(*)` and projected result sets, with final `LIMIT` / `OFFSET`
enforced after residual filtering. Normal SQL `LIKE` over backing-store strings
now routes through residual WHERE evaluation over hydrated values, so
`p_name LIKE 'green%'` and `p_name LIKE '%green%'` return the expected prefix
and contains matches. The older arbitrary text-search experiment remains a
separate capability to clean up and document rather than the default SQL LIKE
path.

Function coverage note: single-table computed grouping now supports expressions such as `year(date_field)` and `substr(string_field, ...)`, and joined projection can evaluate parent-side computed fields. Multi-table grouped joins now cover several count and aggregate-expression shapes, including two-table grouped count/value coverage in Q16. Q8/Q9-style year grouping and market-share work still need separate repeated-alias and profit-expression validation before formal promotion.

TPC-H Q7/Q8 repeated-nation alias probing now preserves independent query roles
for repeated uses of `nation`. The inabox-direct kernel suite promotes the Q7
France/Germany count without the shipdate window, proving supplier nation and
customer nation filters no longer collapse into one physical `nation` foundset.
The formal-shaped Q7/Q8 probes exposed a narrower time-sharded lineitem
relationship-vector projection blocker: after a date window is applied, the
executor must retrieve lineitem vectors and projected values for the filtered
child set without losing rows from non-shard date predicates. The current
inabox-direct adapter handles this with an explicit planner-visible projection
scope and a shard-key rule: when a relationship-vector FK read already has a
child foundset, that foundset is the row constraint and the adapter may broaden
the physical time-shard window to retrieve the vector BSI; ordinary
materialization may only narrow physical shard scope when the SQL date
predicate targets the table's actual shard-key time field. This promoted the Q7
France/Germany shipdate-window count. Q8's formal-shaped count has an empty
lineitem result on the current SF0.01 data, so graph reduction now
short-circuits empty child or parent candidate sets instead of projecting a
relationship vector over an impossible join. These are intentionally temporary
compatibility behaviors; remove or promote them out of the adapter layer once
the new TPCH path owns relationship-vector execution directly.

TPC-H Q13 now has staged inabox-direct coverage for base `customer` counts,
grouped `customer -> orders` inner join counts by customer, and contains-style
comment filtering with `o_comment LIKE '%special%requests%'`. The formal query
still depends on `LEFT OUTER JOIN` null-extension and derived-table second-stage
grouping. The parser now preserves `LEFT OUTER JOIN`, but inabox-direct
relationship-vector execution is intentionally inner-join-only in this slice, so
the left-outer count remains an explicit kernel XFAIL.

TPC-H Q14 has inabox-direct coverage for the formal promo-revenue ratio:
the conditional promo numerator, total discounted-revenue denominator, and final
scalar aggregate-ratio projection over `lineitem -> part`. This builds on the
SQL `LIKE` kernel for left-anchored prefix matching over a `StringEnum` field
and the global aggregate projection path that evaluates scalar arithmetic over
hidden aggregate inputs.

TPC-H Q15 now has staged executable coverage for the lineitem revenue kernel:
the `l_shipdate` window, supplier-key residual grouping over numeric
`l_suppkey`, grouped `count(*)`, and grouped revenue arithmetic sum all execute
correctly. The inabox-direct suite now also covers supplier projection and
max-revenue supplier selection with `ORDER BY total_revenue DESC LIMIT 1`.
Formal Q15 remains blocked on expressing the supplier revenue view or temporary
materialization.

The inabox-direct Q15 grouped kernels now classify as sub-second, mostly
materialization/date-window work rather than grouped-reducer work. On the SF
0.01 data, the lineitem supplier revenue kernel has roughly 2,284 candidates,
100 supplier groups, a ~100ms materialization phase, and sub-millisecond
grouping/order phases after aggregate input precomputation. The joined supplier
projection variant remains sub-second as well. Relationship phase probes show
the joined variant currently spends tens of milliseconds acquiring parent and
child row sets, roughly 15ms reducing through the relationship vector, and most
of the remaining time materializing child and parent projection fields. If it
regresses, inspect relationship row acquisition and projection materialization
before revisiting grouped aggregate reduction.

Relationship materialization detail probes distinguish join-pair fanout from
actual materializer input. The Q15 supplier join sees 2,284 joined child rows,
but parent-side materialization collapses to 100 unique supplier rows before the
compatibility materializer call. That means the remaining work is field
materialization and compatibility materializer cost, not duplicate parent-row
fetches.

TPC-H Q16 now has staged executable coverage for the part-side Brand/Type/Size
filter kernel and the filtered `part -> partsupp` join count, projection,
grouped `count(*)`, grouped supplier-value sum by brand/type/size, and grouped
`count(distinct ps_suppkey)`. Relationship aggregate paths apply the residual
`p_type NOT LIKE 'MEDIUM POLISHED%'` predicate after vector reduction, so the
Q16 join-count and grouped distinct-supplier expectations reflect the fully
filtered rowset. It also covers supplier-key `IN` and `NOT IN`
subqueries over `partsupp -> supplier`, including child-side anti-join
reduction and the empty-RHS case used by the supplier comment predicate
(`s_comment LIKE '%Customer%Complaints%'`). For subquery `count(*)`, the
executor must count the reduced outer membership set rather than the joined
child row stream or the unreduced driver set; this is the distinction that keeps
duplicate child matches from over-counting `IN` and keeps child-side `NOT IN`
from returning the full outer table.

The inabox-direct relationship executor also short-circuits empty membership
right-hand sides: a semi-join with an empty RHS returns no outer rows, while an
anti-join with an empty RHS keeps the already reduced outer pairs without
materializing the left key column. This is intentionally small but important for
Q16-style complaint-supplier predicates where the supplier subquery can be
empty.

The inabox-direct Q16 grouped kernels are also sub-second after grouped
aggregate input precomputation. The part-side grouped count is dominated by
materialization plus residual `NOT LIKE` filtering, not grouping. The
`part -> partsupp` grouped `count(distinct)` kernels have small grouped phases
over roughly 1,228 joined candidates and 304 output groups; remaining elapsed
time belongs to relationship/membership assembly and materialization rather
than aggregate reduction. Relationship probes show the simple filtered join
count is mostly relationship reduction over 8,000 child rows, while the grouped
distinct kernels are dominated by parent-side materialization of the `part`
fields needed for grouping. The complaint-supplier anti-membership variant adds
membership filtering time before the same parent materialization cost. This
makes Q16 a useful regression target for relationship-vector materialization and
anti-membership changes, not for grouped reducer tuning.

The Q16 detail probes currently show about 1,228 joined child rows and 307
unique parent `part` rows for the grouped distinct kernels. Parent rownums are
already deduplicated before materialization, and post-reduction field pruning
now removes relationship keys that were only needed to build the joined pair
stream. Projection and aggregate relationship paths both treat ON-clause
relationship fields as execution fields rather than materialization fields
unless they are explicitly referenced downstream. For the grouped distinct
kernels this reduces child materialization from two fields to one and parent
materialization from four fields to three. Further improvement should focus on
materializer internals or shard-aware retrieval rather than another
row-deduplication pass.

Projection-only relationship joins with `LIMIT`/`OFFSET` and no `ORDER BY` can
push the row window into the joined pair stream before materialization. The Q16
filtered projection probe uses this to materialize only the first 10 joined
child rows and their parent rows instead of hydrating the full 1,228-row joined
candidate set. Ordered projections must continue to sort before limiting.

Formal single-query Q16 remains intentionally deferred at the parser/planner
boundary. The current qlbridge parser represents a top-level membership
subquery through `SqlWhere.Source`; it does not yet represent subqueries as
normal expression-tree nodes inside compound predicates. As a result,
standalone `ps_suppkey NOT IN (SELECT ...)` is supported, but embedding that
membership predicate inside the larger joined/grouped Q16 `WHERE` chain
currently fails parsing at the inner `SELECT`. Treat this as parser/planner
refactor work rather than a Q16 execution-kernel bug.

TPC-H Q17 has staged inabox-direct coverage for a populated Brand/Container
kernel using `Brand#45` / `MED JAR`: part filter count, `part -> lineitem`
join count, and joined extended-price yearly sum. The formal `Brand#23` /
`MED BOX` constants are empty in this data load, so exact formal-value staging
is not useful yet. The formal correlated scalar aggregate predicate
(`l_quantity < 0.2 * avg(...)`) is tracked as an explicit XFAIL because the
parser/binder currently treats the parenthesized subquery as a field-like
expression rather than preserving it as a scalar subquery node.

TPC-H Q20 has staged inabox-direct coverage for the `forest%` part filter,
the forest `part -> partsupp` relationship-vector join count, the
`supplier -> nation` Canada filter join, and the single-table lineitem
date-window grouping by `(l_partkey, l_suppkey)` with `sum(l_quantity)`,
including a fixed-threshold alias-based `HAVING` predicate. The forest part
filter uses an explicit full-domain `p_partkey >= 1` seed before applying the
backing-string residual `p_name LIKE 'forest%'`; pure residual-only table scans
remain outside this inabox-direct path. Exact `p_name` equality works for both
count and projection, and residual backing-store `LIKE` now covers prefix and
contains patterns such as `p_name LIKE 'green%'` and `p_name LIKE '%green%'`.
Residual predicates such as `substr(p_name, 1, 7) = 'frosted'` or
`substr(p_name, 1, 5) = 'green'` also drive both `count(*)` and projected rows.
Formal Q20 remains blocked by subquery membership parsing (`IN (SELECT ...)`),
scalar aggregate comparison, interval syntax, and compound subquery composition
rather than a lineitem grouping, string-filter, residual projection, or
supplier/nation join blocker.

TPC-H Q21 has staged executable coverage for the Saudi supplier filter,
late lineitem receipt-vs-commit predicate, F-status order count, same-order
other-supplier `EXISTS`, and the joined/grouped Saudi supplier wait kernel.
The late receipt count is correct but slow locally, so it remains a performance
target. Formal Q21 remains blocked on the filtered sibling `EXISTS` and
`NOT EXISTS` subqueries over repeated `lineitem` aliases.

TPC-H Q22 has staged executable coverage in `tpc-h-benchmark/sqltests/tpch_q22_profile.yaml` through the reusable customer kernels.
`substr(c_phone, 1, 2)` works in projection and grouping, and the formal phone
prefix set works when expanded as explicit `OR` predicates. Prefix grouping with
`c_acctbal > 0` and `sum(c_acctbal)` also works, and standalone
`customer.c_custkey NOT IN (SELECT o_custkey FROM orders)` returns the expected
anti-membership count. In inabox-direct, the seeded phone-prefix `OR` count now
runs as a bitmap seed plus grouped residual boolean predicate. Remaining
boundaries are `substr(...) IN (...)` misclassification, combined Q22-like
grouped aggregate composition over prefix filtering plus `NOT IN`, and the
formal scalar average threshold.

The current grouped-join execution boundary is intentionally narrow. Inner
joined grouped queries whose aggregate list is limited to `count(*)` and
`sum(...)` can route through the newer multi-table grouped path, which preserves
join multiplicity without late materializing full joined rows. Grouped joins
that require outer-join semantics or reducers such as `min`, `max`, or `avg`
fall back to the older weighted aggregate path. That fallback is required for
existing broad SQL coverage such as `join_group_by.021`, and the fast path
should only expand once those reducers are implemented explicitly.

TPC-H Q19 is supported as a formal `lineitem -> part` discounted revenue
query with mixed-table top-level `OR` branches. Profiling initially suggested
that repeated branch-local found-set construction was the dominant cost, but a
follow-up common-predicate factoring experiment showed that the repeated
`lineitem` predicates (`l_shipmode IN ('AIR', 'AIR REG')` and
`l_shipinstruct = 'DELIVER IN PERSON'`) were cheap to evaluate. The expensive
piece was the branch-specific `l_quantity >= ... AND l_quantity <= ...` numeric
BSI range.

The retained optimization is therefore same-field inclusive BSI range
coalescing in `SQLToQuanta`: predicates of the form `field >= low AND field <=
high` now plan as a single bitmap `RANGE` fragment instead of two independent
BSI comparisons. On the SF 0.01 Q19 profile, the formal OR count/revenue cases
improved from roughly 18-19 seconds to about 9-10 seconds after cleanup, while
preserving expected results. Deeper `RANGE` kernel performance remains a future
optimization target because the coalesced `l_quantity` predicates are still the
largest remaining local predicate cost.

TPC-H Q9 has focused inabox-direct coverage for the green-part filter over the
`part -> lineitem` join and for the `part -> lineitem -> orders` count shape.
This locks in backing-store `p_name LIKE '%green%'` inside joined count and
limited projection shapes. The inabox-direct runtime applies the backing-string
residual predicate after relationship-vector reduction, covering the
`part -> lineitem` count and the `part -> lineitem -> orders` graph count with
the expected 3,223 matching lineitem rows. Focused Q9 coverage now also includes
green-part lineitem revenue arithmetic, supplier/nation grouped count and revenue
staging, sibling `lineitem`/`partsupp` profit tuples under `part`, and the formal
profit grouped by nation and order year. The formal shape depends on the
multi-sink/bidirectional tuple-rowset executor: it expands `part -> lineitem` and
`part -> partsupp`, applies `ps_suppkey = l_suppkey`, and follows `lineitem ->
orders` plus `lineitem -> supplier -> nation` for grouping dimensions.

TPC-H Q10 has formal inabox-direct coverage for returned-item revenue by
customer using `customer -> orders -> lineitem -> nation`, `o_orderdate`
filtering, `l_returnflag = 'R'`, discounted revenue arithmetic, grouping,
ordering, and `LIMIT`. The promoted kernel verifies the full customer projection,
including `c_acctbal`, `n_name`, `c_address`, `c_phone`, and `c_comment`.

TPC-H Q7 now has staged executable coverage for France/Germany one-role
nation filters, the supplier-lineitem-orders date-window count, one-role
supplier/customer nation paths, repeated `nation` aliases without the shipdate
window, the formal-shaped France/Germany shipdate-window count, and the
one-direction France/Germany revenue grouped by supplier nation, customer
nation, and ship year. Formal Q7's bidirectional nation-pair `OR` is now tracked
as an explicit inabox-direct kernel XFAIL because grouped boolean execution must
be lowered across repeated table roles before relationship-graph reduction.

TPC-H Q8 now has staged executable coverage for the formal part type filter,
the America customer-orders-lineitem-part count without the supplier-nation
role, the repeated customer/supplier `nation` alias count, and the market-share
aggregate numerator/denominator inputs grouped by `year(o.o_orderdate)`. It
also covers the final market-share ratio projection by evaluating arithmetic
over hidden grouped aggregate slots. The current SF0.01 data returns zero rows
for the Brazil/part/order count shape, so this also guards the graph executor's
empty-set short-circuit.

TPC-H Q15 depends on view or temporary-table style materialization. Quanta's
current `SELECT INTO` path writes export files rather than session-scoped temp
tables, and named SQL views are not supported yet. Track that capability in
[`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md) until a view/temp-table catalog and
execution model exist.

TPC-H Q19 is covered by the inabox-direct executable suite, including standalone
brand/container/size probes, child-side discounted revenue, and the formal
mixed-table `OR` branch shape over `lineitem -> part`. Inabox-direct now tracks
the same formal branch query as an explicit XFAIL because mixed-table top-level
`OR` requires grouped boolean expression lowering before relationship-vector
reduction. It remains worth profiling because the formal branch evaluation is a
useful stress test for joined predicate pushdown and residual branch filtering.

A follow-up child-side projection pruning experiment for grouped joins was
tested against the SF 0.05 profile and regressed suite median time, so it is not
currently the preferred optimization path. The next near-term performance target
is bitmap-informed join driver selection and cost validation. Quanta can obtain
predicate-reduced counts, shard participation, and standard-bitmap value
distribution signals cheaply enough to make this a natural optimizer input.

The first optimizer milestone should be a diagnostic harness rather than a full
cost optimizer: enumerate plausible driver tables, filter out candidates that
are not legal for the current execution shape, estimate each remaining candidate
using bitmap-derived signals, and record estimated cost versus actual elapsed
time and projected row counts. This should be validated against TPC-H Q3/Q5, the
broader SQLRunner join suites, and synthetic non-TPC-H shapes so the optimizer
does not become benchmark-specific.

A forced-driver experiment showed that `orders` can be a smaller candidate set
than `lineitem` for TPC-H Q3/Q5, but forcing `orders` as the final driver causes
current `orders -> lineitem` count/projection paths to emit no aggregate row.
That establishes an important near-term legality rule: cost can only rank
drivers that the current join/projector path can materialize correctly. Parent-
side driving through child-side relationships requires a future alternate
execution path before it can be costed as a valid plan.

## Benchmark Checkpoints

The first repeatable local benchmark checkpoints compare the SF 0.01 fixture
against an SF 0.05 load using the focused Q3/Q5 profile suites. These numbers
were captured on the local QIAB development environment, not a tuned production
deployment.

Source logs:

- SF 0.01: `tpc-h-benchmark/local/logs/tpch_profile-5x-20260617-191146.log`
- SF 0.05: `tpc-h-benchmark/local/logs/tpch_profile_scale-3x-20260617-200529.log`

| Query | SF 0.01 Median | SF 0.05 Median | Ratio |
| --- | ---: | ---: | ---: |
| Q3 grouped revenue with order fields | 10s | 27s | 2.7x |
| Q5 combined graph regional count | 11s | 27s | 2.5x |
| Q5 same-nation combined graph count | 7s | 18s | 2.6x |
| Q5 simple regional revenue | 8s | 16s | 2.0x |
| Q5 regional revenue ordering | 8s | 16s | 2.0x |
| Q5 formal discounted revenue | 5s | 12s | 2.4x |

Suite-level elapsed median increased from 49s at SF 0.01 to 117s at SF 0.05,
about 2.4x for a 5x scale-factor increase on the profiled query set. This was
an encouraging early scaling signal, but the absolute query times pointed to
repeated projection reads as the first optimization target.

After adding the targeted parent projection cache, SF 0.05 profile medians
improved materially:

Source log:

- SF 0.05 post-cache: `tpc-h-benchmark/local/logs/tpch_profile_scale-3x-20260617-215712.log`

| Query | SF 0.05 Baseline | SF 0.05 Post-Cache | Change |
| --- | ---: | ---: | ---: |
| Q3 grouped revenue with order fields | 27s | 12s | -56% |
| Q5 combined graph regional count | 27s | 18s | -33% |
| Q5 same-nation combined graph count | 18s | 16s | -11% |
| Q5 simple regional revenue | 16s | 12s | -25% |
| Q5 regional revenue ordering | 16s | 12s | -25% |
| Q5 formal discounted revenue | 12s | 12s | 0% |

Suite-level elapsed median improved from 117s to 82s, about a 30% reduction.
The remaining hotspots are now expected to be join driver selection,
child-side projection reads where the large table is unavoidable, plus
opportunities for shard/time-range pruning and late materialization. A grouped
join projection-pruning experiment was correct but slower on SF 0.05, which
suggests pruning should be revisited only with better driver choice or a more
selective late-materialization strategy.

After changing `lineitem` to shard by `l_shipdate` and gating SQL-derived shard
windows on the explicit `timeQuantumField`, the SF 0.05 profile was rerun:

Source log:

- SF 0.05 post-time-pruning: `tpc-h-benchmark/local/logs/tpch_profile_scale-3x-20260621-004635.log`

| Query | SF 0.05 Post-Cache | SF 0.05 Post-Time-Pruning | Change |
| --- | ---: | ---: | ---: |
| Q6 discounted revenue | not in suite | 1s | new focused target |
| Q3 grouped revenue with order fields | 12s | 13s | +8% |
| Q5 combined graph regional count | 18s | 15s | -17% |
| Q5 same-nation combined graph count | 16s | 14s | -13% |
| Q5 simple regional revenue | 12s | 17s | +42% |
| Q5 regional revenue ordering | 12s | 16s | +33% |
| Q5 formal discounted revenue | 12s | 16s | +33% |

The Q6 result is the important cross-cutting proof point: SF 0.05 retains a
1-second discounted revenue path because the query touches only the
`l_shipdate` shard window instead of scanning the full `lineitem` history. Q12
does not benefit from this layout because it filters on `l_receiptdate`, not the
configured shard field. The Q5 revenue-path regressions need a separate look;
the profile now points back toward join driver legality, parent-side expansion,
and projection materialization rather than lineitem date pruning.

A key-grouping comparator experiment was run against SF 0.05 to test whether
grouping by numeric relationship keys instead of display labels would make
late label materialization an obvious next optimization. It did not show a
large win: Q3 key-only grouping was essentially tied with the existing Q3 shape
and Q5 key grouping was neutral to slightly slower. Those cases are retained in
`tpc-h-benchmark/sqltests/tpch_profile_experiments.yaml`, but they are kept out
of the default scale profile so routine benchmark runs remain focused.

Projector timing instrumentation then confirmed that the remaining projection
heat is concentrated in child-side `lineitem` BSI reads, especially relationship
fields such as `l_orderkey` and `l_suppkey`. Parent-side projection reads are
now mostly cache hits or sub-millisecond work. A grouped-join prefetch experiment
was tested and removed: even after deduplicating repeated projection field
requests, prefetching the grouped join projector regressed the SF 0.05 profile
from a 91s baseline run to 102s because it replaced repeated smaller child-side
reads with broader `lineitem` reads. Future work should focus on selective
child-side BSI reuse, shard/time pruning, or relationship-specific caching
rather than full grouped-query prefetch.

A grouped-join batch-size experiment was also tested at 10k and 20k rows per
`Projector.Next` batch. Larger batches reduced projection call count
substantially, but total elapsed time stayed effectively flat because each
child-side `lineitem` projection read became proportionally larger. The
experiment confirms that the current hotspot is data volume scanned in child
BSIs, not per-call overhead. The batch-size knob was removed rather than kept as
configuration surface.

A late-materialization experiment for residual-filtered grouped joins was also
tested against SF 0.05. The experiment correctly deferred driver-table aggregate
expression inputs such as `lineitem.l_extendedprice` and `lineitem.l_discount`
until after residual predicates reduced the candidate row set. In Q5 formal
revenue, it materialized roughly 1.9k surviving rows instead of the broader
9.5k driver set, but two 3-run profiles stayed around 85-86s median, slower than
the prior 78s median baseline. This confirmed that the remaining bottleneck was
relationship BSI traversal (`l_orderkey`, `l_suppkey`) rather than aggregate
value materialization. The experiment was removed; late materialization should
be revisited only with a lower-overhead projector API or stronger row-set
selectivity.

Two targeted projector-local cache optimizations followed and are now the
preferred 1.x boundary:

- relationship BSIs produced during multi-table found-set reduction are carried
  into join projection and reused inside `Projector.Next` batches, avoiding
  repeated projection of child relationship fields such as
  `lineitem.l_orderkey` and `lineitem.l_suppkey`
- numeric child-table payload BSIs are cached within one projector and retained
  to each batch found set, avoiding repeated projection of fields such as
  `lineitem.l_extendedprice` and `lineitem.l_discount`

These changes reduced the SF 0.05 focused profile from roughly 80s median after
the earlier parent projection cache, to 66s median after relationship BSI reuse,
and then to about 60s median after child payload BSI reuse. They are intentionally
local to one query/projector and do not attempt cross-session reuse.

Reusable query fragments that survive beyond one query should be handled by a
future proxy-managed fragment cache rather than by expanding projector-local
state. That cache belongs on the 2.0 roadmap because correctness requires a
coherent invalidation model across mutations, shard sync, schema changes,
cluster topology changes, and eventually multiple proxies. Cache fragments
should be tagged with table, shard, schema, and data-version metadata rather
than simple TTL alone.

## Time-Quantized Shard Planning

TPC-H date fields create many time-quantized shards even at small scale
factors. When a query has a predicate on a table's time quantum field, the
planner should use that predicate to restrict shard selection early.

Q3 is a key example. The formal TPC-H query includes date predicates on
`orders` and `lineitem`. Without those predicates, projecting date fields such
as `o_orderdate` can trigger broad `timeRangeBSI` scans before `LIMIT` can
reduce the result set. This points toward time-aware planning, bounded grouped
aggregation, and late materialization for projected date fields.

## Known Capability Gaps

TPC-H should drive the SQL implementation roadmap incrementally. Expected gaps
include:

- multiple aggregate expressions in one projection
- broader arithmetic-expression coverage inside aggregate inputs beyond the
  currently supported TPC-H revenue shapes
- broader optimizer support for pushing field-to-field residual predicates into
  bitmap/BSI reduction rather than late row filtering
- `GROUP BY` over joined tables
- `HAVING`
- `ORDER BY` on projected aliases and aggregate values
- `LIMIT`
- date arithmetic and interval syntax
- subqueries and `EXISTS`/`NOT EXISTS`
- decimal rendering and comparison consistency

The roadmap suites should retain unsupported formal queries as `xfail` cases.
When a case starts passing, SQLRunner reports `XPASS` so the case can be
reviewed and promoted intentionally.

TPC-H Q18 now has inabox-direct coverage for the large-order
quantity-threshold kernel via `orders -> lineitem`: grouping by `o_orderkey`,
applying alias-based `HAVING`, ordering by the aggregate, and limiting the
result. The formal customer/orders/lineitem projection is also covered with the
current SF0.01 data, returning the two orders above the `sum(l_quantity) > 300`
threshold. Directly grouping on the child FK relationship column
`lineitem.l_orderkey` still returns `NULL/NULL`, so that inabox-direct
relationship-column grouping shape is tracked as an explicit XFAIL.

TPC-H Q12 profiling is captured in `tpc-h-benchmark/sqltests/tpch_q12_profile.yaml`.
The current cost is not grouped output: grouping adds little over the corresponding
count. The original expensive path was repeated backend Date/DateTime BSI shard
assembly for non-shard date predicates such as `l_receiptdate >= '1994-01-01'`
and `l_receiptdate < '1995-01-01'` when `lineitem` is sharded by `l_shipdate`.
A request-local `timeRangeBSI` cache now reuses the assembled
`index/field/fromTime/toTime` BSI within one bitmap query, dropping the staged
receipt-date and formal lineitem filter paths from roughly 12-15 seconds to
about 6-7 seconds at SF 0.01.

Same-table date field comparisons (`l_commitdate < l_receiptdate` and
`l_shipdate < l_commitdate`) are recognized by the native same-row comparison
kernel and return rownums without SQL-row materialization. The current
development branch evaluates this through a Roaring BSI-to-BSI comparison
primitive when available, with residual-scan evaluation as the correctness
fallback. Q21 late-receipt and Q12 date-order probes remain useful performance
canaries because they expose candidate-set size, shard-window behavior, and
standard-vs-direct adapter overhead.

TPC-H Q21 is the roadmap canary for correlated sibling-domain semi/anti joins.
The supported `.040` staged kernel proves the selective spine: supplier/nation
and order-status filters reduce `lineitem l1` before applying the same-row
late-receipt comparison, avoiding a full-lineitem comparison in the joined
shape. The formal query then needs two repeated-alias membership checks over
the same physical `lineitem` table: `EXISTS` keeps `l1` rows whose order has
another supplier row (`l2`), and `NOT EXISTS` removes `l1` rows whose order has
another late supplier row (`l3`). The sibling-domain guardrails, `.050`, `.060`,
`.070`, and `.080`, are now native semi/anti membership shapes. Expected values
are derived from the SF 0.01 generated TPC-H files so future regressions can be
detected intentionally.
