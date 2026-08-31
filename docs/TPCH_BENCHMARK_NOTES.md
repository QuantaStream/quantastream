# TPC-H Benchmark Notes

These notes capture benchmark evidence and follow-up work that is too specific
for the general Benchmark Lab methodology. Treat the numbers here as dated
engineering evidence, not as a public benchmark publication.

## 2026-08-12 SF1 AWS Checkpoint

### Run Shape

- Dataset: TPC-H SF1.
- QuantaStream profile: `inabox-standard`.
- Suite: `tpc-h-benchmark/sqltests/tpch_benchmark_readonly_sf1_scale_safe.yaml`.
- Target commit: `5b5187c`.
- Benchmark runner host: `bench-runner`, Linux amd64, 8 CPUs, Go 1.24.0.
- MySQL baseline: `mysql-reference/aws-m7i-2xlarge-sf1`.
- Warm-up: 1 full suite pass.
- Measured runs: 3.
- Final report: `/tmp/aws-qs-readonly-sf1-final-warm.json`.

The target report was marked `repo_dirty=true` on the AWS runner. The measured
commit should still be considered the checkpoint identifier because the server
build and SQLRunner metadata both point at `5b5187c`, but future reference runs
should use a clean checkout before publication-grade claims.

### Final Comparison

| Case | MySQL Baseline | QuantaStream | Delta | Ratio |
| --- | ---: | ---: | ---: | ---: |
| `tpch_benchmark_readonly.001.lineitem_seed_count` | 518ms | 18ms | -500ms | 0.03x |
| `tpch_benchmark_readonly.010.q1_grouped_lineitem_shape` | 2368ms | 230ms | -2138ms | 0.10x |
| `tpch_benchmark_readonly.020.q3_grouped_revenue_limit` | 6150ms | 4514ms | -1636ms | 0.73x |
| `tpch_benchmark_readonly.030.q5_combined_graph_regional_count` | 8872ms | 1927ms | -6945ms | 0.22x |
| `tpch_benchmark_readonly.040.q6_discounted_revenue` | 1406ms | 523ms | -883ms | 0.37x |
| `tpch_benchmark_readonly.050.q12_same_row_date_comparison` | 1674ms | 487ms | -1187ms | 0.29x |
| `tpch_benchmark_readonly.060.q16_part_filter_count` | 69ms | 34ms | -35ms | 0.49x |
| `tpch_benchmark_readonly.070.q19_formal_discounted_revenue` | 115ms | 277ms | +162ms | 2.41x |
| `tpch_benchmark_readonly.080.q21_sibling_exists_count` | 7060ms | 2478ms | -4582ms | 0.35x |
| `tpch_benchmark_readonly.090.shipdate_year_group_count` | 1448ms | 616ms | -832ms | 0.43x |

### Interpretation

The useful story is not that QuantaStream beats MySQL on every individual query.
The useful story is that bitmap-native execution is already compelling on a
representative set of relationship-heavy analytical shapes:

- Q1 shows bitmap grouped aggregation over a broad fact table.
- Q3 shows relationship-vector graph aggregation with final materialization
  pruning.
- Q5 shows multi-table graph filtering where relationship artifacts and edge
  ordering avoid unnecessary tuple expansion.
- Q12 shows same-row BSI comparison with found-set pushdown.
- Q16 shows count-only bitmap filtering over selective part predicates.
- Q21 shows repeated-alias sibling membership becoming practical at SF1.

Q19 remains slower than MySQL by ratio, but the remaining absolute gap is small
in this suite. The isolated readonly Q19 rerun at the same commit was stable:

- Report: `/tmp/aws-readonly-q19-rowid-foundset.json`.
- Runs: 5 measured, no warm-up.
- Min/median/max: 271ms / 277ms / 279ms.
- Key probes:
  - `candidate_bitmap_query_supported=true`.
  - `candidate_bitmap_query_handled=true`.
  - `standard_found_set_count=1`.
  - `standard_found_set_rows=3204`.
  - `lineitem.l_shipmode` leaf rows dropped from the previous full bitmap size
    of 858104 to 437.
  - Final filter candidate rows were 121.

The Q19 profile says the remaining cost is mostly filter-domain normalization
and filter-tree evaluation, not a failed bitmap lookup. The next Q19 work should
fall out of broader optimizer work rather than driving the roadmap.

### Q17 Correlated-Subquery Scope

Q17 is covered in compatibility/kernel suites, including the formal
correlated-average quantity predicate, but it is not part of the SF1 readonly
checkpoint suite or the final comparison table above. None of the checkpoint
numbers in this section depend on Q17.

The current formal Q17 correlated-average path is a shape-specific rewrite for
the TPC-H pattern: `part` filtered by `p_brand` and `p_container`, joined to
`lineitem`, with an outer `l_quantity` predicate compared to a correlated
average over matching `l_partkey` rows. The implementation still carries
Q17-flavored internal names such as `q17PartKeySeed` and `q17PartThreshold`.
Treat any Q17-specific timing as evidence for that correlated aggregate shape,
not as proof that arbitrary correlated aggregate subqueries have a general
native execution path yet. Generalizing this support is tracked in
[#21](https://github.com/QuantaStream/quantastream/issues/21).

## Mechanisms Proven During This Pass

The final SF1 result depends on a set of reusable execution mechanisms rather
than one benchmark-specific branch:

- Direct bitmap found-set pushdown for constrained leaf evaluation.
- Single dictionary-value `RowID` bitmap queries honoring found sets.
- Same-row BSI comparison constrained by prior candidate sets.
- Count-only bitmap execution for eligible aggregate shapes.
- Bitmap grouped count and bitmap grouped aggregate kernels.
- Dependency-aware relationship edge ordering.
- Child-domain expansion seeding and final materialization pruning for graph
  aggregates.
- StringEnum dictionary bitmap handling for categorical filters.
- Runtime probes that expose optimizer choices and lower-level bitmap timings.

These mechanisms are the basis for the strategic benchmark story. They should be
described as execution-model advantages, not as isolated TPC-H hacks.

## Remaining Performance Boulders

1. Q3 is the largest remaining wall-clock target in the readonly suite. It is
   faster than the MySQL baseline at SF1, but still consumes about 4.5 seconds.
   Local SF0.05 is adequate for finding shape-level wins; AWS SF1 should be
   used to validate material changes.
   The latest local probe pass showed the remaining Q3 aggregate cost is mostly
   broad time-shard fan-out while reading `lineitem.l_extendedprice`: an SF0.05
   readonly Q3 run touched 2518 `YMD` BSI shards and retained 2487 of them for
   only 14908 lineitem rows. A sparse-shard value-read shortcut reduced internal
   value-read time but did not improve wall-clock median, so this should be
   treated as future storage/optimizer work rather than a near-term hot-loop
   tweak.
2. Q21 is now a strong win versus MySQL but remains a multi-second query. Future
   work should focus on general sibling membership, repeated-alias planning, and
   candidate reuse rather than one TPC-H query.
3. Q5 remains a large absolute query even though it is much faster than MySQL.
   Graph pruning, edge ordering, and unnecessary edge elimination are likely the
   next general-purpose wins.
   Local cluster SF0.05 follow-up showed the current supplier-first graph order
   is important: forcing the customer/orders branch before the supplier/nation
   branch regressed `tpch_profile_scale.q5.formal_revenue` from about 240ms to
   about 632ms median. The domain-mapping cache bridge from graph reduction into
   alignment is also important; attempts to skip or duplicate it regressed Q5.
4. Q19 is now mostly polish. Its remaining gap is small in absolute terms and
   should be addressed through broader filter-domain normalization improvements.

## 2026-08-15 AWS Distributed SF3 Load Boundary

The current three-node AWS distributed test cluster establishes a useful
capacity boundary rather than a stable SF3 benchmark target.

### Cluster Shape

- Data nodes: 3 x `m7i.4xlarge`.
- Per-node shape: 16 vCPU, 64 GiB memory.
- Loader host: `bench-runner`.
- Node sync: disabled for benchmark redeploys.
- Consul health check profile: production by default, with a coarse
  `bulk-load` profile available for ingest runs that can temporarily starve
  health checks.

### Load Findings

- SF1 best observed loader shape: `QS_TPCH_LOAD_WORKERS=12`,
  `QS_TPCH_LOAD_BATCH_SIZE=2000`.
- SF2 best stable loader shape: `QS_TPCH_LOAD_WORKERS=12`,
  `QS_TPCH_LOAD_BATCH_SIZE=1000`.
- SF3 on the current three-node shape is not stable with the current
  loader/node memory behavior. Attempts at `w12-b1000`, `w6-b1000`, and
  `w3-b1000` all failed near the end of `lineitem` load or final commit after
  one data node left the cluster.

The decisive failure signature is kernel OOM on a data node, not a Consul
health-check false positive:

```text
Out of memory: Killed process ... quantastream-no ... anon-rss:63369968kB
```

That is roughly 60 GiB resident memory on a 64 GiB node. In another run, the
loader saw `BatchMutateItems` return `EOF`, then `GetAllPeerStatus` failed with
`connect: connection refused` because the target node was no longer listening on
port 4400. After restart, Pogreb recovery logs appeared, confirming the load was
tainted and should not be benchmarked.

A follow-up client bug was also fixed: when membership changed during final
commit, error formatting could index the current client list using an index from
an older fanout snapshot and panic with `index out of range`. Commit
`7573517` changed error formatting to use a safe client-target helper so this
class of failure returns a useful error instead of masking the node failure with
a loader panic.

### Capacity Interpretation

This should be recorded as a scale boundary:

- SF1 and SF2 are usable for clean query benchmarks on the current cluster.
- SF3 requires either more memory per node, more data nodes, or a reduction in
  node-side ingest memory pressure before it should be used as a benchmark data
  point.
- Re-running SF3 on the same three-node `m7i.4xlarge` shape is not likely to
  produce new information until memory behavior changes.

AWS does not allow memory to be hot-added to an EC2 instance. To add memory to a
node, stop the EBS-backed instance and change its instance type, or launch a
replacement instance type. Practical next configurations are:

- Scale up each data node to a larger general-purpose instance such as
  `m7i.8xlarge` for 128 GiB per node.
- Move each data node to a memory-optimized shape such as `r7i.4xlarge`, which
  keeps 16 vCPU while doubling memory to 128 GiB.
- Scale out to five data nodes to reduce per-node shard ownership and increase
  total cluster memory.

The cleanest diagnostic next step is memory-up first, preferably
`r7i.4xlarge`, because it isolates the variable: same vCPU count, double memory.
Five nodes is still likely useful for the product shape, but it changes both
capacity and distribution behavior at once.

## Tech Debt And Follow-Up Items

### Optimizer Completeness

Several fast paths still rely on hardcoded limits, feature flags, or local
eligibility checks. These should become optimizer decisions with recorded cost
evidence:

- Candidate materialization thresholds for dictionary leaves.
- Found-set bitmap pushdown versus late materialization.
- Count-only bitmap execution.
- Bitmap group count and bitmap group aggregate selection.
- Cluster relationship aggregate pushdown versus query-process reduction.
- Relationship edge ordering and graph pruning.
- Correlated sibling membership strategy selection.
- Same-row BSI compare versus projection/materialization fallback.
- Partitioned BSI projection strategy when candidate rows are spread across many
  time shards.

The goal is a small optimizer vocabulary that can explain choices using
candidate cardinality, expected bitmap cost, materialization field count, graph
shape, and available local or cluster capabilities.

### Partitioned BSI Projection Fan-Out

Q3 exposed a useful storage-layout ceiling. Because `lineitem` is partitioned by
`l_shipdate`, projected measure fields such as `l_extendedprice` inherit the
same `YMD` shard layout. When a graph aggregate has a broad child-row candidate
set and no predicate on the table's shard key, the storage aggregate must discover
row locations by probing many shard existence bitmaps before reading values.

Possible future directions:

- row-to-shard routing metadata for partitioned tables,
- shard-grouped projection requests after graph reduction,
- optimizer estimates for shard fan-out versus late materialization, and
- schema guidance when a table's query shapes do not benefit from fine-grained
  time partitioning.

This is not currently worth adding as query-specific Q3 complexity. The existing
runtime probes are sufficient to identify the issue, and the feature belongs in a
general partitioned-projection strategy.

### Cluster Mode Parity

Most recent wins were validated through `inabox-standard`, which shares many
logical boundaries with distributed execution but does not prove cluster parity.
Cluster-mode tests need to cover:

- Server-side found-set application for BSI and standard bitmap queries.
- Single-value `RowID` dictionary bitmap found sets.
- Same-row BSI comparison with found sets.
- Q3 relationship aggregate parity through the shared projection path.
- Relationship reverse artifacts and graph candidate expansion.
- Bitmap grouped count and bitmap grouped aggregate kernels.
- Candidate-aware direct session dispatch across remote node boundaries.
- Probe parity so distributed runs can be debugged without in-process access.

The first cluster-parity phase wires the Q3 relationship aggregate hook in the
shared runtime using cluster projection plus query-process reduction. That is
correct for local-cluster and distributed execution and avoids pretending the
in-process standard bitmap index exists in cluster mode. It is still not the
final node-local pushdown story: grouped bitmap aggregates, reverse-artifact
summaries, and relationship aggregate pushdown/reduction need explicit RPC and
probe coverage before they can match the standard-mode physical tier.

Local-cluster SF0.05 checkpoint through `f1758b0`:

- Fresh cluster direct load completed in 33s using `TPCH_LOAD_MODE=cluster`.
- `tpch_profile_scale` passed on `inabox-direct` with median timings: Q3 720ms,
  Q5 same-nation graph count 500ms, Q5 formal revenue 472ms, Q12 119ms,
  Q19 233ms, Q10 207ms, Q6 62ms, and Q11 18ms.
- `tpch_profile_slow_paths` passed on `inabox-direct` with median timings:
  Q1 grouped lineitem shape about 150ms after count-only bitmap pushdown, Q1 BSI
  summary about 469ms after direct grouped BSI aggregates, shipdate-year group
  193ms, and Q21 sibling exists 671ms.
- Q3 profile on SF0.05 remains a cluster-parity target: the full-group profile
  shape measured around 856ms median, with warm runs split across graph
  reduction (~283-304ms), shared projection aggregate (~225-250ms), and final
  order-field materialization (~166-177ms). The readonly `LIMIT 10` shape already
  uses unordered final-materialization pruning; the remaining full Q3 gap needs
  a mergeable relationship aggregate/value-vector RPC rather than another local
  threshold or feature flag.

### Benchmark Reproducibility

Before sharing externally, build a reproducible proof package:

- Pin instance types, region, storage class, Go version, MySQL version, and
  loader commands.
- Require clean target checkouts for reference reports.
- Preserve raw JSON reports and rendered summaries for each checkpoint.
- Capture SF0.05, SF1, and at least one larger scale factor if practical.
- Separate local discovery runs from reference evidence.
- Publish warm-up policy, measured run count, median and tail values, and
  correctness validation steps.

The public story should emphasize scale behavior and execution-shape advantage,
not a hard sell against MySQL.

### BI Tool SQL Shapes

QuantaStream's likely wedge is BI-generated analytical SQL that includes extra
joins, repeated dimensions, unnecessary select-list fields, broad groupings, and
filter-heavy relationship traversals. Add fixtures that approximate output from:

- Power BI.
- Tableau.
- Looker.
- Sigma.
- ThoughtSpot.

These fixtures should test the optimizer's ability to ignore unnecessary work
and preserve interactivity when generated SQL is more complex than a hand-tuned
query.

### Probe Hygiene

The current probes are excellent for engineering but too verbose for public
evidence. Keep detailed probes in raw JSON, then add a compact plan-trace report
that summarizes:

- chosen execution kernels,
- candidate row counts,
- materialization fields and rows,
- graph edge order,
- found-set use,
- elapsed time by major phase.

That plan trace can support external credibility without exposing every internal
implementation detail.
