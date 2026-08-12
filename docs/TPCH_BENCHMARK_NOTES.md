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
2. Q21 is now a strong win versus MySQL but remains a multi-second query. Future
   work should focus on general sibling membership, repeated-alias planning, and
   candidate reuse rather than one TPC-H query.
3. Q5 remains a large absolute query even though it is much faster than MySQL.
   Graph pruning, edge ordering, and unnecessary edge elimination are likely the
   next general-purpose wins.
4. Q19 is now mostly polish. Its remaining gap is small in absolute terms and
   should be addressed through broader filter-domain normalization improvements.

## Tech Debt And Follow-Up Items

### Optimizer Completeness

Several fast paths still rely on hardcoded limits, feature flags, or local
eligibility checks. These should become optimizer decisions with recorded cost
evidence:

- Candidate materialization thresholds for dictionary leaves.
- Found-set bitmap pushdown versus late materialization.
- Count-only bitmap execution.
- Bitmap group count and bitmap group aggregate selection.
- Relationship edge ordering and graph pruning.
- Correlated sibling membership strategy selection.
- Same-row BSI compare versus projection/materialization fallback.

The goal is a small optimizer vocabulary that can explain choices using
candidate cardinality, expected bitmap cost, materialization field count, graph
shape, and available local or cluster capabilities.

### Cluster Mode Parity

Most recent wins were validated through `inabox-standard`, which shares many
logical boundaries with distributed execution but does not prove cluster parity.
Cluster-mode tests need to cover:

- Server-side found-set application for BSI and standard bitmap queries.
- Single-value `RowID` dictionary bitmap found sets.
- Same-row BSI comparison with found sets.
- Relationship reverse artifacts and graph candidate expansion.
- Bitmap grouped count and bitmap grouped aggregate kernels.
- Candidate-aware direct session dispatch across remote node boundaries.
- Probe parity so distributed runs can be debugged without in-process access.

The server-side set operations used by found-set pushdown are associative enough
to distribute, but ownership, shard boundaries, marshaling cost, and partial
result reduction still need explicit coverage.

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
