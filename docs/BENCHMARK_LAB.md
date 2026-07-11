# Benchmark Lab

The Benchmark Lab is the performance counterpart to the MySQL Compatibility
Lab. Compatibility answers whether QuantaStream behaves like MySQL for a
supported SQL surface. Benchmarking answers how QuantaStream performs under a
controlled deployment and workload.

Keep these signals separate. Correctness failures belong in compatibility or
SQL roadmap suites. Timing comparisons belong here only when the environment is
controlled enough to produce useful evidence.

## Goals

1. Measure QuantaStream performance under repeatable deployment profiles.
2. Compare QuantaStream and stock MySQL only when both systems run in comparable
   environments.
3. Preserve enough environment metadata that a result can be interpreted later.
4. Separate local directional runs from marketing-grade benchmark evidence.

## Non-Goals

- Do not use GitHub-hosted runner timing as benchmark evidence.
- Do not mix compatibility pass/fail decisions with benchmark pass/fail
  decisions.
- Do not tune MySQL or QuantaStream in undocumented one-off ways.
- Do not publish single-run wall-clock numbers as representative performance.

## Benchmark Tiers

### Tier 1: CI Compatibility

Purpose: correctness and smoke signal.

Environment: GitHub-hosted runners or similarly noisy shared runners.

Use for:

- SQLRunner parse and execution smoke tests
- compatibility report generation
- regression checks that do not depend on stable timing

Do not use for public timing claims.

### Tier 2: Developer Benchmark

Purpose: directional performance signal while developing.

Environment: laptop, workstation, or ad hoc cloud instance.

Use for:

- identifying obvious regressions
- validating that an optimization moves the right direction
- collecting preliminary query timings before a reference run

Treat results as local evidence only. Record hardware and deployment notes when
sharing numbers.

### Tier 3: Reference Benchmark

Purpose: repeatable benchmark evidence suitable for technical marketing or
release notes.

Environment: same cloud provider, region, instance family, storage class, and
network placement for QuantaStream and the comparison MySQL instance.

Required controls:

- Same cloud provider and region.
- Comparable CPU, memory, and storage classes.
- Benchmark runner on the same private network or equivalently documented
  network path.
- Fixed dataset and loader procedure.
- Warm-up runs separated from measured runs.
- Repeated measured runs with median and tail values, not a single sample.
- Explicit QuantaStream deployment mode, such as `inabox-standard`.
- Explicit MySQL version and configuration profile.

## Reference Deployment Shape

A useful apples-to-apples comparison is:

- QuantaStream deployed in `inabox-standard` mode.
- MySQL deployed in the same provider, region, and instance class.
- The benchmark runner deployed beside both systems on the same private network.
- The same logical dataset loaded into both systems.
- SQLRunner compatibility suites used for correctness gates before timing is
  interpreted.

This is not perfect equivalence because QuantaStream and MySQL use different
storage and execution models. It is, however, a disciplined comparison when the
purpose is to understand user-observable SQL behavior and response time.

## Measurement Rules

For every benchmark run, record:

- Git commit or release identifier.
- Deployment mode and topology.
- Cloud provider, region, instance type, and storage class.
- Dataset name, scale factor, and loader command.
- Suite name and exact command.
- Warm-up count and measured run count.
- Per-query median, p95 where available, and error count.
- Whether caches were cold, warm, or explicitly cleared.

A benchmark run should be reproducible from the recorded metadata without
relying on chat history or local memory.

## Relationship To SQLRunner

SQLRunner can support benchmark collection, but the compatibility lab should
remain correctness-first:

- `-capture_expected` and `-engine_diff` establish expected SQL behavior.
- `-compat_report` summarizes correctness-oriented compatibility categories.
- Timing comparison should be introduced as benchmark-lab reporting, not as a
  default compatibility failure mode.

Future benchmark tooling can add explicit flags or wrappers for repeated runs,
warm-up passes, and report artifacts. Those additions should preserve the
boundary between correctness and performance.

## Initial Work Items

1. Define the `inabox-standard` deployment profile and document its operational
   shape.
2. Add a benchmark runner wrapper that records environment metadata and repeats
   SQLRunner suites.
3. Add a MySQL reference deployment recipe for the same cloud profile.
4. Produce a local developer benchmark report format before attempting a public
   reference benchmark.
5. Promote benchmark results only after correctness has passed for the same
   suite and dataset.
