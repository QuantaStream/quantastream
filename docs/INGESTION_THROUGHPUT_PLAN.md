# Streaming Ingestion Throughput Plan

This document defines the next focused performance effort for QuantaStream's
streaming ingestion path. The objective is to increase sustained throughput
without weakening ordering, correctness, durability, visibility, or bounded
resource use.

The plan is transport-neutral. QuantaStream ingestion does **not** depend on
Amazon Kinesis, and this work does not introduce that dependency. Kinesis,
Kafka, HTTP producers, files, and other sources may be supported by adapters,
but the performance work belongs in QuantaStream's native ingestion boundary
and bitmap mutation path.

The effort has three distinct parts:

1. a deterministic load generator;
2. end-to-end instrumentation; and
3. measured improvements to the ingestion hot path.

The load generator and instrumentation make bottlenecks observable. They do
not, by themselves, improve throughput. The final performance gains must come
from batching, routing, concurrency, mutation, persistence, and allocation
changes justified by measurements.

## Current Architecture

The existing JSON loader accepts HTTP JSON, normalizes records into
`IngestEnvelope` values, applies selectors, and routes accepted records through
`core.SessionRouter`.

Important properties to preserve:

- router workers own their sessions because `core.Session` is not concurrency
  safe;
- independent workers may use independent sessions;
- rendezvous hashing provides affinity for the configured shard key;
- loader routing is distinct from upstream stream partition ownership and from
  distributed storage placement;
- HTTP acceptance is not the same as processing, flushing, committing, or
  durable acknowledgement;
- distributed and native gRPC modes must retain explicit completion semantics.

Any new fast path must preserve these ownership and correctness boundaries.

## Goals

The project should produce repeatable answers to the following questions:

- What are sustained records-per-second and bytes-per-second?
- What are p50, p95, and p99 latencies for acceptance, processing, flush, and
  durable commit?
- How does throughput scale with producer count, worker count, shard-key
  distribution, and batch size?
- What happens under hot keys, skew, bursts, retries, and downstream slowdown?
- Where are CPU time, allocations, lock contention, queueing, network time, and
  persistence time spent?
- Does the system maintain bounded memory and predictable backpressure?
- Can a benchmark result be reproduced from a commit, schema, data seed, and
  configuration?

Numeric throughput targets should be set only after the baseline is measured.

## Non-Goals

This effort will not:

- add Kinesis or another broker as a core ingestion dependency;
- require a broker to run the reference benchmark;
- make a `core.Session` concurrently shared by multiple workers;
- weaken selector, relationship, update, flush, commit, or recovery semantics;
- treat HTTP receipt as proof of durable ingestion;
- publish performance claims without reproducible configurations and raw
  results.

## Workstream 1: Deterministic Load Generator

Add a standalone load generator that targets QuantaStream's native ingestion
contract directly. HTTP should be available as an adapter for compatibility
measurement. Future source-specific adapters can be added without leaking
broker concepts into the core generator or server.

The generator should support:

- deterministic seeds;
- synthetic data and sanitized recorded corpora;
- repeatable multiplication of a source corpus;
- optional mutation of selected fields while preserving chosen business keys;
- exact record-count and byte-count reporting;
- configurable record and schema sizes;
- uniform, Zipfian, and intentionally hot shard-key distributions;
- insert, update, and mixed workloads;
- steady-rate, unbounded, and burst traffic;
- configurable producer concurrency and batch size;
- warm-up, measurement, and cool-down phases;
- machine-readable result output;
- controlled disconnect, retry, duplicate, and slowdown scenarios.

A multiplied-record mode must describe exactly which values are copied and
which are regenerated. It must not claim that data is sanitized unless the
sanitization step is explicit and tested.

Initial workload profiles should include:

1. simple bitmap mutations;
2. BSI-heavy numeric mutations;
3. StringEnum and backing-value mutations;
4. relationship mutations;
5. a single hot shard;
6. mixed inserts and updates; and
7. sustained traffic followed by a burst.

A Kinesis adapter may be useful for compatibility testing, but it is optional
and is not the reference transport.

## Workstream 2: End-to-End Instrumentation

Instrument the complete pipeline:

```text
receive -> decode -> normalize -> select -> route -> queue
        -> mutate -> persist -> acknowledge
```

### Throughput and latency

Record at least:

- accepted, rejected, processed, failed, flushed, and committed records;
- input and processed bytes;
- records and bytes per second;
- p50, p95, and p99 latency for each documented completion boundary;
- per-stage elapsed time;
- batch-size distributions;
- retries, duplicates, and retry outcomes.

### Resources and contention

Record at least:

- CPU time by major stage;
- allocations and allocation rate;
- garbage-collection frequency and pause time;
- lock or mutex wait time where measurable;
- worker utilization;
- queue depth, enqueue wait, and queue saturation;
- per-worker and per-shard load imbalance;
- mutation type counts;
- WAL, checkpoint, savepoint, flush, and commit timing;
- network and RPC latency, bytes, reconnects, and retries;
- storage latency and error counts.

Metrics must distinguish acceptance from processing and durability so a fast
HTTP response cannot be misread as completed ingestion.

## Workstream 3: Ingestion Hot-Path Improvements

Implement improvements one at a time and compare each result with the frozen
baseline.

### 1. Define a batch ingestion contract

Make batching an explicit server contract rather than only a producer
optimization. Define:

- maximum records and bytes per batch;
- whole-batch versus per-record failure behavior;
- partial-success reporting;
- ordering guarantees and ordering scope;
- retry and idempotency behavior;
- flush and commit boundaries;
- backpressure and overload responses.

Single-record ingestion should remain a batch of one through the same internal
path wherever practical.

### 2. Add a long-lived native streaming path

Evaluate a long-lived bidirectional gRPC stream that carries batches and
returns explicit acknowledgements. It should:

- avoid connection setup per request;
- integrate with bounded server-side flow control;
- distinguish received, processed, flushed, and committed acknowledgements;
- make retryable failures unambiguous;
- avoid implying that a successful send is durable.

### 3. Scale partition-local workers

Retain the worker-owned-session rule. Each worker should own its independent
session and process an explicit routing partition or shard-affinity group.

Measure:

- 1, 2, 4, and 8 workers initially;
- per-worker queue depth and utilization;
- ordering under hot keys;
- throughput degradation caused by imbalance;
- the point at which additional workers stop helping.

Avoid a global lock or shared session that erases the benefit of parallelism.

### 4. Route directly to the owning worker

Remove avoidable hops between decode, routing, queueing, and mutation where
measurements show they are significant. Keep loader affinity, source partition
ownership, and distributed storage ownership as separate concepts.

Document behavior for:

- rebalancing;
- stale ownership information;
- retries after rerouting;
- preservation of ordering scope.

### 5. Add bulk bitmap-native mutations

Group compatible operations by table, shard, field, and operation so the engine
can mutate roaring bitmaps, BSIs, dictionaries, backing values, and
relationships in bulk.

Bulk paths require:

- equivalence tests against record-at-a-time behavior;
- correct handling of updates and relationship changes;
- explicit temporary-memory limits;
- measurements for both throughput and tail latency.

### 6. Evaluate WAL group commit

Group persistence work where durability permits it rather than forcing
record-at-a-time synchronization. Define exactly when an acknowledgement is
issued and test recovery from:

- a completely written group;
- a partially written group;
- a torn tail;
- an ambiguous client retry after restart.

Group commit must improve amortization without weakening the documented
durability contract.

### 7. Reduce allocation and encoding overhead

Use profiles to identify repeated JSON decoding, map construction, byte copying,
and temporary object creation. Candidate improvements include:

- reusable buffers with explicit ownership;
- typed normalized records;
- fewer format conversions;
- binary-native messages for the native stream;
- pooling only where profiles show a benefit.

Do not introduce complicated pooling or unsafe reuse without a measured gain and
race coverage.

### 8. Make backpressure explicit and bounded

When workers or persistence cannot keep up, the system should slow or reject
producers predictably. It must not convert overload into unbounded memory growth
or increasingly misleading acceptance latency.

Define:

- queue limits;
- retryable overload errors;
- producer flow-control behavior;
- metrics and alerts for saturation;
- shutdown and drain behavior.

## Benchmark Matrix

Every benchmark artifact should record the following dimensions.

| Dimension | Initial values |
| --- | --- |
| Topology | single-node in-process, native gRPC, distributed development |
| Durability | WAL disabled, WAL enabled, flush boundary, commit boundary |
| Workers | 1, 2, 4, 8 |
| Batch records | 1, 10, 100, 1,000, subject to a byte limit |
| Producers | 1, 2, 4, 8, 16 |
| Key distribution | uniform, Zipfian, single hot shard |
| Payload size | small, medium, large with exact byte counts |
| Schema profile | bitmap, BSI, StringEnum, relationship, mixed |
| Mutation mix | insert, update, mixed |
| Traffic pattern | steady, maximum-rate, burst |
| Duration | warm-up plus a sustained measurement interval |

Save with every result:

- commit and build identifiers;
- generator version and complete configuration;
- schema, seed, and dataset or corpus description;
- host CPU, memory, storage, and operating-system details;
- topology and durability settings;
- raw metric output;
- summarized throughput and latency;
- correctness and recovery results.

## Correctness Gates

A throughput improvement is not complete until it passes:

- existing focused and full Go tests;
- record-at-a-time versus batch equivalence tests;
- selector and rejection behavior;
- ordering tests, including hot keys;
- update and relationship-mutation tests;
- WAL restart and torn-tail recovery tests;
- flush and commit boundary tests;
- overload and bounded-memory tests;
- verification that accepted records are not silently lost.

The first implementation phase must also make an explicit decision about
idempotency: either define a supported idempotency key and retention policy or
document that clients must handle possible duplicate application after
ambiguous retries.

## Implementation Sequence

1. Freeze and record the current baseline.
2. Add pipeline and completion-boundary instrumentation.
3. Add the deterministic load generator and initial workload profiles.
4. Define and implement the batch ingestion contract.
5. Add bulk bitmap-native mutation paths.
6. Measure and, if justified, add WAL group commit.
7. Add the long-lived native streaming transport.
8. Tune routing, worker count, encoding, and allocations from profiles.
9. Run scale, skew, overload, restart, and recovery tests.
10. Set numeric performance targets and publish reproducible results.

Each step should produce an independently reviewable change and a before/after
benchmark. Avoid combining multiple optimizations into a result that cannot be
attributed.

## Initial Deliverables

The first milestone should leave the repository with:

- a checked-in deterministic load generator;
- checked-in workload configurations and representative schemas;
- a machine-readable benchmark result format;
- instrumentation for each pipeline stage and completion boundary;
- documented acceptance, flush, commit, and durability semantics;
- a reviewed batch and idempotency contract;
- a measured bottleneck list that determines the next optimization.

This is the plan for the next ingestion-performance effort. Implementation
should begin with measurement and correctness gates, then proceed to the
highest-cost verified bottleneck.
