// Package qsruntime contains transitional runtime adapters for qsbridge.
//
// qsbridge owns parser, planner, and execution-handoff vocabulary. qsruntime
// owns the boundary where those neutral requests are routed to direct QIAB
// execution, local-node execution, or temporary compatibility adapters.
//
// The inabox-direct adapters are intentionally narrow compatibility islands.
// They let SQLRunner run the new SQL stack against a live local node cluster by
// borrowing core sessions, executing lowered bitmap fragments, and routing
// projection work through explicit native materialization kernels. They should
// not become a second planner or a place where historical parser/query-layer
// behavior leaks back into qsbridge.
//
// Historical qlbridge join code remains reference material only. New join
// execution should grow through explicit qsruntime kernel contracts around
// relationship-vector reduce, expand, semi, anti, and null-extension primitives
// instead of recreating the retired qlbridge query flow.
//
// Native projection and materialization semantics are qsbridge executor
// boundary concepts. qsruntime may stage executable adapters while direct
// storage and local node cluster seams are still mixed, but durable field hydration,
// rehydration, row assembly, batching, and fallback inspection vocabulary should
// migrate toward qsbridge-owned contracts rather than remain runtime folklore.
//
// RuntimeOwnershipForFile is the burn-down map for this package. It keeps
// durable engine contracts, native-kernel staging, temporary preflight
// scaffolding, runtime composition, metadata bridges, and legacy compatibility
// quarantine visibly separate while the package is still intentionally mixed.
//
// QueryScratchpad centralizes request-scoped memoization so inabox-direct,
// inabox-standard, and future distributed adapters do not grow separate cache
// implementations for the same execution algebra.
package qsruntime
