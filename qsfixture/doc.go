// Package qsfixture provides deterministic direct-runtime fixtures.
//
// The package exists so SQLRunner smoke suites can exercise qsbridge and
// qsruntime without requiring a proxy, Consul, or bitmap node cluster. It also
// owns reusable in-memory fixture constructors for native relationship-vector,
// materialization, and same-row comparison kernel tests. It is intentionally
// small and should stay separate from production runtime adapters.
package qsfixture
