// Package qsexpr evaluates schema-owned QuantaStream expressions.
//
// The package is intentionally below core and qsruntime: it depends only on
// qsbridge vocabulary plus the standard library, so catalog defaults and stream
// selectors can be used by core loading/mutation code without importing legacy
// runtime adapters.
package qsexpr
