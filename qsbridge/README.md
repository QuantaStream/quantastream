# qsbridge

`qsbridge` is the protocol-neutral SQL planning vocabulary for QuantaStream. It
defines parser, binding, planning, diagnostic, client-metadata, and
adapter-facing contracts without importing runtime or storage packages.

The package boundary is intentionally narrow:

- SQL meaning and planner vocabulary live here.
- Runtime execution, bitmap/BSI storage, Consul, KV, and node transport live
  outside this package.
- MySQL wire adapters consume `qsbridge` contracts; they do not define SQL
  semantics.

`architecture_test.go` enforces the no-runtime-import rule for this package.

## Internal Architecture Notes

Detailed package state, design decisions, and migration notes are maintained in
the private internal repository:

- [qsbridge package notes](https://github.com/QuantaStream/quantastream-internal/blob/main/docs/qsbridge/README.md)
- [qsbridge design decisions](https://github.com/QuantaStream/quantastream-internal/blob/main/docs/qsbridge/DESIGN_DECISIONS.md)

Keep internal roadmap state in those files instead of relying on chat history or
expanding this public package README.

## Documentation Standard

All exported types, constants, and functions need documentation comments.

Use source comments for public API contracts, counter-intuitive planner behavior,
and local invariants that a maintainer needs while reading the code. Keep broad
architecture state and release planning in the internal docs linked above.

Tests should assert typed behavior, stable diagnostic codes, and plan shape.
