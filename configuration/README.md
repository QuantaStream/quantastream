# Configuration Documentation

This directory is the public entry point for QuantaStream configuration
documentation and small sample schemas.

Start with the [Schema Design Guide](../docs/SCHEMA_DESIGN.md) when deciding how
to model tables, keys, relationships, and mapper choices. Use the
[Schema Config Reference](SCHEMA_CONFIG_REFERENCE.md) when you need the exact
`schema.yaml` fields and mapper-specific options.

## Documents

- [Schema Config Reference](SCHEMA_CONFIG_REFERENCE.md) - complete field-level
  reference for table schema YAML files, catalog manifests, view definitions,
  mapper options, relationship artifacts, selectors, and text search metadata.
- [Auth And Access](AUTH_ACCESS.md) - MySQL-compatible users, roles, grants,
  and access-policy configuration.
- [Streaming Loader Configuration](STREAMING_LOADER.md) - `quantastream-loader`
  startup, JSON ingest envelopes, routing behavior, health checks, and producer
  examples.

## Sample Schemas

- [cities/schema.yaml](cities/schema.yaml) - compact local table example.
- [cityzip/schema.yaml](cityzip/schema.yaml) - compact local table example with
  additional lookup-style fields.

## Related Guides

- [Schema Design Guide](../docs/SCHEMA_DESIGN.md) - physical modeling guidance
  and mapping-strategy tradeoffs.
- [Supported SQL](../docs/SUPPORTED_SQL.md) - supported SQL surface area.
- [Unsupported SQL](../docs/UNSUPPORTED_SQL.md) - known unsupported or partial
  SQL features.
- [Deployment Guide](../docs/DEPLOYMENT.md) - local, single-node, and
  distributed deployment notes.
