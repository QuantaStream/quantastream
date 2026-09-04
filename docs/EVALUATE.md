# Evaluate QuantaStream 0.1.2

QuantaStream is in an early adoption phase. The most useful feedback comes from
trying one bounded workflow and reporting the exact point where it succeeds,
becomes confusing, or fails.

Choose one of the following tracks. Do not share credentials, private data, or
unsanitized logs in a public issue.

## Track A: First Query

Use the Linux/AMD64 release bundle to restore the included TPC-H SF0.01 sample,
run the local health checks, and execute an aggregate query.

Run permissive authentication only on the documented loopback bind. It accepts
all valid MySQL handshakes and is provided solely for isolated evaluation.

Start with [Getting Started](GETTING_STARTED.md).

When you finish, report:

- operating system and architecture;
- whether checksum verification passed;
- whether `qstream-admin doctor local` passed;
- approximate time from download to first query; and
- the first confusing or failed step, if any.

## Track B: Migrate a Small MySQL Model

Use the public
[`qstream-migrate`](https://github.com/QuantaStream/qstream-migrate)
workbench to analyze a small relational model, review the generated mapping,
load it, and validate row counts.

Begin with 3-8 tables and one analytical question. The workbench documentation
includes a complete seven-table FoodMart star-schema walkthrough.

Report table and row counts, relationships found, plan warnings, manual mapping
changes, load and validation results, and the query used to judge usefulness.
Use public, synthetic, or safely anonymized details only.

## Track C: Connect Tableau

Use Tableau's **Other Databases (JDBC)** connector with MySQL Connector/J,
connect to QuantaStream, open a curated view, and try one worksheet or extract.

Start with the
[`quantastream-tableau`](https://github.com/QuantaStream/quantastream-tableau)
quick-connect guide.

Report Tableau and Connector/J versions, operating-system arrangement,
connection and metadata outcome, and the phase where a failure occurred. Remove
passwords and other sensitive values from JDBC URLs and logs.

## Report Results

Add a short result to the
[0.1.2 feedback thread](https://github.com/QuantaStream/quantastream/issues/16).

For a reproducible defect, use the repository's structured bug report form. A
useful report states the attempted outcome, affected version, environment,
numbered reproduction steps, expected and observed behavior, and the smallest
safe example that reproduces the problem.

Before reporting an SQL compatibility failure, check
[Supported SQL](SUPPORTED_SQL.md) and [Known SQL Boundaries](UNSUPPORTED_SQL.md).
For private workload discussions, email
[info@quantastream.org](mailto:info@quantastream.org).
