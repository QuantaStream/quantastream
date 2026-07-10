# Quanta Glossary

This glossary records vocabulary that matters when moving between SQL,
Quanta's logical planning model, and bitmap execution internals. New refactor
work should prefer these terms unless an adapter is explicitly speaking a
legacy or third-party API.

## Tuple Identity

### rownum

`rownum` is the Quanta tuple or entity identifier.

In SQL-facing planning, projection, join assembly, and result assembly code,
use `rownum` for the stable candidate identifier that represents one logical
row-like entity in a table.

`rownum` is a Quanta concept, not a Roaring Bitmap concept.

### columnID

`columnID` is the bitmap/BSI coordinate term for the numeric position of a set
bit.

At bitmap adapter boundaries, a Quanta `rownum` is translated to the
bitmap-facing `columnID` terminology. This name is appropriate when code is
calling into, adapting, or documenting bitmap/BSI primitives.

### bitmap row

A bitmap row represents an encoded value bucket in a standard bitmap.

Examples include:

- a `StringEnum` dictionary value
- a boolean state such as true or false
- another bitmap-backed value row

A bitmap row is not a relational row. Its set bits identify matching
`columnID` values.

### rowid

Avoid `rowid` in Quanta-native code and docs unless referring to external
relational database terminology.

`rowid` is overloaded: relational systems often use it for a tuple identifier,
while Quanta bitmap processing uses bitmap rows for value buckets. Prefer
`rownum` for Quanta tuple identifiers and `bitmap row` for value buckets.

## SQL Names And Physical Names

### table

Use `table` for SQL-facing catalog, parser, planner, and user documentation.

### index

Use `index` for Quanta's physical bitmap storage unit when code is speaking to
bitmap/runtime internals. In many current schemas, a SQL table maps to a Quanta
index, but the terms should not be treated as interchangeable at every layer.

### column

Use `column` for SQL-facing catalog, parser, planner, projection, and user
documentation.

### field

Use `field` for Quanta physical schema and bitmap/index adapter vocabulary.

Legacy code often uses `field` where SQL users would expect `column`. New
refactor code should use `column` at SQL-facing boundaries and reserve `field`
for Quanta physical representation or compatibility adapters.

### columnID: true

`columnID: true` is a schema-level setting that means a field's value supplies
the Quanta `rownum` for that table.

The name is inherited from bitmap terminology. SQL-facing design docs should
explain that this is a rownum source, even when the schema spelling remains
`columnID`.
