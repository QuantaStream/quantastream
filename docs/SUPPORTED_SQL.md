# Supported SQL

Quanta supports a practical analytical SQL subset plus a small set of
Quanta-specific SQL extensions. This document records supported behavior that
is useful to users but may not be portable to MySQL or standard SQL.

Known unsupported or partial SQL behavior is tracked separately in
[`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md).

## Date And Time Helpers

Quanta supports a focused set of date/time scalar helpers in covered
select-list and grouping shapes:

- `year(date_value)` returns the four-digit year.
- `mm(date_value)` returns the month number.
- `monthofyear(date_value)` is an alias for `mm(date_value)`.
- `dayofweek(date_value)` returns the weekday number using Go's weekday
  convention, where Sunday is `0`.
- `hourofday(date_value)` returns the hour of day.

Example:

```sql
select order_id,
       year(order_date) as order_year,
       mm(order_date) as order_month,
       dayofweek(order_date) as order_dow,
       hourofday(order_date) as order_hour
from orders_qa;
```

These functions are inherited from the current expression layer and are not a
complete MySQL date/time compatibility surface. MySQL-style aliases such as
`month(...)` and `hour(...)` should be added deliberately with SQLRunner
coverage if Quanta chooses to expose them.

## Type Coercion Helpers

Quanta supports a focused set of qlbridge type-coercion helpers in covered
select-list and predicate shapes:

- `tostring(value)` coerces a value to string.
- `toint(value)` coerces a value to integer.
- `tonumber(value)` coerces a value to number.

Example:

```sql
select first_name, tostring(age) as age_text
from customers_qa;

select first_name
from customers_qa
where toint(age) >= 40;
```

MySQL `CAST(... AS ...)` compatibility should be treated separately from these
function-style helpers.

## String Scalar Helpers

Quanta supports a focused set of string scalar helpers in covered select-list
shapes:

- `lower(value)` lowercases a string value; `lcase(value)` is also covered.
- `upper(value)` uppercases a string value; `ucase(value)` is also covered.
- `length(value)` returns the string length; `char_length(value)` is also
  covered.
- `substr(value, start, length)` returns a substring; `substring(...)` and
  `mid(...)` are also covered. The SQL-facing behavior is one-based for the
  covered shapes.

Predicate coverage currently includes `lower(...)`, `lcase(...)`,
`upper(...)`, `ucase(...)`, `length(...)`, `char_length(...)`, `substr(...)`,
`substring(...)`, and `mid(...)` shapes.

Example:

```sql
select first_name, lower(city) as city_lower
from customers_qa;

select first_name, substr(first_name, 1, 2) as name_prefix
from customers_qa;
```

These functions are inherited from the current expression layer. Coverage
should be expanded deliberately when adding aliases or broader mixed-projection,
join, grouping, and aggregate shapes.

## Quanta Custom SQL

### `timediff(end_time, start_time, unit)`

`timediff(...)` is a Quanta scalar function for computing elapsed time between
two date/time values. Current SQLRunner coverage exercises the `'hours'` unit
in select-list, predicate, aggregate-filter, and joined select-list shapes.

Examples:

```sql
select order_id, timediff(ship_date, order_date, 'hours') as ship_hours
from orders_qa;

select count(*) as delayed_order_count
from orders_qa
where timediff(ship_date, order_date, 'hours') > 3;
```

Covered result values include integer and fractional hour strings, such as
`'3'`, `'4.5'`, and `'2'`.

This is custom Quanta SQL, not a MySQL-compatible `TIMEDIFF()` implementation.
Future coverage should characterize additional units, null handling, timezone
behavior, result typing, negative durations, and parity expectations with any
MySQL-compatible date/time functions Quanta chooses to expose.

### `topn(field)`

`topn(field)` is a Quanta aggregate extension that returns the most frequent
values for a field, their counts, and their percentage of the scanned result
set.

Example:

```sql
select topn(l_shipmode)
from lineitem;
```

Example result shape:

```text
+-----------------+------------+--------------+
| topn_l_shipmode | topn_count | topn_percent |
+-----------------+------------+--------------+
| TRUCK           |       8710 |        14.47 |
| MAIL            |       8669 |        14.41 |
| FOB             |       8641 |        14.36 |
| REG AIR         |       8616 |        14.32 |
| RAIL            |       8566 |        14.24 |
| AIR             |       8491 |        14.11 |
| SHIP            |       8482 |        14.10 |
| TOTAL:          |      60175 |       100.00 |
+-----------------+------------+--------------+
```

This is custom Quanta SQL, not MySQL-compatible syntax. It should be retained
because it maps naturally to bitmap-native cardinality work and is useful for
profiling categorical distributions.

Future coverage should characterize:

- alias behavior
- ordering guarantees
- tie behavior
- result limits
- filtered input behavior
- joined input behavior
- interaction with other aggregates
