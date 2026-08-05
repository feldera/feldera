# JSON and Dynamically-Typed Value Support

## Direct conversion of JSON strings to User-Defined data types

Consider the following example:

```sql
CREATE TYPE address AS (
   city VARCHAR,
   street VARCHAR,
   number INT
);

CREATE TABLE data(addr VARCHAR);
```

Let us assume that the table `data` contains JSON strings that encode
addresses, e.g.,

```
{ "city": "Boston", "street": "Main", "number": 10 }
```

One way to convert JSON strings to values of a user-defined data type
is to perform the conversion in two steps:

- parse the JSON string using the `PARSE_JSON` function, described
  below.  This function returns a value of type `VARIANT`.
- cast the `VARIANT` value to a value of a user-defined data type.

This can be achieved with the following code:

```sql
CREATE VIEW decoded AS
SELECT CAST(PARSE_JSON(data.addr) AS address) FROM data;
```

The conversion to `VARIANT` is wasteful, since often the JSON string
can be directly parsed into a user-defined data type.  Since SQL
provides no convenient syntax for writing generic functions, the
Feldera compiler provides the following mechanism:

- The user can declare a function with a name of the form
`jsonstring_as_<udt>`, where `<udt>` is the name of a user-defined
data type.  The function's argument is a nullable string type, while
the function's result type is a nullable value of the user-defined
type.

- The compiler will automatically synthesize the body of this function.

- The user can directly invoke the function in expressions.

With these changes, the previous program can be rewritten as:

```sql
CREATE FUNCTION jsonstring_as_address(addr VARCHAR) RETURNS address;
CREATE VIEW decoded AS
SELECT jsonstring_as_address(data.addr) FROM data;
```

This program is more efficient than the previous one, but achieves
almost the same effect.

There is a subtle difference between the two programs: the former
program, using `VARIANT` requires the case of fields in the JSON to match
the case of the *normalized* fields in the user-defined type, whereas
the automatic function using `jsonstring` is case-insensitive.  With
the default settings, the following record:

```
{ "city": "Boston", "street": "Main", "NUMBER": 10 }
```

returns a structure with a `NULL` value for the `NUMBER` field using
the first method.  Using the second method all three fields are
deserialized.

## The `VARIANT` type

Values of `VARIANT` type are dynamically-typed.
Any such value holds at runtime two pieces of information:
- the data type
- the data value

Values of `VARIANT` type can be created by casting any other value to
a `VARIANT`, e.g., `SELECT CAST(x AS VARIANT)`.  Conversely, values of
type `VARIANT` can be cast to any other data type, e.g., `SELECT CAST(v AS INT)`.
A cast of a value of type `VARIANT` to target type `T` will
compare the runtime type with `T`.  If the types are identical or there
is a natural conversion from the runtime type to `T`, the original value
is returned.  Otherwise the `CAST` returns `NULL`.

A value of type `VARIANT` that stores a `MAP` can be converted to a
user-defined type.  Each name of a field of the user-defined type is
used as an index into the map.  If a field is missing in the map,
the corresponding field of the struct gets the `NULL` value.  Fields
in the map that do not correspond to the struct field names are
ignored.

:::note

Remember that the `DECIMAL` type specified without precision is the
same as `DECIMAL(38, 0)`, with no digits after the decimal point.  When
you cast a `VARIANT` value to `DECIMAL` you should specify a precision
and scale large enough for the values that you expect in the data.

:::

Values of type `ARRAY`, `MAP` and user-defined types can be cast to
`VARIANT`.

There exists a special value of `VARIANT` type called `null`.  This
value is different from the SQL `NULL` value.  It is used to implement
the JSON `null` value.  An important difference is that two `VARIANT`
`null` values are equal, whereas `NULL` in SQL is not equal to anything.

Converting a user-defined type to a `VARIANT` produces a `VARIANT`
storing a value of type `MAP<VARCHAR, VARIANT>`, where each field of
the map corresponds to a field of the user-defined structure.

`VARIANT` values also offer the following operations:

- indexing using array indexing notation `variant[index]`.  If the `VARIANT` is
  obtained from an `ARRAY` value, the indexing operation returns a `VARIANT` whose value
  is the element at the specified index.  Otherwise, this operation returns `NULL`
- indexing using map element access notation `variant[key]`, where `key` can have
  any legal `MAP` key type.  If the `VARIANT` is obtained from a `MAP` value
  that has an element with this key, a `VARIANT` value holding the associated value in
  the `MAP` is returned.  Otherwise `NULL` is returned.  If the `VARIANT` is obtained from
  user-defined structure which has a field with the name `key`, this operation returns a `VARIANT` value holding
  the corresponding field value.  Otherwise `NULL` is returned.
- field access using the dot notation: `variant.field`.  This operation is interpreted
  as equivalent to `variant['field']`.  Note, however, that the field notation
  is subject to the capitalization rules of the SQL dialect, so for correct
  operation the field may need to be quoted: `variant."field"`

## Functions that operate on `VARIANT` values

| Function                      | Description |
|-------------------------------|-------------|
| `VARIANTNULL()`               | Can be used to create an instance of the `VARIANT` `null` value. |
| `TYPEOF(variant)`             | Argument must be a `VARIANT` value.  Returns a string describing the runtime type of the value |
| `PARSE_JSON(string)`          | Parses a string that represents a JSON value, returns a `VARIANT` object, or `NULL` if parsing fails (more details [below](#parse_json)) |
| `TO_JSON(variant)`            | Argument must be a `VARIANT` value.  Returns a string that represents the serialization of a `VARIANT` value. If the value cannot be represented as JSON, the result is `NULL` (more details [below](#to_json)) |
| `JSON_EACH_<type>(variant)`   | A family of functions; each extracts from a `VARIANT` holding a JSON object the fields whose values have a specified runtime type, as a `MAP` (more details [below](#json_each)) |
| `JSON_OBJECT_KEYS(variant)`   | Returns the top-level keys of a `VARIANT` holding a JSON object, as a sorted `ARRAY` of strings (more details [below](#json_object_keys)) |
| `JSON_KEYS(variant)`          | Returns the keys of all nested objects in a `VARIANT`, as a sorted `ARRAY` of dot-joined paths (more details [below](#json_keys)) |
| `VARIANT_FILTER(variant, lambda)` | Returns a `VARIANT` with the items of the input for which a predicate lambda is true (more details [below](#variant_filter)) |
| `VARIANT_DEEP_FILTER(variant, lambda)` | Like `VARIANT_FILTER`, but recursive: the predicate receives the dot-joined path of each nested item (more details [below](#variant_deep_filter)) |
| `VARIANT_MAP(variant, lambda)` | Builds a `VARIANT` isomorphic to the input, with each value replaced by the lambda's result (more details [below](#variant_map)) |
| `VARIANT_DEEP_MAP(variant, lambda)` | Like `VARIANT_MAP`, but recursive: transforms only the leaves, labeled by their dot-joined path (more details [below](#variant_deep_map)) |
| `VARIANT_MERGE(variant, variant)` | Merges two `VARIANT` values recursively; the second wins on conflicts (more details [below](#variant_merge)) |

### `PARSE_JSON`

`PARSE_JSON` converts a JSON value as follows:

- JSON `null` is converted to a `VARIANT` `null` value (not a SQL `NULL`!); see above the description of `VARIANT` `null`
- JSON Booleans are converted to `BOOLEAN` values (wrapped in `VARIANT` values)
- JSON integer numbers are converted to `BIGINT` values for negative integers, and to `BIGINT UNSIGNED` values for non-negative integers (wrapped in `VARIANT` values)
- other JSON numbers are converted to `DECIMAL` values (wrapped in `VARIANT` values)
- JSON strings are converted to `VARCHAR` values (wrapped in `VARIANT` values)
- JSON arrays are converted to `VARIANT ARRAY` values (wrapped in `VARIANT` values).  Each array element is a `VARIANT`
- JSON objects are converted to `MAP<VARIANT, VARIANT>` values (wrapped in `VARIANT` values).  Each key and each value is a `VARIANT`

For example, `PARSE_JSON('{"a": 1.0, "b": [2.2, 3.3, null]}')` generates the same SQL value that would be generated by the following code:

```sql
SELECT CAST(
   MAP[
      CAST('a' AS VARIANT), CAST(1.0 AS VARIANT),
      CAST('b' AS VARIANT), CAST(ARRAY[
          CAST(2.2 AS VARIANT),
          CAST(3.3 AS VARIANT),
          VARIANTNULL()
                                      ] AS VARIANT)
      ] AS VARIANT)
```

### `TO_JSON`

`TO_JSON` converts a `VARIANT` value to a `VARCHAR`:

- the `VARIANT` `null` value is converted to the string `null`
- a `VARIANT` wrapping a Boolean value is converted to the respective Boolean string `true` or `false`
- a `VARIANT` wrapping any numeric value (`DECIMAL`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `TINYINT UNSIGNED`,
  `SMALLINT UNSIGNED`, `INT UNSIGNED`, `BIGINT UNSIGNED`, `REAL`, `DOUBLE`) is converted
   to the string representation of the value as produced using a `CAST(value AS VARCHAR)`
- a `VARIANT` wrapping a `VARCHAR` value is converted to a string with double quotes, and with escape sequences for special characters (e.g., quotes), as mandated by the JSON grammar
- a `VARIANT` wrapping an `ARRAY` with elements of any type is converted to a JSON array, and the elements are recursively converted
- a `VARIANT` wrapping a `MAP` whose keys have any SQL `CHAR` type, or are `VARIANT` values wrapping `CHAR` values, is converted to a JSON object by recursively converting each key-value pair.
- a `VARIANT` wrapping a `DATE`, `TIME`, or `DATETIME` value will be serialized as a JSON string

### `JSON_EACH`

The `JSON_EACH_<type>` functions generalize the Postgres `json_each_text`
function.  Each function in the family extracts from a `VARIANT` value
holding a JSON object the fields whose values have a specified runtime
type.  The result is a `MAP` from field name to field value; the `MAP`
value type is nullable, but the extracted values are never `NULL`.

| Function                        | Result type                    | Fields extracted |
|---------------------------------|--------------------------------|------------------|
| `JSON_EACH_BIGINT(variant)`     | `MAP<VARCHAR, BIGINT>`         | Numeric values with no fractional part that fit in `BIGINT` |
| `JSON_EACH_STRING(variant)`     | `MAP<VARCHAR, VARCHAR>`        | String values |
| `JSON_EACH_BOOLEAN(variant)`    | `MAP<VARCHAR, BOOLEAN>`        | Boolean values |
| `JSON_EACH_DATE(variant)`       | `MAP<VARCHAR, DATE>`           | `DATE` values, and strings that parse as dates |
| `JSON_EACH_TIME(variant)`       | `MAP<VARCHAR, TIME>`           | `TIME` values, and strings that parse as times |
| `JSON_EACH_TIMESTAMP(variant)`  | `MAP<VARCHAR, TIMESTAMP>`      | `TIMESTAMP` values, and strings that parse as timestamps |

These functions obey the following rules:

- A field is selected based on the runtime type of its value.  With
  the exception of the date and time functions described below, values
  are never parsed from strings, and never converted to strings: a
  field holding the string `"7"` is not returned by `JSON_EACH_BIGINT`,
  and a field holding the number `7` is not returned by
  `JSON_EACH_STRING`.
- `JSON_EACH_BIGINT` does not truncate: a field holding `2.5` is not
  returned.  Such fields can be selected with
  [`VARIANT_FILTER`](#variant_filter) using a `TYPEOF` predicate,
  which keeps the values as `VARIANT`, avoiding a commitment to a
  fixed `DECIMAL` precision and scale.
- Fields holding JSON `null` values are never returned.
- Fields whose keys are not strings are never returned.
- A `VARIANT` that does not hold an object (e.g., a scalar, an array,
  or a `null`) produces an empty map.
- A SQL `NULL` argument produces a SQL `NULL` result.
- JSON has no date or time types, so `JSON_EACH_DATE`,
  `JSON_EACH_TIME`, and `JSON_EACH_TIMESTAMP` also accept string
  values, parsing them with the grammar of the corresponding SQL
  literal (e.g., `'2024-01-01'`, `'17:30:40'`, `'2024-12-19
  16:39:57'`); strings that do not parse are omitted.  Like in a
  `CAST`, a date-only string is also a valid midnight timestamp.
  Values typed as `DATE`, `TIME`, or `TIMESTAMP`, which arise from
  expressions such as `CAST(MAP['d', DATE '2024-01-01'] AS VARIANT)`,
  qualify as well.

These functions are commonly combined with [`UNNEST`](map.md#the-unnest-operator)
to produce a table with a row for each extracted field:

```sql
CREATE TABLE data(json VARIANT);

CREATE VIEW ints AS
SELECT t.k, t.v
FROM data, UNNEST(JSON_EACH_BIGINT(data.json)) AS t(k, v);
```

### `JSON_OBJECT_KEYS`

`JSON_OBJECT_KEYS(variant)` returns the top-level keys of a `VARIANT`
holding a JSON object, as a sorted `ARRAY` of strings, following the
Postgres function with the same name.  All keys are returned,
including keys whose values are JSON `null` values, nested objects, or
arrays; keys that are not strings are skipped.  A `VARIANT` that does
not hold an object produces an empty array; a SQL `NULL` argument
produces a SQL `NULL` result.

```sql
SELECT JSON_OBJECT_KEYS(PARSE_JSON('{"a": 1, "b": {"c": 2}, "d": null}'));
-- [a, b, d]
```

### `JSON_KEYS`

`JSON_KEYS(variant)` returns the keys of all nested objects in a
`VARIANT`, as dot-joined paths, deduplicated and sorted, following the
BigQuery function with the same name in its default `'strict'` mode.
Objects nested inside arrays are not traversed.  Keys that are not
strings are skipped.  A `VARIANT` that does not hold an object
produces an empty array; a SQL `NULL` argument produces a SQL `NULL`
result.  (The BigQuery `max_depth` and `mode` arguments are not
supported.)

Like the BigQuery function, `JSON_KEYS` escapes keys containing
special characters using double quotes, so paths are unambiguous: the
object `{"a.b": 1}` produces the path `"a.b"`, including the quotes,
distinct from the path `a.b` produced by `{"a": {"b": 1}}`.  The
[`VARIANT_DEEP_FILTER`](#variant_deep_filter) and
[`VARIANT_DEEP_MAP`](#variant_deep_map) functions use the same
quoting in their paths.

```sql
SELECT JSON_KEYS(PARSE_JSON('{"a": {"b": 1, "c": {"d": 2}}, "e": [{"f": 3}], "g": 4}'));
-- [a, a.b, a.c, a.c.d, e, g]
```

### `VARIANT_FILTER`

`VARIANT_FILTER(variant, (key, value) -> predicate)` keeps the parts
of a `VARIANT` for which a predicate is true.  Consider this call:

```sql
SELECT VARIANT_FILTER(
    PARSE_JSON('{"name": "Ada",
                 "age": 36,
                 "address": {"city": "Boston", "zip": "02115"},
                 "tags": [1, 2],
                 "note": null}'),
    (k, x) -> TYPEOF(x) = 'VARCHAR');
```

The predicate is called once per top-level field of the object; both
arguments are `VARIANT` values, and a field holding a nested object or
array is passed whole, as a single value:

| `k`         | `x`                                | `TYPEOF(x) = 'VARCHAR'` |
|-------------|------------------------------------|-------------------------|
| `'address'` | `{"city": "Boston", "zip": "02115"}` | `FALSE`               |
| `'age'`     | `36`                               | `FALSE`                 |
| `'name'`    | `'Ada'`                            | `TRUE`                  |
| `'note'`    | JSON `null`                        | `FALSE`                 |
| `'tags'`    | `[1, 2]`                           | `FALSE`                 |

The result is the object `{"name": "Ada"}`.  A field is kept only when
the predicate evaluates to `TRUE`.  Note that the predicate is never
called on the nested fields `city` and `zip`: the whole `address`
object is one item, kept or dropped as a unit.  Use `VARIANT_DEEP_FILTER`
if deep inspection is required.

When the variant does not hold an object, the predicate is called once,
with a `NULL` key and the value; the result is the value unchanged when
the predicate is true, and SQL `NULL` otherwise:

```sql
SELECT VARIANT_FILTER(PARSE_JSON('5'), (k, x) -> k IS NULL);
-- 5
SELECT VARIANT_FILTER(PARSE_JSON('5'), (k, x) -> k IS NOT NULL);
-- NULL

-- remove fields holding JSON nulls
SELECT VARIANT_FILTER(v, (k, x) -> x <> VARIANTNULL());

-- keep fields whose key starts with 'user'
SELECT VARIANT_FILTER(v, (k, x) -> CAST(k AS VARCHAR) LIKE 'user%');

-- keep strings that parse as dates
SELECT t.k, t.d FROM data, UNNEST(
    CAST(VARIANT_FILTER(data.json,
                        (k, v) -> TYPEOF(v) = 'VARCHAR'
                                  AND CAST(v AS DATE) IS NOT NULL)
         AS MAP<VARCHAR, DATE>)) AS t(k, d);
```

### `VARIANT_DEEP_FILTER`

`VARIANT_DEEP_FILTER(variant, (path, value) -> predicate)` is the
recursive version of `VARIANT_FILTER`.  The predicate receives the
dot-joined path of each item instead of its key; the path is a
nullable `VARCHAR`.

- fields of objects are labeled by their path, e.g. `a.b.c`;
- array elements are items too, labeled with 1-based bracket
  components, e.g. `e[1].f`; a dropped element shrinks the array;
- the predicate receives the original, unfiltered value of each item;
  dropping an item removes its whole subtree, and the predicate is not
  evaluated on the contents of a dropped container;
- a `VARIANT` holding a top-level array is filtered element-wise, with
  paths `[1]`, `[2]`, etc.;
- a scalar or JSON `null` is a single item with a `NULL` path, kept
  whole or dropped to SQL `NULL`, as in `VARIANT_FILTER`;
- fields with non-string keys are kept untouched, without invoking the
  predicate

Field names with special characters are
double-quoted in the path, and backslashes escape embedded quotes and
backslashes.  E.g.:

```json
{ "example.com": { "a": 1 }, "example": { "b": 2 } }
```

The field `a` has the path `"example.com".a`, including the quotes, so
the predicate `p LIKE 'example.%'` selects only the subtree of the key
`example`.

```sql
-- remove one subtree
SELECT VARIANT_DEEP_FILTER(PARSE_JSON('{"a": {"b": 1, "c": {"d": 2}}}'),
                           (p, x) -> p <> 'a.c');
-- {"a":{"b":1}}

-- keep only paths under 'a', at any depth
SELECT VARIANT_DEEP_FILTER(v, (p, x) -> p = 'a' OR p LIKE 'a.%');
```

### `VARIANT_MAP`

`VARIANT_MAP(variant, (key, value) -> expression)` shallowly transforms the
values of a `VARIANT` with a lambda, building a result isomorphic to
the input:

- a `VARIANT` holding an object produces an object with the same keys,
  where each value is replaced by the lambda's result;
- any other `VARIANT` is a single item with a `NULL` key; the lambda's
  result is the result of the function;
- the lambda may produce a value of any type; it is converted to a
  `VARIANT` automatically;
- a SQL `NULL` produced by the lambda becomes a JSON `null` inside an
  object;
- the function does not recurse into nested objects;
- a SQL `NULL` argument produces a SQL `NULL` result.

```sql
-- double every numeric value; non-numbers become JSON nulls
SELECT TO_JSON(VARIANT_MAP(PARSE_JSON('{"a": 1, "b": "x"}'),
                           (k, x) -> CAST(x AS BIGINT) * 2));
-- {"a":2,"b":null}

-- replace each value by its runtime type
SELECT TO_JSON(VARIANT_MAP(PARSE_JSON('{"a": 1, "b": "x"}'),
                           (k, x) -> TYPEOF(x)));
-- {"a":"BIGINT UNSIGNED","b":"VARCHAR"}
```

### `VARIANT_DEEP_MAP`

`VARIANT_DEEP_MAP(variant, (path, value) -> expression)` is the
recursive version of `VARIANT_MAP`.  The structure of nested objects
and arrays is preserved exactly; the lambda transforms only the
leaves, i.e. the values that are not objects or arrays:

- the first lambda argument is the leaf's dot-joined path, a nullable
  `VARCHAR`, with the same syntax as in `VARIANT_DEEP_FILTER`: array
  elements use 1-based bracket components, e.g. `e[1].f`;
- JSON `null` values are leaves too, and are passed to the lambda;
- the lambda may produce a value of any type; it is converted to a
  `VARIANT` automatically, and a SQL `NULL` result becomes a JSON
  `null` in place;
- a top-level scalar is a single leaf with a `NULL` path; the lambda's
  result is the result of the function;
- fields with non-string keys are kept untouched, without
  transformation or recursion;
- a SQL `NULL` argument produces a SQL `NULL` result.

:::note

The two recursive functions treat containers differently, by design:
`VARIANT_DEEP_FILTER` applies its predicate to every nested item,
including objects and arrays, since filtering decides which subtrees
survive; `VARIANT_DEEP_MAP` applies its lambda only to leaves, since
mapping transforms values while preserving the structure.

:::

```sql
-- double every number, at any depth; other leaves become JSON nulls
SELECT TO_JSON(VARIANT_DEEP_MAP(PARSE_JSON('{"a": {"b": 1}, "e": [1, 2], "s": "x"}'),
                                (p, x) -> CAST(x AS BIGINT) * 2));
-- {"a":{"b":2},"e":[2,4],"s":null}

-- redact all values under the 'user' subtree
SELECT TO_JSON(VARIANT_DEEP_MAP(PARSE_JSON('{"user": {"name": "Ada", "ssn": "123"}, "id": 7}'),
                                (p, x) -> CASE WHEN p LIKE 'user.%'
                                               THEN CAST('***' AS VARIANT)
                                               ELSE x END));
-- {"id":7,"user":{"name":"***","ssn":"***"}}
```

### `VARIANT_MERGE`

`VARIANT_MERGE(v1, v2)` merges two `VARIANT` values recursively,
following the JSON Merge Patch algorithm (RFC 7386) with one
difference: JSON `null` values are ordinary values, and never delete
fields.

- When both arguments hold objects, their fields are merged: fields
  present on only one side are kept, and fields present on both sides
  are merged recursively.
- In every other case the second argument wins: scalars, arrays, and
  mixed combinations are replaced, not combined.  In particular, two
  arrays are not concatenated.
- A JSON `null` on the right replaces the value; it does not remove
  the field.  Removing fields is
  [`VARIANT_FILTER`](#variant_filter)'s job.
- A SQL `NULL` argument produces a SQL `NULL` result.

```sql
-- fields of the second argument win on common keys, recursively:
-- "x" is overridden, "y" is kept, "z" and "c" are added
SELECT TO_JSON(VARIANT_MERGE(
    PARSE_JSON('{"a": {"x": 1, "y": 2}, "b": 1}'),
    PARSE_JSON('{"a": {"x": 9, "z": 3}, "c": 4}')));
-- {"a":{"x":9,"y":2,"z":3},"b":1,"c":4}

-- arrays are replaced, not concatenated
SELECT TO_JSON(VARIANT_MERGE(
    PARSE_JSON('{"tags": [1, 2]}'),
    PARSE_JSON('{"tags": [3]}')));
-- {"tags":[3]}

-- a JSON null is an ordinary value; it does not remove the field
SELECT TO_JSON(VARIANT_MERGE(
    PARSE_JSON('{"a": 1, "b": 2}'),
    PARSE_JSON('{"a": null}')));
-- {"a":null,"b":2}

-- inserting a field is a merge with a one-field object
SELECT TO_JSON(VARIANT_MERGE(
    PARSE_JSON('{"a": 1}'),
    PARSE_JSON('{"new": 5}')));
-- {"a":1,"new":5}
```

When the inserted value is computed rather than constant, build the
one-field object with the `MAP` constructor instead of `PARSE_JSON`:
`VARIANT_MERGE(v, CAST(MAP['new', CAST(x AS VARIANT)] AS VARIANT))`.

## Processing JSON data using `VARIANT`

The `VARIANT` type enables efficient JSON processing in SQL.  In this sense it is similar to
the `JSONB` type in Postgres and other databases.  There are two ways to convert
JSON data to and from `VARIANT`:

1. Use `PARSE_JSON` and `TO_JSON` functions to convert strings to `VARIANT` and back.
2. Automatically, when ingesting data to or outputting data from columns of type `VARIANT`.

The following example demonstrates the first approach. Here, input events
contain a field called `json` of type string, which carries JSON-encoded data.
We ingest this field as a string and use `PARSE_JSON` to convert it to a
`VARIANT` and store the result in an intermediate view.

```sql
CREATE TABLE json (id INT, json VARCHAR);
CREATE VIEW parsed_json AS SELECT id, PARSE_JSON(json) AS json FROM json;
```

Input events can use any [supported data format](/formats/).  For instance, when
ingesting a [JSON stream](/formats/json), a valid input record could look like this
(note the use of escaping in the `json` field):

```json
{"id": 123, "json": "{\"foo\": \"bar\"}"}
```

The second approach parses JSON into `VARIANT` directly during ingestion, eliminating
the need for calling `PARSE_JSON` explicitly:

```sql
CREATE TABLE json (id INT, json VARIANT);
```

**Note** that this program has a subtly different semantics from the previous one
depending on the input [format](/formats/) used.  For most input formats, e.g.,
[Avro](/formats/avro), [Parquet](/formats/parquet), or [CSV](/formats/csv),
it is equivalent, i.e., it converts an input field of type string into a `VARIANT`.
However, when the input stream carries JSON data using [raw](/formats/json#the-raw-format)
or [insert/delete](/formats/json#the-insertdelete-format) encoding, the `json` field can contain
an arbitrary JSON value, which gets parsed into `VARIANT`:

```json
{"id": 123, "json": {"name": "John Doe", "scores": [8, 10]}}
```

This is useful for processing  **semi-structured** data, i.e., data whose schema is only
partially fixed or is too complex to represent in SQL.
In this case, the schema contains an integer field `id` and a field called `json`, whose
schema is not specified. The `VARIANT` type allows us to parse this field and
manipulate its contents.  For instance, the following query
extracts `name` and `scores` fields, interprets the latter as an array of numbers
and computes the average of the first two entries in the array:

```sql
CREATE TABLE json (id INT, json VARIANT);

CREATE VIEW average AS SELECT
CAST(json['name'] AS VARCHAR) AS name,
((CAST(json['scores'][1] AS DECIMAL(8, 2)) + CAST(json['scores'][2] AS DECIMAL(8, 2))) / 2) AS average
FROM json;
```

Note how object fields are accessed using
map indexing operators `['scores']`, `['name']`, and how array
elements are accessed using indexing with numeric values `[1]`.
Recall that array indexes in SQL start from 1!

Finally, notice how the `DECIMAL` values that are retrieved need to
specify the precision and scale: `CAST(... AS DECIMAL(8, 2))`.  Using
`CAST(... AS DECIMAL)` would lose all digits after the decimal point.

## Examples

Here are some simple SQL query examples using `VARIANT` and JSON
values and the expected output values.  (Note that these examples
cannot be executed directly, since they involve no views.)

```sql
SELECT CAST(1 AS VARIANT)
1

SELECT TYPEOF(CAST(1 AS VARIANT))
INTEGER

SELECT CAST(CAST(1 AS TINYINT) AS VARIANT)
1

-- The runtime knows that this is a TINYINT
SELECT TYPEOF(CAST(CAST(1 AS TINYINT) AS VARIANT))
TINYINT

-- Converting something to VARIANT and back works
SELECT CAST(CAST(1 AS VARIANT) AS INT)
1

-- Conversions between numeric types are allowed
SELECT CAST(CAST(1 AS VARIANT) AS TINYINT)
1

-- Some VARIANT objects when output receive double quotes
SELECT CAST('string' AS VARIANT)
"string"

-- CHAR(3) values are represented as VARCHAR in variants
SELECT CAST(CAST('abc' AS VARIANT) AS VARCHAR)
abc

-- VARCHAR and CHAR(N) have the same underlying runtime type
SELECT CAST(CAST('abc' AS VARIANT) AS CHAR(3))
abc

-- The value representing a VARIANT null value (think of a JSON null)
SELECT VARIANTNULL()
NULL

-- VARIANT null is not the same as SQL NULL
SELECT VARIANTNULL() IS NULL
FALSE

-- Two VARIANT nulls are equal, unlike SQL NULL
SELECT VARIANTNULL() = VARIANTNULL()
TRUE

SELECT TYPEOF(VARIANTNULL())
VARIANT

-- Variants delegate equality to the underlying values
SELECT CAST(1 AS VARIANT) = CAST(1 AS VARIANT)
TRUE

-- To be equal two variants must have the same value and the same runtime type
SELECT CAST(1 AS VARIANT) = CAST(CAST(1 AS TINYINT) AS VARIANT)
FALSE

-- An array of variant values can have values with any underlying type
SELECT ARRAY[CAST(1 AS VARIANT), CAST('abc' AS VARIANT)]
[1, "abc"]

-- A map with VARCHAR keys and VARIANT values
SELECT MAP['a', CAST(1 AS VARIANT), 'b', CAST('abc' AS VARIANT), 'c', CAST(ARRAY[1,2,3] AS VARIANT)]
{a=1, b="abc", c=[1, 2, 3]}

-- Variant values allow access by index, but return null if they are not arrays
SELECT (CAST(1 AS VARIANT))[1]
NULL

SELECT CAST(ARRAY[1,2,3] AS VARIANT)[1]
1

-- Accessing items in a VARIANT array returns VARIANT values,
-- even if the array itself does not contain VARIANT values
-- (Otherwise TYPEOF would not compile)
SELECT TYPEOF(CAST(ARRAY[1,2,3] AS VARIANT)[1])
INTEGER

SELECT CAST(x'0102' AS VARIANT)
x'0102'

SELECT CAST(CAST(x'0102' AS VARBINARY) AS VARIANT)
x'0102'

SELECT CAST(TIME '10:01:01' AS VARIANT)
10:01:01

-- One can access fields by name in a VARIANT, even if the
-- variant does not have named fields
SELECT CAST(ARRAY[1,2,3] AS VARIANT)['name']
NULL

-- One can access fields by name in a VARIANT, even if the
-- variant does not have named fields
SELECT CAST(ARRAY[1,2,3] AS VARIANT)."name"
NULL

-- One can access fields by index in a VARIANT
SELECT CAST(MAP[1,'a',2,'b',3,'c'] AS VARIANT)[1]
"a"

SELECT TYPEOF(CAST(MAP[1,'a',2,'b',3,'c'] AS VARIANT)[1])
VARCHAR

-- Note that field name is quoted to match the case of the key
SELECT CAST(MAP['a',1,'b',2,'c',3] AS VARIANT)."a"
1

-- Unquoted uppercase field name does not match
SELECT CAST(MAP['A',1,'b',2,'c',3] AS VARIANT).A
NULL

-- The safest way is to index with a string
SELECT CAST(MAP['a',1,'b',2,'c',3] AS VARIANT)['a']
1

-- Maps can have variant keys too
-- (but you have to index with a variant).
SELECT (MAP[CAST('a' AS VARIANT), 1, CAST(1 AS VARIANT), 2])[CAST(1 AS VARIANT)]
2

-- Navigating a JSON-like object
SELECT CAST(MAP['a', CAST(1 AS VARIANT), 'b', CAST('abc' AS VARIANT), 'c', CAST(ARRAY[1,2,3] AS VARIANT)]
               ['c'][1] AS INTEGER)
1

SELECT PARSE_JSON(1)
1

SELECT PARSE_JSON('1')
1

-- Integer values in JSON are stored as BIGINT or BIGINT UNSIGNED values
SELECT TYPEOF(PARSE_JSON('1'))
BIGINT UNSIGNED

SELECT PARSE_JSON('"a"')
"a"

SELECT PARSE_JSON('false')
FALSE

-- A VARIANT null
SELECT PARSE_JSON('null')
NULL

SELECT TYPEOF(PARSE_JSON('null'))
VARIANT

-- a SQL null
SELECT PARSE_JSON(NULL)
NULL


SELECT PARSE_JSON('[1,2,3]')
[1, 2, 3]

SELECT PARSE_JSON('{"a": 1, "b": 2}')
{"a"=1,"b"=2}

-- all the following are strings
SELECT TO_JSON(PARSE_JSON(1))
1

SELECT TO_JSON(NULL)
NULL

SELECT TO_JSON(PARSE_JSON('1'))
1

SELECT TO_JSON(PARSE_JSON('"a"'))
"a"

SELECT TO_JSON(PARSE_JSON('false'))
FALSE

SELECT TO_JSON(PARSE_JSON('null'))
NULL

SELECT TO_JSON(PARSE_JSON(NULL))
NULL

SELECT TO_JSON(PARSE_JSON('[1,2,3]'))
[1,2,3]

SELECT TO_JSON(PARSE_JSON('{ "a": 1, "b": 2 }'))
{"a":1,"b":2}

SELECT PARSE_JSON('{ "a": 1, "b": 2 }') = PARSE_JSON('{"b":2,"a":1}')
TRUE

-- dates are emitted as strings
SELECT TO_JSON(CAST(DATE '2020-01-01' AS VARIANT))
"2020-01-01"

-- timestamps are emitted as strings
SELECT TO_JSON(CAST(TIMESTAMP '2020-01-01 10:00:00' AS VARIANT))
"2020-01-01 10:00:00"

-- values with user-defined types can be converted to JSON
CREATE TYPE s AS (i INT, s VARCHAR, a INT ARRAY);
SELECT TO_JSON(CAST(s(2, 'a', ARRAY[1, 2, 3]) AS VARIANT));
{"a":[1,2,3],"i":2,"s":"a"}

-- The result of JSON parsing can be converted to user-defined types
SELECT CAST(PARSE_JSON('{"i": 2, "s": "a", "a": [1, 2, 3]}') AS s);
{a=[1,2,3], i=2, s="a"}

-- This works even for nested types, such as user-defined types that
-- contain arrays of user-defined types
CREATE TYPE t AS (sa s ARRAY);
SELECT TO_JSON(CAST(t(ARRAY[s(2, 'a', ARRAY[1, NULL, 3]), s(3, 'b', ARRAY())]) AS VARIANT));
{"sa":[{"a":[1,NULL,3],"i":2,"s":"a"},{"a":[],"i":3,"s":"b"}]}

SELECT CAST(CAST(MAP['i', 0] AS VARIANT) AS s)
-- produces a structure S(I=0, A=NULL, S=NULL); missing fields are set to 'NULL'

SELECT CAST(CAST(MAP['i', 's'] AS VARIANT) AS s)
-- produces a structure S(I=NULL, A=NULL, S=NULL), since the field 'I' has the wrong type

SELECT CAST(CAST(MAP['I', 's'] AS VARIANT) AS s)
-- produces a structure S(I=NULL, A=NULL, S=NULL), since the field 'i' is uppercase
-- yet unquoted field names are converted to lowercase

SELECT CAST(CAST(MAP['i', 0, 'X', 2] AS VARIANT) AS s)
-- produces a structure S(I=0, A=NULL, S=NULL), since the extra field 'X' in the map is ignored

SELECT CAST(PARSE_JSON('{"sa": [{"i": 2, "s": "a", "a": [1, 2, 3]}]}') AS t)
-- produces a structure T(sa=[i=2, s="a", "a"={1,2,3}])
```
