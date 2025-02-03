# Dynamically-Typed Values and JSON Support

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
Feldera compiler following mechanism:

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

This program is more efficient than the previous one, but achieves the
almost the same effect.

There is a subtle difference between the two programs: the former
program, using `VARIANT` requires the case fields in the JSON to match
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
used as an index into the map.  If a field is missing in the map and
the corresponding field of the struct gets the `NULL` value.  Fields
in the map that do not correspond to the struct field names are
ignored.  The user-defined structure field names are case-sensitive,
according to the compiler flag `--unquotedCasing`.

:::note

Remember that the `DECIMAL` type specified without precision is the
same as `DECIMAL(0)`, with no digits after the decimal point.  When
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
  that has en element with this key, a `VARIANT` value holding the associated value in
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

### `PARSE_JSON`

`PARSE_JSON` converts a JSON value as follows:

- JSON `null` is converted to a `VARIANT` `null` value (not a SQL `NULL`!); see above the description of `VARIANT` `null`
- JSON Booleans are converted to `BOOLEAN` values (wrapped in `VARIANT` values)
- JSON numbers are converted to `DECIMAL` values (wrapped in `VARIANT` values)
- JSON strings are converted to `VARCHAR` values (wrapped in `VARIANT` values)
- JSON arrays are converted to `VARIANT ARRAY` values (wrapped in `VARIANT` values).  Each array element is a `VARIANT`
- JSON objects are converted to `MAP<VARIANT, VARIANT>` values (wrapped in `VARIANT` values).  Each key and each value is a `VARIANT`

For example, `PARSE_JSON("{"a": 1, "b": [2, 3.3, null]}")` generates the same SQL value that would be generated by the following code:

```sql
SELECT CAST(
   MAP[
      CAST('a' AS VARIANT), CAST(1.0 AS VARIANT),
      CAST('b' AS VARIANT), CAST(ARRAY[
          CAST(2.0 AS VARIANT),
          CAST(3.3 AS VARIANT),
          VARIANTNULL()
                                      ] AS VARIANT)
      ] AS VARIANT)
```

### `TO_JSON`

`TO_JSON` converts a `VARIANT` value to a `VARCHAR`:

- the `VARIANT` `null` value is converted to the string `null`
- a `VARIANT` wrapping a Boolean value is converted to the respective Boolean string `true` or `false`
- a `VARIANT` wrapping any numeric value (`DECIMAL`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `REAL`, `DOUBLE`, `DECIMAL`) is converted to the string representation of the value as produced using a `CAST(value AS VARCHAR)`
- a `VARIANT` wrapping a `VARCHAR` value is converted to a string with double quotes, and with escape sequences for special characters (e.g., quotes), as mandated by the JSON grammar
- a `VARIANT` wrapping an `ARRAY` with elements of any type is converted to a JSON array, and the elements are recursively converted
- a `VARIANT` wrapping a `MAP` whose keys have any SQL `CHAR` type, or `VARIANT` values wrapping `CHAR` values will generate a JSON object, by recursively converting each key-value pair.
- a `VARIANT` wrapping a `DATE`, `TIME`, or `DATETIME` value will be serialized as a JSON string

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
extracts `name` and `scores` fields, interpets the latter as an array of numbers
and computes the average of the first two entries in the array:

```sql
CREATE TABLE json (id INT, json VARIANT);

CREATE VIEW average AS SELECT
CAST(json['name'] AS VARCHAR) as name,
((CAST(json['scores'][1] AS DECIMAL(8, 2)) + CAST(json['scores'][2] AS DECIMAL(8, 2))) / 2) as average
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
select CAST('string' as VARIANT)
"string"

-- CHAR(3) values are represented as VARCHAR in variants
SELECT CAST(CAST('abc' AS VARIANT) AS VARCHAR)
abc

-- VARCHAR and CHAR(N) have the same underlying runtime type
SELECT CAST(CAST('abc' AS VARIANT) AS CHAR(3))
abc

-- The value representing a VARIANT null value (think of a JSON null)
SELECT VARIANTNULL()
null

-- VARIANT null is not the same as SQL NULL
SELECT VARIANTNULL() IS NULL
false

-- Two VARIANT nulls are equal, unlike SQL NULL
SELECT VARIANTNULL() = VARIANTNULL()
true

SELECT TYPEOF(VARIANTNULL())
VARIANT

-- Variants delegate equality to the underlying values
SELECT CAST(1 AS VARIANT) = CAST(1 AS VARIANT)
true

-- To be equal two variants must have the same value and the same runtime type
SELECT CAST(1 AS VARIANT) = CAST(CAST(1 AS TINYINT) AS VARIANT)
false

-- An array of variant values can have values with any underlying type
SELECT ARRAY[CAST(1 AS VARIANT), CAST('abc' AS VARIANT)]
[1, "abc"]

-- A map with VARCHAR keys and VARIANT values
SELECT MAP['a', CAST(1 AS VARIANT), 'b', CAST('abc' AS VARIANT), 'c', CAST(ARRAY[1,2,3] AS VARIANT)]
{a=1, b="abc", c=[1, 2, 3]}

-- Variant values allow access by index, but return null if they are not arrays
SELECT (CAST(1 AS VARIANT))[1]
null

SELECT CAST(ARRAY[1,2,3] AS VARIANT)[1]
1

-- Accessing items in a VARIANT array returns VARIANT values,
-- even if the array itself does not contain VARIANT values
-- (Otherwise TYPEOF would not compile)
SELECT TYPEOF(CAST(ARRAY[1,2,3] AS VARIANT)[1])
INTEGER

SELECT CAST(x'0102' AS VARIANT)
x'0102;

SELECT CAST(CAST(x'0102' AS VARBINARY) AS VARIANT)
x'0102;

SELECT CAST(TIME '10:01:01' AS VARIANT)
10:01:01

-- One can access fields by name in a VARIANT, even if the
-- variant does not have named fields
SELECT CAST(ARRAY[1,2,3] AS VARIANT)['name']
null

-- One can access fields by name in a VARIANT, even if the
-- variant does not have named fields
SELECT CAST(ARRAY[1,2,3] AS VARIANT)."name"
null

-- One can access fields by index in a VARIANT
SELECT CAST(Map[1,'a',2,'b',3,'c'] AS VARIANT)[1]
"a"

SELECT TYPEOF(CAST(Map[1,'a',2,'b',3,'c'] AS VARIANT)[1])
VARCHAR

-- Note that field name is quoted to match the case of the key
SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT)."a"
1

-- Unquoted field may not match, depending on the 'unquotedCasing' compiler flag
SELECT CAST(Map['A',1,'b',2,'c',3] AS VARIANT).A
NULL

-- The safest way is to index with a string
SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT)['a']
1

-- Maps can have variant keys too
-- (but you have to index with a variant).
SELECT (Map[CAST('a' AS VARIANT), 1, CAST(1 AS VARIANT), 2])[CAST(1 AS VARIANT)]
2

-- Navigating a JSON-like object
SELECT CAST(MAP['a', CAST(1 AS VARIANT), 'b', CAST('abc' AS VARIANT), 'c', CAST(ARRAY[1,2,3] AS VARIANT)]
               ['c'][1] AS INTEGER)
1

SELECT PARSE_JSON(1)
1

SELECT PARSE_JSON('1')
1

-- Numeric values in JSON are stored as DECIMAL values
SELECT TYPEOF(PARSE_JSON('1'))
DECIMAL

SELECT PARSE_JSON('\"a\"')
"a"

SELECT PARSE_JSON('false')
false

-- A VARIANT null
SELECT PARSE_JSON('null')
null

SELECT TYPEOF(PARSE_JSON('null'))
VARIANT

-- a SQL null
SELECT PARSE_JSON(null)
NULL


SELECT PARSE_JSON('[1,2,3]')
[1, 2, 3]

SELECT PARSE_JSON('{\"a\": 1, \"b\": 2}')
{"a"=1,"b"=2}

-- all the following are strings
SELECT TO_JSON(PARSE_JSON(1))
1

SELECT TO_JSON(null)
NULL

SELECT TO_JSON(PARSE_JSON('1'))
1

SELECT TO_JSON(PARSE_JSON('\"a\"'))
a

SELECT TO_JSON(PARSE_JSON('false'))
false

SELECT TO_JSON(PARSE_JSON('null'))
NULL

SELECT TO_JSON(PARSE_JSON(null))
null

SELECT TO_JSON(PARSE_JSON('[1,2,3]'))
[1,2,3]

SELECT TO_JSON(PARSE_JSON('{ \"a\": 1, \"b\": 2 }'))
{\"a\":1,\"b\":2}

SELECT PARSE_JSON('{ \"a\": 1, \"b\": 2 }') = PARSE_JSON('{\"b\":2,\"a\":1}')
true

-- dates are emitted as strings
SELECT TO_JSON(CAST(DATE '2020-01-01' AS VARIANT))
"2020-01-01"

-- timestamps are emitted as strings
SELECT TO_JSON(CAST(TIMESTAMP '2020-01-01 10:00:00' AS VARIANT))
"2020-01-01 10:00:00"

-- values with user-defined types can be converted to JSON
CREATE TYPE S AS (i INT, s VARCHAR, a INT ARRAY);
SELECT TO_JSON(CAST(s(2, 'a', ARRAY[1, 2, 3]) AS VARIANT));
{"a":[1,2,3],"i":2,"s":"a"}

-- The result of JSON parsing can be converted to user-defined types
SELECT CAST(PARSE_JSON('{"i": 2, "s": "a", "a": [1, 2, 3]}') AS S);
{a=[1,2,3], i=2, s="a"}

-- This works even for nested types, such as user-defined types that
-- contain arrays of user-defined types
CREATE TYPE t AS (sa S ARRAY);
SELECT TO_JSON(CAST(t(ARRAY[s(2, 'a', ARRAY[1, NULL, 3]), s(3, 'b', array())]) AS VARIANT));
{"SA":[{"a":[1,null,3],"i":2,"s":"a"},{"a":[],"i":3,"s":"b"}]}

SELECT CAST(CAST(MAP['i', 0] AS VARIANT) AS S)
-- produces a structure S(I=0, A=NULL, S=NULL); missing fields are set to 'NULL'

SELECT CAST(CAST(MAP['i', 's'] AS VARIANT) AS S)
-- produces a structure S(I=NULL, A=NULL, S=NULL), since the field 'I' has the wrong type

SELECT CAST(CAST(MAP['I', 's'] AS VARIANT) AS S)
-- produces a structure S(I=NULL, A=NULL, S=NULL), since the field 'i' is uppercase
-- yet unquoted field names are converted to lowercase

SELECT CAST(CAST(MAP['i', 0, 'X', 2] AS VARIANT) AS S)
-- produces a structure S(I=NULL, A=NULL, S=NULL), since the extra field 'X' in the map is ignored

SELECT CAST(PARSE_JSON('{"sa": [{"i": 2, "s": "a", "a": [1, 2, 3]}]}') AS T)
-- produces a structure T(sa=[i=2, s="a", "a"={1,2,3}])
```
