# Dynamically-typed values and JSON support

## The `VARIANT` type

Values of `VARIANT` type are dynamically-typed.
Any such value holds at runtime two pieces of information:
- the data type
- the data value

Values of `VARIANT` type can be created by casting any other value to a `VARIANT`: e.g.
`SELECT CAST(x AS VARIANT)`.  Conversely, values of type `VARIANT` can be cast to any other data type
`SELECT CAST(variant AS INT)`.  A cast of a value of type `VARIANT` to target type T
will compare the runtime type with T.  If the types are identical, the
original value is returned.  Otherwise the `CAST` returns `NULL`.

Values of type `ARRAY`, `MAP`, and `ROW` type can be cast to `VARIANT`.  `VARIANT` values
also offer the following operations:

- indexing using array indexing notation `variant[index]`.  If the `VARIANT` is
  obtained from an `ARRAY` value, the indexing operation returns a `VARIANT` whose value element
  is the element at the specified index.  Otherwise, this operation returns `NULL`
- indexing using map element access notation `variant[key]`, where `key` can have
  any legal `MAP` key type.  If the `VARIANT` is obtained from a `MAP` value
  that has en element with this key, a `VARIANT` value holding the associated value in
  the `MAP` is returned.  Otherwise `NULL` is returned.  If the `VARIANT` is obtained from `ROW` value
  which has a field with the name `key`, this operation returns a `VARIANT` value holding
  the corresponding field value.  Otherwise `NULL` is returned.
- field access using the dot notation: `variant.field`.  This operation is interpreted
  as equivalent to `variant['field']`.  Note, however, that the field notation
  is subject to the capitalization rules of the SQL dialect, so for correct
  operation the field may need to be quoted: `variant."field"`

The runtime types do not need to match exactly the compile-time types.
As a compiler front-end, Calcite does not mandate exactly how the runtime types
are represented.  Calcite does include one particular implementation in
Java runtime, which is used for testing.  In this representation
the runtime types are represented as follows:

- The scalar types do not include information about precision and scale.  Thus all `DECIMAL`
  compile-time types are represented by a single run-time type.
- `CHAR(N)` and `VARCHAR` are both represented by a single runtime `VARCHAR` type.
- `BINARY(N)` and `VARBINARY` are both represented by a single runtime `VARBINARY` type.
- `FLOAT` and `DOUBLE` are both represented by the same runtime type.
- All "short interval" types (from days to seconds) are represented by a single type.
- All "long interval" types (from years to months) are represented by a single type.
- Generic types such as `INT ARRAY`, `MULTISET`, and `MAP` do carry runtime
  information about the element types
- The `ROW` type does have information about all field types (currently not yet supported)

## Functions that operate on `VARIANT` values

| Function | Description |
| `VARIANTNULL()` | Can be used to create an instance of the `VARIANT` `null` value. |
| `TYPEOF(` _variant_ `)` | Argument must be a `VARIANT` value.  Returns the runtime type of the value |
| `PARSE_JSON(` _string_ `)` | Parses a string that represents a JSON value, returns a `VARIANT` object, or `NULL` if parsing fails |
| `UNPARSE_JSON(` _variant_ `)` | Returns a string that represents the serialization of a `VARIANT` value. If the value cannot be represented as JSON, the result is `NULL` |

Here are some examples using `VARIANT` and `JSON` values:

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

-- However, you have to use the right type, or you get NULL
SELECT CAST(CAST(1 AS VARIANT) AS TINYINT)
null

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

-- Acessing items in a VARIANT array returns VARIANT values,
-- even if the array itself does not contain VARIANT values
-- (Otherwise TYPEOF would not compile)
SELECT TYPEOF(CAST(ARRAY[1,2,3] AS VARIANT)[1])
INTEGER

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

-- Unquoted field may not match, depending on dialect
SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT).a
null

-- The safest way is to use an index
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
SELECT UNPARSE_JSON(PARSE_JSON(1))
1

SELECT UNPARSE_JSON(null)
NULL

SELECT UNPARSE_JSON(PARSE_JSON('1'))
1

SELECT UNPARSE_JSON(PARSE_JSON('\"a\"'))
a

SELECT UNPARSE_JSON(PARSE_JSON('false'))
false

SELECT UNPARSE_JSON(PARSE_JSON('null'))
NULL

SELECT UNPARSE_JSON(PARSE_JSON(null))
null

SELECT UNPARSE_JSON(PARSE_JSON('[1,2,3]'))
[1, 2, 3]

SELECT UNPARSE_JSON(PARSE_JSON('{ \"a\": 1, \"b\": 2 }'))
{\"a\": 1, \"b\": 2}

SELECT PARSE_JSON('{ \"a\": 1, \"b\": 2 }') = PARSE_JSON('{\"b\":2,\"a\":1}')
true

-- JSON cannot contain dates, result is NULL
SELECT UNPARSE_JSON(CAST(DATE '2020-01-01' AS VARIANT))
NULL