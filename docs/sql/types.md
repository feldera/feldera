# Supported Data Types

The compiler supports the following SQL data types:

| Name                        | Description                                                                                                                                                        | Aliases                    |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOLEAN`                   | A boolean value                                                                                                                                                    | `BOOL`                     |
| `TINYINT`                   | 8-bit signed integer using two's complement.                                                                                                                       |                            |
| `SMALLINT`                  | 16-bit signed integer using two's complement.                                                                                                                      | `INT2`                     |
| `INTEGER`                   | 32-bit signed integer using two's complement.                                                                                                                      | `INT`, `SIGNED`, `INT4`    | 
| `BIGINT`                    | 64-bit signed integer using two's complement.                                                                                                                      | `INT8`, `INT64`            |
| `DECIMAL(precision, scale)` | A high precision fixed-point type, with a precision (total number of decimal digits) and a scale (number of decimal digits after period).                          | `DEC`, `NUMERIC`, `NUMBER` |
| `REAL`                      | IEEE 32-bit floating point number                                                                                                                                  | `FLOAT4`, `FLOAT32`        |
| `DOUBLE`                    | IEEE 64-bit floating point number                                                                                                                                  | `FLOAT8`, `FLOAT64`        |
| `VARCHAR(n)`                | A string value with maximum fixed width. Trailing spaces are removed when converting a value to this type.                                                         | `CHARACTER VARYING(n)`     |
| `CHAR(n)`                   | A string value with a fixed width. Values are truncated if longer, or padded with spaces if shorter, to be brought to the specified size.                          | `CHARACTER(n)`             |
| `VARCHAR`                   | A string of unlimited length. Trailing spaces are removed when converting a `CHAR(n)` value to this type.                                                          | `STRING`, `TEXT`           |
| `BINARY(n)`                 | A byte string with a fixed width.                                                                                                                                  |                            |
| `VARBINARY`                 | A byte string with an unlimited width.                                                                                                                             | `BYTEA`                    |
| `NULL`                      | A type comprising only the `NULL` value.                                                                                                                           |                            |
| `INTERVAL`                  | A SQL interval. Two types of intervals are supported: long intervals (comprising years and months), and short intervals, comprising days, hours, minutes, seconds. |                            |
| `TIME`                      | A time of the day.                                                                                                                                                 |                            |
| `TIMESTAMP`                 | A value containing a date and a time without a timezone.                                                                                                           | `DATETIME`                 |
| `DATE`                      | A date value.                                                                                                                                                      |                            |
| `GEOMETRY`                  | A geographic data type (only rudimentary support at this point).                                                                                                   |                            |
| `ARRAY`                     | An array with element of the specified type. Used as a suffix for another type (e.g., `INT ARRAY`)                                                                 |                            |

- For `DECIMAL` types: 23.456 has a precision of 5 and a scale of 3.
  If scale is missing it is assumed to be 0.

- A suffix of `NULL` or `NOT NULL` can be appended to a type name to
  indicate the nullability. A type with no suffix is not nullable by
  default.

- The `FLOAT` type is not supported. Please use `REAL` or
  `DOUBLE` instead. Various SQL dialects do not agree on the size of
  the `FLOAT` type, so we have decided to prohibit its use to avoid
  subtle bugs.

- `INTERAL` or `NULL` types are not supported in table schemas as field types. They
  are only used in expressions.

## Computations on nullable types

A type is nullable if it can represent the `NULL` value. For input
tables the nullability of a column is declared explicitly. For
intermediate results and output views the compiler infers the
nullability of each column using type inference rules.

Most SQL operations are defined for nullable types. Our compiler
follows the SQL standard in this respect. Most operations (e.g.,
`+`), when applied a `NULL` operand will produce a `NULL`
value.

## Grammar for specifying types

```
type:
      typeName
      [ collectionsTypeName ]*

typeName:
      sqlTypeName
  |   compoundIdentifier

sqlTypeName:
      char [ precision ] [ charSet ]
  |   varchar [ precision ] [ charSet ]
  |   DATE
  |   time
  |   timestamp
  |   GEOMETRY
  |   decimal [ precision [, scale] ]
  |   BOOLEAN
  |   integer
  |   BINARY [ precision ]
  |   varbinary [ precision ]
  |   TINYINT
  |   SMALLINT
  |   BIGINT
  |   REAL
  |   double

collectionsTypeName:
      ARRAY

char:
      CHARACTER | CHAR

varchar:
      char VARYING | VARCHAR

decimal:
      DECIMAL | DEC | NUMERIC | NUMBER

integer:
      INTEGER | INT

varbinary:
      BINARY VARYING | VARBINARY

double:
      DOUBLE [ PRECISION ]

time:
      TIME [ precision ] [ timeZone ]

timestamp:
      TIMESTAMP [ precision ] [ timeZone ]

charSet:
      CHARACTER SET charSetName

timeZone:
      WITHOUT TIME ZONE
```

A `compoundIdentifier` is a sequence of identifiers separated by dots.
