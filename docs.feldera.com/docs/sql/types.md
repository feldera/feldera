# Data Types

The compiler supports the following SQL data types:

| Name                        | Description                                                                                                                                                        | Aliases                    |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOLEAN`                   | A boolean value                                                                                                                                                    | `BOOL`                     |
| `TINYINT`                   | 8-bit signed integer using two's complement.                                                                                                                       |                            |
| `SMALLINT`                  | 16-bit signed integer using two's complement.                                                                                                                      | `INT2`                     |
| `INTEGER`                   | 32-bit signed integer using two's complement.                                                                                                                      | `INT`, `SIGNED`, `INT4`    |
| `BIGINT`                    | 64-bit signed integer using two's complement.                                                                                                                      | `INT8`, `INT64`            |
| `TINYINT UNSIGNED`          | 8-bit unsigned integer                     .                                                                                                                       |                            |
| `SMALLINT UNSIGNED`         | 16-bit unsigned integer.                                                                                                                                           |                            |
| `INTEGER UNSIGNED`          | 32-bit unsigned integer.                                                                                                                                           | `INT UNSIGNED`, `UNSIGNED` |
| `BIGINT UNSIGNED`           | 64-bit unsigned integer.                                                                                                                                           |                            |
| `DECIMAL(precision, scale)` | A high precision fixed-point type, with a precision (total number of decimal digits) and a scale (number of decimal digits after period).                          | `DEC`, `NUMERIC`, `NUMBER` |
| `REAL`                      | IEEE 32-bit floating point number                                                                                                                                  | `FLOAT4`, `FLOAT32`        |
| `DOUBLE`                    | IEEE 64-bit floating point number                                                                                                                                  | `FLOAT8`, `FLOAT64`        |
| `VARCHAR`                   | A string of unlimited length. Trailing spaces are removed when converting a `CHAR(n)` value to this type. This is the preferred string type.                       | `STRING`, `TEXT`           |
| `VARCHAR(n)`                | A string that holds at most `n` Unicode characters. Trailing spaces are removed when converting a `CHAR(n)` value to this type.                                    | `CHARACTER VARYING(n)`     |
| `CHAR(n)`                   | A string value that holds exactly `n` Unicode characters. Values are truncated if longer, or padded with spaces if shorter, to be brought to the specified size. We recommend against using CHAR(n) columns; see [below](#string-types)  | `CHARACTER(n)`             |
| `BINARY(n)`                 | A byte string with a fixed width; n is the number of bytes.                                                                                                        |                            |
| `VARBINARY`                 | A byte string with an unlimited width.                                                                                                                             | `BYTEA`                    |
| `NULL`                      | A type comprising only the `NULL` value.                                                                                                                           |                            |
| `INTERVAL`                  | A SQL interval. Two types of intervals are supported: long intervals (comprising years and months), and short intervals, comprising days, hours, minutes, seconds. |                            |
| `TIME`                      | A time of the day.                                                                                                                                                 |                            |
| `TIMESTAMP`                 | A value containing a date and a time without a timezone.                                                                                                           | `DATETIME`                 |
| `DATE`                      | A date value.                                                                                                                                                      |                            |
| `GEOMETRY`                  | A geographic data type (only rudimentary support at this point).                                                                                                   |                            |
| `ROW`                       | A tuple (anonymous struct with named fields) with 1 or more elements.  Example `ROW(left int null, right varchar)`                                                 |                            |
| `ARRAY`                     | An array with element of the specified type. Used as a suffix for another type (e.g., `INT ARRAY`)                                                                 |                            |
| `MAP`                       | A map with keys and values of specified types. The syntax is `MAP<KEYTYPE, VALUETYPE>`                                                                             |                            |
| `UUID`                      | An 128 bit unique identifier                                                                                                                                       |                            |
| `VARIANT`                   | A dynamically-typed value that can wrap any other SQL type                                                                                                         |                            |

- For `DECIMAL` types: 23.456 has a precision of 5 and a scale of 3.
  If scale is missing it is assumed to be 0.

- A suffix of `NULL` or `NOT NULL` can be appended to a type name to
  indicate the nullability. A type with no suffix is not nullable by
  default.  These suffixes do not work for types of elements of ARRAYs
  or MAPs.  ARRAY elements and MAP values are always nullable, while
  MAP keys are never nullable.

- The `FLOAT` type is not supported. Please use `REAL` or
  `DOUBLE` instead. Various SQL dialects do not agree on the size of
  the `FLOAT` type, so we have decided to prohibit its use to avoid
  subtle bugs.

- `INTERVAL`, `NULL` and types are currently not supported in table
  schemas or as types for the columns of output views (non-`LOCAL`
  views).

- `VARIANT` is used to implement JSON.  See [JSON support](json.md)

- <a id="row_constructor"></a>Values of type `ROW` can be constructed
  using the `ROW(x, y, z)` syntax, or, when not ambiguous, using the
  tuple syntax `(x, y, z)`, where `x`, `y`, and `z` are expressions.
  E.g. `SELECT x, (y, z+2) FROM T`, is equivalent to `SELECT x, ROW(y,
  z+2) FROM T`.

- Arithmetic operations combining signed and unsigned values will
  produce a result with the wider type; if both types have the same
  width, the result is unsigned;

## Computations on nullable types

A type is nullable if it can represent the `NULL` value. For input
tables the nullability of a column is declared explicitly. For
intermediate results and output views the compiler infers the
nullability of each column using type inference rules.

Most SQL operations are defined for nullable types. Our compiler
follows the SQL standard in this respect. Most operations (e.g.,
`+`), when applied a `NULL` operand will produce a `NULL`
value.

## String types

The `n` in `VARCHAR(n)` and `CHAR(n)` is measured in Unicode
characters, not in bytes.  Thus, `"🎉"` fits in a `VARCHAR(1)` or
`CHAR(1)` string, even though it requires 4 bytes in the UTF-8
encoding that Feldera uses internally.

The preferred type for string data is `VARCHAR`, omitting a maximum
length, because operations on `VARCHAR(n)` strings can require extra
work to count Unicode characters so that extra ones can be trimmed
off.

We strongly suggest avoiding `CHAR(n)` entirely.  It suffers from the
same performance problems as `VARCHAR(n)`, but worse, because Feldera
has to count Unicode characters in many more cases.  It also behaves
unintuitively by padding strings with spaces to length `n`.  Comparing
such values with values having different types (e.g., `CHAR(k)` for `k
≠ n`) or even string literals of different lengths may provide
surprising results.  It is supported only for legacy reasons.  The
runtime also expects data sources to provide correctly padded data.

## User-defined types

Users can declare new types.  Such types can be used for columns,
record fields, user-defined function parameters or results.

We distinguish two kinds of user-defined types:

- Users can define new type names for existing types
- Users can define new record types

### New type names for existing types

The following example shows a table using two user-defined types:

```sql
CREATE TYPE INT32 AS INTEGER;
CREATE TYPE IA AS INT ARRAY;
CREATE TABLE T(x INT32, a IA);
```

### New structure types

The syntax for defining new structures resembles the syntax for
defining tables.  For example, we can declare types `address_typ` and
`employee_typ`:

```sql
CREATE TYPE address_typ AS (
   street          VARCHAR(30),
   city            VARCHAR(20),
   state           CHAR(2),
   postal_code     VARCHAR(6));

CREATE TYPE employee_typ AS (
  employee_id       DECIMAL(6),
  first_name        VARCHAR(20),
  last_name         VARCHAR(25),
  email             VARCHAR(25),
  phone_number      VARCHAR(20),
  hire_date         DATE,
  job_id            VARCHAR(10),
  salary            DECIMAL(8,2),
  commission_pct    DECIMAL(2,2),
  manager_id        DECIMAL(6),
  department_id     DECIMAL(4),
  address           address_typ);
```

Recursive or mutually-recursive user-defined types are currently not
supported (i.e.  a user-defined type cannot have a field that refers
to the type itself).

An expression that constructs a structure uses the type name, e.g.:

```sql
employee_typ(315, 'Francis', 'Logan', 'FLOGAN',
    '555.777.2222', DATE '2004-05-01', 'SA_MAN', 11000, .15, 101, 110,
     address_typ('376 Mission', 'San Francisco', 'CA', '94222'))
```

Tables can have structure-valued columns, but these have to be fully
qualified using both the table name and the column name in programs:

```sql
CREATE TABLE PERS(p0 employee_typ, p1 employee_typ);
CREATE VIEW V AS SELECT PERS.p0.address FROM PERS
WHERE PERS.p0.first_name = 'Mike'
```

## Grammar for specifying types

```
type:
      typeName
      [ collectionsTypeName ]*

typeName:
      sqlTypeName
  |   compoundIdentifier
  |   MAP < type , type >
  |   ROW ( columnDecl [, columnDecl ]* )

sqlTypeName:
      char [ precision ]
  |   varchar [ precision ]
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
  |   VARIANT

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

timeZone:
      WITHOUT TIME ZONE
```

A `compoundIdentifier` is a sequence of identifiers separated by dots.
