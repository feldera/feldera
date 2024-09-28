# User-defined functions

The SQL statement `CREATE FUNCTION` can be used to declare new
functions.  Functions can be implemented either in SQL or in Rust.

## User-defined functions written in SQL

The following example shows a user-defined function:

```sql
CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER)
RETURNS BOOLEAN NOT NULL
AS (str LIKE ('%' || CAST(value AS VARCHAR) || '%'));
CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5' AS VARCHAR), 5)
```

The function name capitalization obeys the same rules as table and
view names: the names are converted by default to all-capitals, but if
the name is quoted capitalization is unchanged.

## User-defined functions written in Rust

:::caution Feature under construction

This feature is still being developed and is not yet usable through
Feldera's public API or Web Console.

:::

The SQL statement `CREATE FUNCTION` can be used to declare new
functions whose implementation is provided separately in Rust.

The implementation of these functions must be provided to the Rust
compiler in a separate file.  Moreover, when the SQL program contains
user-defined functions, the compiler will generate an additional
output file named `protos.rs` containing the signature for all the
Rust user-defined functions.

The following example shows a user-defined function:

```sql
CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER) RETURNS BOOLEAN NOT NULL;
CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5' AS VARCHAR), 5);
```

The function name capitalization obeys the same rules as table and
view names: the names are converted by default to all-capitals, but if
the name is quoted capitalization is unchanged.  This is important,
because the user must provide a rust implementation that matches the
canonical function name.  Here is a possible implementation of the
function `contains_number` above:

```rs
use feldera_sqllib::*;

pub fn CONTAINS_NUMBER(str: String, value: Option<i32>) ->
   Result<bool, Box<dyn std::error::Error>> {
   match value {
      None => Err(\"null value\".into()),
      Some(value) => Ok(str.contains(&format!(\"{}\", value).to_string())),
   }
}
```

The `use feldera_sqllib::*` directive imports the definitions of the
standard Rust types that the compiler uses to implement SQL datatypes.
The next section explains what these types are.

Notice the function implemented has an all-capitals name (which is not
a standard convention for Rust), dictated by the default SQL
capitalization rules.

Currently there is no type casting or type inference performed for the
function arguments in the SQL program.  For example, a call such as
`CONTAINS_NUMBER('2010-10-20', '5')` will fail at SQL compilation time
because the first argument has type `CHAR(8)` instead of `VARCHAR`,
and the second argument has type `CHAR(1)` instead of `INTEGER`.

Clearly, user-defined functions can pose security problems, since the
Rust implementation is only verified by the Rust compiler.  Such
functions are expected to have no side-effects, to be deterministic,
and not to crash.

## Type representation in Rust

The following table shows the Rust representation of standard SQL data
types.  A nullable SQL type is represented by the corresponding rust
`Option<>` type.  Notice that some of these types are not standard
Rust types, but are defined in the DBSP runtime library.

SQL | Rust
-----------
`BOOLEAN` | `bool`
`TINYINT` | `i8`
`SMALLINT` | `i16`
`INT`  | `i32`
`BIGINT` | `i64`
`DECIMAL`(p, s) | `Decimal`
`REAL` | `F32`
`DOUBLE` | `F64`
`CHAR`(n) | `String`
`VARCHAR`, `VARCHAR`(n) | `String`
`BINARY`, `BINARY`(n) | `ByteArray`
`NULL` | `()`
`INTERVAL` | `ShortInterval`, `LongInterval`
`TIME` | `Time`
`TIMESTAMP` | `Timestamp`
`DATE` | `Date`
`T ARRAY` | `Vec<T>`
`MAP<K, V> | `BTreeMap<K, V>`
`VARIANT` | `Variant`

Multiple SQL types may be represented by the same Rust type.  For
example, `CHAR`, `CHAR(n)`, `VARCHAR(n)`, and `VARCHAR` are all
represented by the standard Rust `String` type.

The SQL family of `INTERVAL` types translates to one of two Rust
types: `ShortInterval` (representing intervals from days to seconds),
and `LongInterval` (representing intervals from years to months).
(Our dialect of SQL does not allow mixing the two kinds of intervals
in a single expression.)

In the Rust implementation the function always has to return the type
`Result<T, Box<dyn std::error::Error>>`, where `T` is the Rust
equivalent of the expected return type of the SQL function.  The Rust
function should return an `Err` only when the function fails at
runtime.  The function should return an error only for fatal
conditions, similar to other SQL functions (e.g., array index out of
bounds, arithmetic overflows, etc.).

## Limitations

There can be only one function with each name.

Functions cannot have names identical to standard SQL library function
names.

Polymorphic functions are not supported.  For example, in SQL the
addition operation operates on any numeric types; such an operation
cannot be implemented as a single user-defined function.

The current type checker is very strict, and it requires the function
arguments to have exactly the specified types.  No casts are inserted
by the compiler.

