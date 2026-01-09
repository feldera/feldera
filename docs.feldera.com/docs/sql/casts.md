# Casts and Data Type Conversions

SQL expressions can mix data of different types in the same
expression.  Most operations, however, require operands to have either
the same type, or specific type combinations.  The SQL compiler will
in this case insert implicit type conversion operations, also called
*casts*.

An explicit cast can be specified in two ways:

* using the `CAST` operator: <code>CAST(value AS type)</code>
* <a id="coloncolon"></a>using an infix operator <code>::</code> from Postgres:
  <code>value :: type</code>

The rules for implicit casts are complex; we [inherit these
rules](https://calcite.apache.org/docs/reference.html#conversion-contexts-and-strategies)
from Calcite.

In general SQL casts may discard low order digits.  A cast form a wide
to a narrow datatype which cannot represent the value in the target
type will generate a runtime error.  Note however that casts to
floating point values never generate runtime errors, since they use
"infinity" values to represent out of range values.

Conversions from decimal and floating point types to integer types
always truncate the decimal digits (round towards zero).  For example,
`CAST(2.9 AS INTEGER)` returns 2, while `CAST(-2.9 AS INTEGER)`
returns -2.

Casts of strings to numeric types produce a runtime error when the
string cannot be interpreted as a number.  Use `SAFE_CAST` if runtime
errors are undesired.

Casts of strings to `DATE`, `TIME`, `TIMESTAMP` produce the result
`NULL` when the string does not have the correct format.

Casting a `NULL` value to any type produces a `NULL` result.

A value of type `VARIANT` can be cast to any type and will produce a
nullable result; this kind of cast will never fail at runtime.

A value of any type can be cast to a `VARIANT` type.

A cast from an `INTEGER` or `INTEGER UNSIGNED` to a `BINARY` or
`VARBINARY` value will produce a big-endian result, which is truncated
or padded on the *left* if it is too large.  Casts between `BINARY`
values truncate and pad on the *right*.  Casts of a string value to a
`BINARY` of `VARBINARY` value will attempt to parse the string as a
hexadecimal value, and it will truncate the value on the *right* if
its too large.  Casts from `BINARY` to `INTEGER` types are not
supported.

A cast to a `ROW` type is only allowed for compatible `ROW` types, or
for `VARIANT` types.  Such a cast will cast recursively each field of
the source value to the corresponding type of the destination field.
For example, the following statment is legal:

```sql
SELECT cast(row(1, 2) as row(a integer, b tinyint)) as r;
```

## Safe casts

The `SAFE_CAST` function has the same syntax as `CAST`.  `SAFE_CAST`
produces the same result as `CAST` for all legal inputs.  The main
difference is that `SAFE_CAST` never produces a runtime error,
producing a `NULL` value when a conversion is illegal.
