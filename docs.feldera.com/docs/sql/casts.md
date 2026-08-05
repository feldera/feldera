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
from Calcite.  We strongly recommend avoiding casts when possible,
and using explicit conversion functions.  For example, avoid
converting using casts between integers and `TIME`, `TIMESTAMP`,
`DATE`, `INTERVAL` values; these casts are not portable between SQL
dialects, and may have surprising behaviors.

In general SQL casts may discard low order digits.  A cast from a wide
to a narrow datatype which cannot represent the value in the target
type will generate a runtime error.  Note however that casts to
floating point values from other numeric values never generate runtime
errors, since they use "infinity" values to represent out of range
values.

Conversions from decimal and floating point types to integer types
always truncate the decimal digits (round towards zero).  For example,
`CAST(2.9 AS INTEGER)` returns 2, while `CAST(-2.9 AS INTEGER)`
returns -2.

Casts from an interval to a numeric type return the length of the
interval expressed in the interval's unit.  For example, `CAST(INTERVAL
'10' DAY AS BIGINT)` returns 10, and `CAST(INTERVAL '10.6' SECONDS AS
INTEGER)` returns 10.  Long intervals use years or months as the unit;
short intervals use days, hours, minutes, or seconds.

Casts of strings to numeric types produce a runtime error when the
string cannot be interpreted as a number.  Use `SAFE_CAST` if runtime
errors are undesired.

Casts of strings to `DATE`, `TIME`, `TIMESTAMP` produce a runtime
error when the string does not have the correct format.  Use
`SAFE_CAST` to obtain `NULL` instead.

Casting a `NULL` value to any type produces a `NULL` result.

A value of type `VARIANT` can be cast to any type and will produce a
nullable result; this kind of cast will never fail at runtime.

A value of any type can be cast to a `VARIANT` type.

A cast from an `INTEGER` or `INTEGER UNSIGNED` to a `BINARY` or
`VARBINARY` value will produce a big-endian result, which is truncated
on the *left* if the target is too narrow, and padded on the *left*
otherwise.  Casts between `BINARY` values truncate and pad on the
*right*.  Casts of a string value to a `BINARY` or `VARBINARY` value
produce the UTF-8 bytes of the string (for example, `CAST('1234567890'
AS VARBINARY)` produces `x'31323334353637383930'`), truncated on the
*right* if the target is too narrow.  Casts from `BINARY` to `INTEGER`
types are not supported.

A cast to a `ROW` type is only allowed for compatible `ROW` types, or
for `VARIANT` types.  Such a cast will cast recursively each field of
the source value to the corresponding type of the destination field.
For example, the following statement is legal:

```sql
SELECT CAST(ROW(1, 2) AS ROW(a INTEGER, b TINYINT)) AS r;
```

## Safe casts

<a id="safe_cast"></a>The `SAFE_CAST` function has the same syntax as `CAST`.  `SAFE_CAST`
produces the same result as `CAST` for all legal inputs.  The main
difference is that `SAFE_CAST` never produces a runtime error,
producing a `NULL` value when a conversion is illegal.

Currently `SAFE_CAST` is not supported when the target type is a `ROW`
or user-defined type.