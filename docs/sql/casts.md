# Data type conversions

SQL expressions can mix data of different types in the same
expression.  Most operations, however, require operands to have either
the same type, or specific type combinations.  The SQL compiler will
in this case insert implicit type conversion operations, also called
*casts*.

An explicit cast can be specified in three ways:

* using the `CAST` operator: <code>CAST(value AS type)</code>
* using an infix operator <code>::</code> from Postgres:
  <code>value :: type</code>
* using the `CONVERT` function: `CONVERT(value, type)`

The rules for implicit casts are complex; we [inherit these
rules](https://calcite.apache.org/docs/reference.html#conversion-contexts-and-strategies)
from Calcite.

In general SQL casts may discard low order digits but never high order
digits.  A cast form a wide to a narrow datatype which cannot
represent the value in the target type will generate a runtime error.
Note however that casts to floating point values never generate
runtime errors, since they use "infinity" values to represent out of
range values.

Conversions from decimal and floating point types to integer types
always truncate the decimal digits (round towards zero).  For example,
`CAST(2.9 AS INTEGER)` returns 2, while `CAST(-2.9 AS INTEGER)`
returns -2.

Casts of strings to numeric types produce a runtime error when the
string cannot be interpreted as a number.

Casts of strings to `DATE`, `TIME`, `TIMESTAMP` produce the result
`NULL` when the string does not have the correct format.

Casting a `NULL` value to any type produces a `NULL` result.
