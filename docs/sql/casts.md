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
