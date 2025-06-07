# UUID Operations

The `UUID` type represents 128-bit unique identifiers.

## UUID literals

`UUID` literals are specified with `UUID 'string-literal'`, where the
string literal must have an appropriate shape for a UUID, composed of
hex digits grouped in 8-4-4-4-12.  An example is: `UUID
'123e4567-e89b-12d3-a456-426655440000'`.

## UUID value operations

`UUID` values can be cast to strings (`VARCHAR` or `CHAR`), producing
a string with a representation similar to the one of UUID literals
described above.

`CHAR` or `VARCHAR` values can be cast to `UUID` values.  The cast
will succeed if the string is a legal UUID literal; otherwise a runtime
error will occur.

`UUID` values can be cast to `BINARY` or `VARBINARY` values, and will
produce a 16 byte result.

Conversely, `BINARY` and `VARBINARY` values that have exactly 16 bytes
can be cast to `UUID` values.

`UUID` values can be cast to `VARIANT`; `VARIANT` values can be cast
to `UUID`.
