# UUID Operations

The `UUID` type represents 128-bit unique identifiers.

## UUID literals

`UUID` literals are specified with `UUID 'string-literal'`, where the
string literal must have an appropriate shape for a UUID.  An example
is: `UUID '123e4567-e89b-12d3-a456-426655440000'`.  A literal accepts
the same spellings as a cast from a string, described below.

## UUID value operations

`UUID` values can be cast to strings (`VARCHAR` or `CHAR`), producing
a string with a representation similar to the one of UUID literals
described above.

`CHAR` or `VARCHAR` values can be cast to `UUID` values.  The string
must hold 32 hex digits of either case, optionally enclosed in braces,
and a hyphen may separate any complete group of four digits.  All of
the following denote the same value:

```
123e4567-e89b-12d3-a456-426655440000
123E4567-E89B-12D3-A456-426655440000
123e4567e89b12d3a456426655440000
{123e4567-e89b-12d3-a456-426655440000}
123e-4567-e89b-12d3-a456-4266-5544-0000
```

Any other string causes a runtime error.  Blanks are never trimmed, so
a leading or trailing blank makes the cast fail, as does the URN form
`urn:uuid:123e4567-e89b-12d3-a456-426655440000`.  These rules match
PostgreSQL.

`UUID` values can be cast to `BINARY` or `VARBINARY` values, and will
produce a 16 byte result.

Conversely, `BINARY` and `VARBINARY` values can be cast to `UUID`
values.  The value must be exactly 16 bytes; any other length causes a
runtime error.

## UUID comparisons

Comparing a `UUID` with a string or a binary value converts that value
to a `UUID`, the same direction as comparing a string with a number.
A value that does not denote a `UUID` is therefore a runtime error:

```sql
UUID '123e4567-e89b-12d3-a456-426655440000' = '123E4567-E89B-12D3-A456-426655440000'  -- true
UUID '123e4567-e89b-12d3-a456-426655440000' = x'123e4567e89b12d3a456426655440000'     -- true
UUID '123e4567-e89b-12d3-a456-426655440000' <> ''                                     -- runtime error
```

A `CHAR(n)` value is padded with blanks to `n` characters, so comparing
a `UUID` with a `CHAR(40)` value fails, whereas `CHAR(36)`, the exact
width of a UUID, leaves nothing to trim.

`UUID` values can be cast to `VARIANT`; `VARIANT` values can be cast
to `UUID`.
