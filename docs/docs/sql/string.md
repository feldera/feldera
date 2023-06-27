# String operations

SQL defines two primary character types: `character varying(n)` and
`character(n)`, where n is a positive integer.  Both of these types
can store strings up to n characters (not bytes) in length. An attempt
to store a longer string into a column of these types will result in
an error, unless the excess characters are all spaces, in which case
the string will be truncated to the maximum length. (This somewhat
bizarre exception is required by the SQL standard.) If the string to
be stored is shorter than the declared length, values of type
character will be space-padded; values of type character varying will
simply store the shorter string.

In addition, we provides the `text`, or `varchar` type, which stores
strings of any length.  Although the type `text` is not in the SQL
standard, several other SQL database management systems have it as
well.

## String constants (literals)

A string constant in SQL is an arbitrary sequence of characters
bounded by single quotes (`'`), for example `'This is a string'`. To
include a single-quote character within a string constant, write two
adjacent single quotes, e.g., `'Dianne''s horse'`. Note that this is
not the same as a double-quote character (`"`).

Two string constants that are only separated by whitespace with at
least one newline are concatenated and effectively treated as if the
string had been written as one constant. For example:

```sql
SELECT 'foo'
'bar'
```

is equivalent to:

```
SELECT 'foobar'
```

but:

```
SELECT 'foo'      'bar'
```

is not valid syntax.

## Escaped characters

We also accepts escaped characters withing string constants, which are
an extension to the SQL standard.  Within an escape string, a
backslash character (`\`) begins a C-like backslash escape sequence, in
which the combination of backslash and following character(s)
represent a special byte value:

|Backslash Escape Sequence|Interpretation|
|-------------------------|--------------|
|<code>\b</code>          | backspace    |
|<code>\f</code>          | form feed    |
|<code>\n</code>          | newline      |
|<code>\r</code>          | carriage return |
|<code>\t</code>          | tab          |
|<code>\o, \oo, \ooo</code> | (o = 0–7) octal byte value |
|<code>\xh, \xhh (h = 0–9, A–F)</code> | hexadecimal byte value |
|<code>\uxxxx, \Uxxxxxxxx (x = 0–9, A–F)</code> | 16 or 32-bit hexadecimal Unicode character value |

Any other character following a backslash is taken literally. Thus, to
include a backslash character, write two backslashes `\\`. Also, a
single quote can be included in an escape string by writing `\'`, in
addition to the normal way of `''`.

## Operations on string values

`||` is string concatenation, written as an infix operator.
