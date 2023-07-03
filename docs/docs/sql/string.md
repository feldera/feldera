# String operations

SQL defines two primary character types: `character varying(n)` and
`character(n)`, where n is a positive integer.  Both of these types
can store strings up to n characters (not bytes) in length. An attempt
to store a longer string into a column of these types will result in
an error, unless the excess characters are all spaces, in which case
the string will be truncated to the maximum length. (This somewhat
bizarre exception is required by the SQL standard.)  If the string to
be stored is shorter than the declared length, values of type
character will be space-padded; values of type character varying will
simply store the shorter string.

In addition, we provides the `text`, or `varchar` type, which stores
strings of any length.

Trailing spaces are removed when converting a character value to one
of the other string types.  Note that trailing spaces are semantically
significant in character varying and text values, and when using
pattern matching, that is LIKE and regular expressions.

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

```sql
SELECT 'foobar'
```

but:

```sql
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

<table>
  <tr>
    <th>Operation</th>
    <th>Description</th>
  </tr>
  <tr>
    <td><code>||</code></td>
    <td>String concatenation (infix)</td>
  </tr>
  <tr>
    <td><code>trim ( [ LEADING | TRAILING | BOTH ]</code> characters <code>FROM</code> string <code>)</code></td>
    <td>Remove the specified characters from the specified ends of the string argument</td>
  </tr>
  <tr>
    <td><code>substring (</code> string <code>[ FROM</code> start <code>] [ FOR</code> count<code> ] )</code></td>
    <td>Extracts the substring of string starting at the "start"'th character if that is specified, and stopping after "count" characters if the value is specified. At least one of "start" or "count" must be provided.  If "start" is negative, it is replaced with 1.  If "count" is negative the empty string is returned.  The index of the first character is 1.</td>
  </tr>
  <tr>
    <td><code>string LIKE pattern [ESCAPE escape-character]</code> and
        <code>string NOT LIKE pattern [ESCAPE escape-character]</code></td>
    <td>The LIKE expression returns true if the string matches the supplied pattern. (As expected, the NOT LIKE expression returns false if LIKE returns true.  See below for details.
    </td>
  </tr>
</table>

## `LIKE`

string `LIKE` pattern [`ESCAPE` escape-character]

string `NOT LIKE` pattern [`ESCAPE` escape-character]

If pattern does not contain percent signs or underscores, then the
pattern only represents the string itself; in that case `LIKE` acts
like the equals operator. An underscore (`_`) in pattern stands for
(matches) any single character; a percent sign (`%`) matches any
sequence of zero or more characters.

Some examples:

```sql
'abc' LIKE 'abc'    true
'abc' LIKE 'a%'     true
'abc' LIKE '_b_'    true
'abc' LIKE 'c'      false
```

`LIKE` pattern matching always covers the entire string. Therefore, if
it's desired to match a sequence anywhere within a string, the pattern
must start and end with a percent sign.

To match a literal underscore or percent sign without matching other
characters, the respective character in pattern must be preceded by
the escape character. The default escape character is the backslash
but a different one can be selected by using the ESCAPE clause. To
match the escape character itself, write two escape characters.  The
escape character cannot be one of the special pattern characters `_`
or `%`.

Some examples where the escape character is changed to `#`:

```sql
SELECT 'hawkeye' LIKE 'h%' ESCAPE '#'          true
SELECT 'hawkeye' NOT LIKE 'h%' ESCAPE '#'      false
SELECT 'h%' LIKE 'h#%' ESCAPE '#'              true
SELECT 'h%' NOT LIKE 'h#%' ESCAPE '#'          false
SELECT 'h%wkeye' LIKE 'h#%' ESCAPE '#'         false
SELECT 'h%wkeye' NOT LIKE 'h#%' ESCAPE '#'     true
SELECT 'h%wkeye' LIKE 'h#%%' ESCAPE '#'        true
SELECT 'h%wkeye' NOT LIKE 'h#%%' ESCAPE '#'    false
SELECT 'h%awkeye' LIKE 'h#%a%k%e' ESCAPE '#'   true
```
