# Identifiers

Identifiers are the names of tables, columns and other metadata
elements used in a SQL query.

Unquoted identifiers, such as `emp`, must start with a letter and can
only contain letters, digits, and underscores. They are implicitly
converted to upper case.

Quoted identifiers, such as `"Employee Name"`, start and end with
double quotes. They may contain virtually any character, including
spaces and other punctuation. If you wish to include a double quote in
an identifier, use another double quote to escape it, like this: `"An
employee called ""Fred""."`.

Quoting an identifier also makes it case-sensitive, whereas unquoted
names are always folded to upper case.

A variant of quoted identifiers allows including escaped Unicode
characters identified by their code points. This variant starts with
U& (upper or lower case U followed by ampersand) immediately before
the opening double quote, without any spaces in between, for example
U&"foo".  Inside the quotes, Unicode characters can be specified in
escaped form by writing a backslash followed by the four-digit
hexadecimal code point number. For example, the identifier "data"
could be written as

`U&"d\0061t\0061"`

If a different escape character than backslash is desired, it can be
specified using the UESCAPE clause after the string, for example using
`!` as the escape character:

`U&"d!0061t!0061" UESCAPE '!'`

The escape character can be any single character other than a
hexadecimal digit, the plus sign, a single quote, a double quote, or a
whitespace character. Note that the escape character is written in
single quotes, not double quotes, after UESCAPE.

To include the escape character in the identifier literally, write it
twice.

Currently table, view, and column names are looked-up in a
case-insensitive manner even if they are quoted, so a program cannot
contain simultaneously two such objects whose names only differ in
case-sensitivity.  Function names however can be case-sensitive.

## Comments

A comment is a sequence of characters beginning with double dashes and
extending to the end of the line, e.g.:

```SQL
-- This is a standard SQL comment
```

Alternatively, C-style block comments can be used:

```
/* multiline comment
 * with nesting: /* nested block comment */
 */
```

where the comment begins with `/*` and extends to the matching
occurrence of `*/`.  Note that block comments cannot be nested unlike
the requirements of the SQL standard.