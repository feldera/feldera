# Identifiers

The names of tables, columns, functions, user-defined types, field
names, and other metadata elements used in a SQL query are represented
by identifiers.

Unquoted identifiers, such as `emp`, must start with a letter and can
only contain letters, digits, and underscores. They are automatically
converted to all-lower case.  So `emp`, `EMP`, and `eMp` are all
converted to the same lowercase identifier `emp`, and they all
represent the same identifier.

Quoted identifiers, such as `"Employee Name"`, start and end with
double quotes. They may contain virtually any character, including
spaces and other punctuation. If you wish to include a double quote in
an identifier, use another double quote to escape it, like this: `"An
employee called ""Fred""."`.  The identifiers `"emp"` and `"EMP"` are
thus different.  Quoted identifiers are left unchanged.

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

## Name lookup

Unquoted identifiers are all converted automatically to lowercase
during parsing.  Quoted identifiers are left unchanged.

After these conversions, table, view, user-defined type names,
user-defined field names, `ROW` type field names, user-defined
function names, and column names are looked-up in a case-sensitive
manner.  However, for the pre-defined functions, name lookup is
case-insensitive.  This means that users *cannot* define new functions
or types whose names match existing function names, if they only
differ in case.

## Lateral column aliasing

Feldera SQL supports an extension to standard SQL where columns
defined within a `SELECT` statement can be used immediately by
expressions that appear to the right of their definition in the same
`SELECT` statement.  These columns can also be referenced within the
associated `GROUP BY` and `HAVING` statements.  This is a feature
available in some SQL dialects such as Spark SQL, DuckDB, Snowflake,
and Redshift.

Example:

```sql
CREATE TABLE T(x INT);
INSERT INTO T VALUES(3);

SELECT 1 as X, X+X as Y;
-- result is 1, 2

SELECT x+1 as Y
FROM T
GROUP BY Y;
-- result is 4

SELECT x+1 as Y
FROM T
GROUP BY Y
HAVING Y > 0;
-- result is 4
```

These statements are illegal in standard SQL, but legal in Feldera
SQL.

Such statements are compiled by substituting the expression defining
the column for the column name everywhere the column name is used.  So
the above programs are equivalent to the following standard SQL
programs:

```sql
SELECT 1 as X, 1+1 as Y;

SELECT x+1 as Y
FROM T
GROUP BY x+1;

SELECT x+1 as Y
FROM T
GROUP BY x+1
HAVING x+1 > 0;
```

::: danger

In order to preserve the semantics of standard SQL programs unchanged,
a new column alias is used only if there is no column with the same
name already available in the `FROM` statement.  In the following
example:

```sql
SELECT 1+1 as x, x+x as Y FROM T;
-- result is 2, 6, and not 2, 4!
```

the `SELECT` statement uses for `x` the value of the column from table
`T`, and *not* the newly defined column `x`.

:::

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