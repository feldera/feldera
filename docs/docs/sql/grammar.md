# SQL Grammar

This is a short formal description of the grammar supported:

```
statementList:
      statement [ ';' statement ]* [ ';' ]

statement
  :   createTableStatement
  |   createViewStatement

createTableStatement
  :   CREATE TABLE name
      '(' tableElement [, tableElement ]* ')'

createViewStatement
  :   CREATE VIEW name
      [ '(' columnName [, columnName ]* ')' ]
      AS query

tableElement
  :   columnName type [ columnConstraint ]
  |   columnName
  |   tableConstraint

columnConstraint
  :   [ CONSTRAINT name ]
      [ NOT ] NULL

tableConstraint
  :   [ CONSTRAINT name ]
      {
          CHECK '(' expression ')'
      |   PRIMARY KEY '(' columnName [, columnName ]* ')'
      }

query
  :   values
  |   WITH withItem [ , withItem ]* query
  |   {
          select
      |   selectWithoutFrom
      |   query UNION [ ALL | DISTINCT ] query
      |   query EXCEPT [ ALL | DISTINCT ] query
      |   query MINUS [ ALL | DISTINCT ] query
      |   query INTERSECT [ ALL | DISTINCT ] query
      }
      [ ORDER BY orderItem [, orderItem ]* ]


values
  :   { VALUES | VALUE } expression [, expression ]*

select
  :   SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY [ ALL | DISTINCT ] { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]
      [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]

tablePrimary
  :   [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
  |   tablePrimary '(' columnDecl [, columnDecl ]* ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'

columnDecl
  :   column type [ NOT NULL ]

selectWithoutFrom
  :   SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

withItem
  :   name
      [ '(' column [, column ]* ')' ]
      AS '(' query ')'

orderItem
  :   expression [ ASC | DESC ]

projectItem
  :   expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression
  :   tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ { LEFT | RIGHT | FULL } [ OUTER ] ] JOIN tableExpression [ joinCondition ]
  |   tableExpression CROSS JOIN tableExpression
  |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

joinCondition
  :   ON booleanExpression
  |   USING '(' column [, column ]* ')'

tableReference
  :   tablePrimary [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

windowSpec
  :   '('
      [ windowName ]
      [ ORDER BY orderItem [, orderItem ]* ]
      [ PARTITION BY expression [, expression ]* ]
      [
          RANGE numericOrIntervalExpression { PRECEDING | FOLLOWING }
       ]
      ')'
```

In `orderItem`, if expression is a positive integer n, it denotes the
nth item in the `SELECT` clause.

An aggregate query is a query that contains a `GROUP BY` or a `HAVING`
clause, or aggregate functions in the `SELECT` clause. In the
`SELECT`, `HAVING` and `ORDER` BY clauses of an aggregate query, all
expressions must be constant within the current group (that is,
grouping constants as defined by the `GROUP BY` clause, or constants),
or aggregate functions, or a combination of constants and aggregate
functions. Aggregate and grouping functions may only appear in an
aggregate query, and only in a `SELECT`, `HAVING` or `ORDER BY`
clause.

A scalar sub-query is a sub-query used as an expression. If the
sub-query returns no rows, the value is `NULL`; if it returns more
than one row, it is an error.

`IN`, `EXISTS`, and scalar sub-queries can occur in any place where an
expression can occur (such as the `SELECT` clause, `WHERE` clause,
`ON` clause of a `JOIN`, or as an argument to an aggregate function).

An `IN`, `EXISTS`, or scalar sub-query may be correlated; that is,
it may refer to tables in the `FROM` clause of an enclosing query.

`GROUP BY DISTINCT` removes duplicate grouping sets (for example,
`GROUP BY DISTINCT GROUPING SETS ((a), (a, b), (a))` is equivalent to
`GROUP BY GROUPING SETS ((a), (a, b))`); `GROUP BY ALL` is equivalent
to `GROUP BY`.

`MINUS` is equivalent to `EXCEPT`.

## Identifiers

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