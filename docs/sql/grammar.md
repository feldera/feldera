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
  :   columnName type [NOT [NULL]] [ columnConstraint ]
  |   columnName
  |   tableConstraint

columnConstraint
  :   PRIMARY KEY
  |   FOREIGN KEY REFERENCES identifier '(' identifier ')'
  |   LATENESS expression
  |   /* empty */

parensColumnList
  :   '(' columnName [, columnName ]* ')'

tableConstraint
  :   [ CONSTRAINT name ]
      {
          CHECK '(' expression ')'
      |   PRIMARY KEY parensColumnList
      }
  |   FOREIGN KEY parensColumnList REFERENCES identifier parensColumnList

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
      [ LIMIT { count | ALL } ]      


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
  :   tablePrimary [ pivot ] [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

pivot
  :   PIVOT '('
      pivotAgg [, pivotAgg ]*
      FOR pivotList
      IN '(' pivotExpr [, pivotExpr ]* ')'
      ')'

pivotAgg
  :   agg '(' [ ALL | DISTINCT ] value [, value ]* ')'
      [ [ AS ] alias ]

pivotList
  :   columnOrList

pivotExpr
  :   exprOrList [ [ AS ] alias ]

columnOrList
  :   column
  |   '(' column [, column ]* ')'

exprOrList
  :   expr
  |   '(' expr [, expr ]* ')'
```

Note: `PRIMARY KEY` and `FOREIGN KEY` information is parsed, but
ignored.

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

### Window aggregates

One type of expression that can appear in a `SELECT` statement is a
window aggregate.  The grammar for window aggregates is:

```
windowedAggregateCall
  : agg '(' [ ALL | DISTINCT ] value [, value ]* ')'
      [ RESPECT NULLS | IGNORE NULLS ]
      [ WITHIN GROUP '(' ORDER BY orderItem [, orderItem ]* ')' ]
      [ FILTER '(' WHERE condition ')' ]
      OVER windowSpec
  | agg '(' '*' ')'
      [ FILTER  '(' WHERE condition ')' ]
      OVER windowSpec

windowSpec
  :   '('
      [ windowName ]
      PARTITION BY expression [, expression ]*
      [ ORDER BY orderItem [, orderItem ]* ]
      [
          RANGE BETWEEN windowRange AND windowRange
      ]
      ')'

windowRange
  :   CURRENT
  |   ( UNBOUNDED | expression ) ( PRECEDING | FOLLOWING )
```

where `agg` is one of the operators in the following table.

<table>
  <tr>
    <th>Aggregate</th>
    <th>Description</th>
  </tr>
  <tr>
    <td><code>COUNT(</code>value [, value ]*<code>)</code></td>
    <td>Returns the number of rows in window for which value is not null</td>
  </tr>
  <tr>
    <td><CODE>COUNT(*)</code></td>
    <td>Returns the number of rows in window</td>
  </tr>
  <tr>
    <td>AVG(numeric)</td>
    <td>Returns the average (arithmetic mean) of numeric across all values in window</td>
  </tr>
  <tr>
    <td><code>SUM</code>(numeric)</td>
    <td>Returns the sum of numeric across all values in window</td>
  </tr>
  <tr>
    <td><code>MAX</code>(value)</td>
    <td>Returns the maximum value of value across all values in window</td>
  </tr>
  <tr>
    <td><code>MIN</code>(value)</td>
    <td>Returns the minimum value of value across all values in window</td>
  </tr>
  <tr>
    <td><code>RANK()</code></td>
    <td>Returns the rank of the current row with gaps</td>
  </tr>
  <tr>
    <td><code>DENSE_RANK()</code></td>
    <td>Returns the rank of the current row without gaps</td>
  </tr>
  <tr>
    <td><code>ROW_NUMBER()</code></td>
    <td>Returns the number of the current row within its partition, counting from 1</td>
  </tr>
  <tr>
    <td><code>FIRST_VALUE</code>(value)</td>
    <td>Returns value evaluated at the row that is the first row of the window frame</td>
  </tr>
  <tr>
    <td><code>LAST_VALUE</code>(value)</td>
    <td>Returns value evaluated at the row that is the last row of the window frame</td>
  </tr>
  <tr>
    <td><code>LEAD</code>(value, offset, default)</td>
    <td>Returns value evaluated at the row that is offset rows after the current row within the partition; if there is no such row, instead returns default. Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to <code>NULL</code></td>
  </tr>
  <tr>
    <td><code>LAG</code>(value, offset, default)</td>
    <td>Returns value evaluated at the row that is offset rows before the current row within the partition; if there is no such row, instead returns default. Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to <code>NULL</code></td>
  </tr>
  <tr>
    <td><code>NTH_VALUE</code>(value, nth)</td>
    <td>Returns value evaluated at the row that is the nth row of the window frame</td>
  </tr>
  <tr>
    <td><code>NTILE</code>(value)</td>
    <td>Returns an integer ranging from 1 to value, dividing the partition as equally as possible</td>
  </tr>
</table>