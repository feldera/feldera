# SQL Grammar

This is a short formal description of the grammar supported in a BNF
form.

- Constructs enclosed between `[]` are optional.
- `*` denotes zero or many repetitions.
- Uppercase words (`FUNCTION`) and single-quoted text (`')'`) indicate
  grammar terminals.
- Parentheses `()` are used for grouping productions together.
- The vertical bar `|` indicates alternation.

```
statementList:
      statement [ ';' statement ]* [ ';' ]

statement
  :   createTableStatement
  |   createViewStatement
  |   createFunctionStatement
  |   createTypeStatement
  |   latenessStatement

generalType
  :   type [NOT NULL]

createFunctionStatement
  :   CREATE FUNCTION name '(' [ columnDecl [, columnDecl ]* ] ')' RETURNS generalType

latenessStatement
  :   LATENESS view '.' column expression

createTableStatement
  :   CREATE TABLE name
      '(' tableElement [, tableElement ]* ')'

createTypeStatement
  :   CREATE TYPE name AS '(' typedef ')'

typedef
  : generalType
  | name generalType [, name type ]*

createViewStatement
  :   CREATE VIEW name
      [ '(' columnName [, columnName ]* ')' ]
      AS query

tableElement
  :   columnName generalType ( columnConstraint )*
  |   columnName
  |   tableConstraint

columnConstraint
  :   PRIMARY KEY
  |   FOREIGN KEY REFERENCES identifier '(' identifier ')'
  |   LATENESS expression
  |   DEFAULT expression

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

tablePrimary
  :   [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
  |   tablePrimary '(' columnDecl [, columnDecl ]* ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'
  |   CUBE '(' expression [, expression ]* ')'
  |   ROLLUP '(' expression [, expression ]* ')'

columnDecl
  :   column generalType

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

Note: `FOREIGN KEY` information is parsed, but it is not validated,
and is currently ignored.

In `orderItem`, if expression is a positive integer n, it denotes the
nth item in the `SELECT` clause.

SQL `CREATE FUNCTION` can be used to declare [user-defined
functions](udf.md).

An aggregate query is a query that contains a `GROUP BY` or a `HAVING`
clause, or aggregate functions in the `SELECT` clause. In the
`SELECT`, `HAVING` and `ORDER` BY clauses of an aggregate query, all
expressions must be constant within the current group (that is,
grouping constants as defined by the `GROUP BY` clause, or constants),
or aggregate functions, or a combination of constants and aggregate
functions. Aggregate and grouping functions may only appear in an
aggregate query, and only in a `SELECT`, `HAVING` or `ORDER BY`
clause.  Aggregate functions are described in [this
section](aggregates.md#standard-aggregate-operations).

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

### Grouping functions

<table>
  <tr>
    <th>Function</th>
    <th>Description</th>
  </tr>
  <tr>
     <td><code>GROUPING(</code>expression, [ expression ]<code>)</code></td>
     <td>Returns a bit vector of the grouping expressions</td>
  </tr>
  <tr>
     <td><code>GROUPING_ID(</code>expression, [ expression ]<code>)</code></td>
     <td>Synonym for <code>GROUPING</code></td>
  </tr>
</table>

Example using `GROUPING`:

```sql
select deptno, job, count(*) as c, grouping(deptno) as d,
  grouping(job) j, grouping(deptno, job) as x
from emp
group by cube(deptno, job);
+--------+-----------+----+---+---+---+
| DEPTNO | JOB       | C  | D | J | X |
+--------+-----------+----+---+---+---+
|     10 | CLERK     |  1 | 0 | 0 | 0 |
|     10 | MANAGER   |  1 | 0 | 0 | 0 |
|     10 | PRESIDENT |  1 | 0 | 0 | 0 |
|     10 |           |  3 | 0 | 1 | 1 |
|     20 | ANALYST   |  2 | 0 | 0 | 0 |
|     20 | CLERK     |  2 | 0 | 0 | 0 |
|     20 | MANAGER   |  1 | 0 | 0 | 0 |
|     20 |           |  5 | 0 | 1 | 1 |
|     30 | CLERK     |  1 | 0 | 0 | 0 |
|     30 | MANAGER   |  1 | 0 | 0 | 0 |
|     30 | SALESMAN  |  4 | 0 | 0 | 0 |
|     30 |           |  6 | 0 | 1 | 1 |
|        | ANALYST   |  2 | 1 | 0 | 2 |
|        | CLERK     |  4 | 1 | 0 | 2 |
|        | MANAGER   |  3 | 1 | 0 | 2 |
|        | PRESIDENT |  1 | 1 | 0 | 2 |
|        | SALESMAN  |  4 | 1 | 0 | 2 |
|        |           | 14 | 1 | 1 | 3 |
+--------+-----------+----+---+---+---+
```

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

Where `agg` is a window aggregate function as described in the [section
on aggregation](aggregates.md#window-aggregate-functions).

### LATENESS expressions

`LATENESS` is a property of the data in a column of a table or a view
that is relevant for the case of stream processing.  `LATENESS` is
described by an expression that evaluates to a constant value.  The
expression must have a type that can be subtracted from the column
type.  For example, a column of type `TIMESTAMP` may have a lateness
specified as an `INTERVAL` type.

To specify `LATENESS` for a table column, the column declaration can
be annotated in the `CREATE TABLE` statement.  For example:

```sql
CREATE TABLE order_pickup (
   when TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES,
   location VARCHAR
);
```

To specify `LATENESS` for a view, our custom SQL statement `LATENESS`
must be used.  The statement specifies a view, a column of the view,
and an expression for the latness value.  currently must appear
*before* the view declaration.  For example:

```sql
LATENESS V.COL1 INTERVAL '1' HOUR;
CREATE VIEW V AS SELECT T.COL1, T.COL2 FROM T;
```

The `LATENESS` property of a column allows values that are too much
"out of order" to be ignored.  A value is "out of order" the table or
view has previously contains a row with a larger value for the
respective column.

For example, consider the table above, and a sequence of insertions,
each as a separate transaction:

```sql
INSERT INTO T VALUES('2020-01-01 00:00:00', 'home');
INSERT INTO T VALUES('2020-01-01 01:00:00', 'office');
-- next row is late, but not too late
INSERT INTO T VALUES('2020-01-01 00:10:00', 'shop');
INSERT INTO T VALUES('2020-01-01 02:00:00', 'home');
-- next row is too late, and it will be ignored
INSERT INTO T VALUES('2020-01-01 00:20:00', 'friend');
```

The third insertion is out of order, since its timestamp value is
smaller than the previous insertion.  But it is still not too late,
since it is only 50 minutes late, whereas the column lateness is 1
hour.  The fifth row is too late, though, since it is 100 minutes late
with respect to the fourth row.

Lateness is used to instruct the data processing system to ignore some
records.  This ensures that some computed results that reflect past
data may not be updated due to very late coming data.  This also
enables the runtime system to avoid maintaining very old state, which
may never impact future results.

A table or view can have any number of columns annotated with
lateness.  A row is considered "too late" if any of the columns is too
late.

