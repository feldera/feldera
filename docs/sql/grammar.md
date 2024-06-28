# SQL Grammar

This is a short formal description of the grammar supported in a BNF
form.

- Constructs enclosed between `[]` are optional.
- `*` denotes zero or many repetitions.
- Uppercase words (`FUNCTION`) and single-quoted text (`')'`) indicate
  grammar terminals.
- Parentheses `()` are used for grouping productions together.
- The vertical bar `|` indicates choice between two constructs.

```
statementList:
      statement [ ';' statement ]* [ ';' ]

statement
  :   createTableStatement
  |   createViewStatement
  |   createFunctionStatement
  |   createTypeStatement
  |   latenessStatement

columnDecl
  :   column generalType
```

## Creating user-defined types

```
generalType
  :   type [NOT NULL]

createTypeStatement
  :   CREATE TYPE name AS '(' typedef ')'

typedef
  : generalType
  | name generalType [, name type ]*
```

See [user-defined structures](types.md#user-defined-structures)

## Creating tables

```
createTableStatement
  :   CREATE TABLE name
      '(' tableElement [, tableElement ]* ')'
      [ 'WITH' keyValueList ]

tableElement
  :   columnName generalType ( columnConstraint )*
  |   columnName
  |   tableConstraint

columnConstraint
  :   PRIMARY KEY
  |   FOREIGN KEY REFERENCES identifier '(' identifier ')'
  |   LATENESS expression
  |   WATERMARK expression
  |   DEFAULT expression

tableConstraint
  :   [ CONSTRAINT name ]
      {
          CHECK '(' expression ')'
      |   PRIMARY KEY parensColumnList
      }
  |   FOREIGN KEY parensColumnList REFERENCES identifier parensColumnList

parensColumnList
  :   '(' columnName [, columnName ]* ')'

keyValueList
  : '(' keyValue ( ',' keyValue )* ')'

keyValue
  : stringLiteral '=' stringLiteral
```

Note: `FOREIGN KEY` information is parsed, but it is not validated,
and is currently ignored.

`CREATE TABLE` is used to declare tables.  Tables correspond to input
data sources.  A table declaration must list the table columns and
their types.  Here is an example:

```sql
CREATE TABLE empsalary (
    depname varchar,
    empno bigint,
    salary int,
    enroll_date date
);
```

A table declaration can have an optional `WITH` clause which is used
to specify properties of the connector that provides the source data.
The properties are specified as key-value pairs, each written as a
string.  Here is an example:

```sql
CREATE TABLE empsalary (
    depname varchar,
    empno bigint,
    salary int,
    enroll_date date
) WITH (
    'source' = 'kafka',
    'url' = 'localhost:8080'
);
```

Unlike a database, Feldera does not normally maintain the contents of
tables; it will only store as much data as necessary to compute future
outputs.  By specifying the property `'materialized' = 'true'` a user
instructs Feldera to also maintain the complete contents of the table.
Such materialized tables can be browsed and queried at runtime.
See [Materialized Tables and Views](materialized.md) for more details.

### LATENESS

```
latenessStatement
  :   LATENESS view '.' column expression
```

See [Streaming SQL Extensions](streaming.md#lateness-expressions)

### WATERMARKS

See [Streaming SQL Extensions](streaming.md#watermark-expressions)

## Creating user-defined functions.

`CREATE FUNCTION` is used to declare [user-defined functions](udf.md).

```
createFunctionStatement
  :   CREATE FUNCTION name '(' [ columnDecl [, columnDecl ]* ] ')' RETURNS generalType
      [ AS expression ]
```

## Creating views

`CREATE VIEW` is used to declare a view.  The optional `LOCAL`
keyword can be used to indicate that the declared view is not exposed
to the outside world as an output of the computation.  This is useful
for modularizing the SQL code, by declaring intermediate views that
are used in the implementation of other views.
The `MATERIALIZED` keyword instructs Feldera to maintain a full copy
of the view's output in addition to producing the
stream of changes.
Such materialized views can be browsed and queried at runtime.
See [Materialized Tables and Views](materialized.md) for more details.

```
createViewStatement
  :   CREATE [ LOCAL | MATERIALIZED ] VIEW name
      [ '(' columnName [, columnName ]* ')' ]
      [ 'WITH' keyValueList ]
      AS query

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


withItem
  :   name
      [ '(' column [, column ]* ')' ]
      AS '(' query ')'

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
  :   tableName '(' TABLE tableName ')'
  |   tablePrimary '(' columnDecl [, columnDecl ]* ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   TABLE '(' functionName '(' expression [, expression ]* ')' ')'

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'
  |   CUBE '(' expression [, expression ]* ')'
  |   ROLLUP '(' expression [, expression ]* ')'

selectWithoutFrom
  :   SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

orderItem
  :   expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]

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

In `orderItem`, if expression is a positive integer n, it denotes the
nth item in the `SELECT` clause.

### Aggregate queries

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

## Sub-queries

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

## Window aggregates

One type of expression that can appear in a `SELECT` statement is a
window aggregate.  The grammar for window aggregates is:

```
windowedAggregateCall
  : agg '(' [ ALL | DISTINCT ] value [, value ]* ')'
      [ RESPECT NULLS | IGNORE NULLS ]
      OVER windowSpec
  | agg '(' '*' ')'
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

Currently we require window ranges to have constant values.  This
precludes ranges such as `INTERVAL 1 YEAR`, which have variable sizes.

## Table functions

Table functions are invoked using the syntax `TABLE(function(arguments))`.

See [Table functions](table.md#table-functions)
