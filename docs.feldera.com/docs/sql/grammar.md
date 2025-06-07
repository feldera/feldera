# SQL Grammar

This is a short formal description of the grammar supported in a BNF
form.

- Constructs enclosed between `[]` are optional.
- `*` denotes zero or many repetitions.
- Uppercase words (`FUNCTION`) and single-quoted text (`')'`) indicate
  grammar terminals.
- Parentheses `()` are used for grouping productions together.
- The vertical bar `|` indicates choice between two constructs.

SQL reserved keywords cannot be used as table and view names.  In
addition, the following keywords are reserved: `USER`, `NOW`.  All
identifiers starting with "Feldera" are also reserved (in all case
combinations).

```
statementList:
      statement [ ';' statement ]* [ ';' ]

statement
  :   createTableStatement
  |   declareRecursiveViewStatement
  |   createViewStatement
  |   createFunctionStatement
  |   createTypeStatement
  |   createIndexStatement
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

See [user-defined types](types.md#user-defined-types)

## Creating tables

<a id="default"></a>
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

Columns that are part of a `PRIMARY KEY` cannot have nullable types.

Columns that are part of a `FOREIGN KEY` must refer to `PRIMARY KEY`
columns in some table.  Their types must must match (but `FOREIGN KEY`
columns may be nullable).

`CREATE TABLE` is used to declare tables.  Tables correspond to input
data sources.  A table declaration must list the table columns and
their types.  Here is an example:

```sql
CREATE TABLE empsalary (
    depname varchar not null PRIMARY KEY,
    empno bigint FOREIGN KEY REFERENCES employee(empid),
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
    'url' = '127.0.0.1:8080'
);
```

### Table properties that impact the program semantics

#### Materialized tables

Unlike a database, Feldera does not normally maintain the contents of
tables; it will only store as much data as necessary to compute future
outputs.  By specifying the property `'materialized' = 'true'` a user
instructs Feldera to also maintain the complete contents of the table.
Such materialized tables can be browsed and queried at runtime.
See [Materialized Tables and Views](materialized.md) for more details.

#### Append-only tables

The `append_only` Boolean property describes tables that only support
insertions.  Updates or deletes are not allowed in such tables.

See [Streaming SQL Extensions, append-only
tables](streaming.md#append_only-tables)

#### Size hints

The property `expected_size` can be used to pass information to the
SQL compiler about the expected size of a table in steady state
operation.  The value of this property should be an integer value.

### LATENESS

```
latenessStatement
  :   LATENESS view '.' column expression
```

See [Streaming SQL Extensions, LATENESS](streaming.md#lateness-expressions)

### WATERMARKS

See [Streaming SQL Extensions, WATERMARKS](streaming.md#watermark-expressions)

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

`DECLARE RECURSIVE VIEW` is used to declare a view that can afterwards
be used in a recursive SQL query.  The syntax of this statement is
reminiscent of a table declaration, without constraints.  Recursive
queries are documented in [this section](recursion).

```
declareRecursiveViewStatement:
  :   DECLARE RECURSIVE VIEW name
      '(' columnDecl [, columnDecl ]* ')'

createViewStatement
  :   CREATE [ LOCAL | MATERIALIZED ] VIEW name
      [ '(' columnName [, columnName ]* ')' ]
      [ 'WITH' keyValueList ]
      AS query
```

<a id="setop"></a>
```
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
```

<a id="values"></a>
```
values
  :   { VALUES | VALUE } expression [, expression ]*

select
  :   SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY [ ALL | DISTINCT ] { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]
```

<a id="lateral"></a>
```
tablePrimary
  :   tableName '(' TABLE tableName ')'
  |   tablePrimary '(' columnDecl [, columnDecl ]* ')'
  |   [ LATERAL ] '(' query ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   TABLE '(' functionName '(' expression [, expression ]* ')' ')'
```

<a id="cube"></a>
```
groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'
  |   CUBE '(' expression [, expression ]* ')'
  |   ROLLUP '(' expression [, expression ]* ')'

selectWithoutFrom
  :   SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
```

<a id="order"></a>
```
orderItem
  :   expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]
```

<a id="as"></a>
```
projectItem
  :   expression [ [ AS ] columnAlias ]
  |   tableAlias . *
```

<a id="join"></a>
```
tableExpression
  :   tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ { LEFT | RIGHT | FULL } [ OUTER | ASOF ] ] JOIN tableExpression [ joinCondition ]
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

Beware that in the SQL `UNION`, `EXCEPT`, and `INTERSECT` statements
the column names are *not* used to reorder columns.

In `orderItem`, if expression is a positive integer n, it denotes the
nth item in the `SELECT` clause.

## Creating indexes

Feldera supports the `CREATE INDEX` SQL statement with the following
syntax:

```
createIndexStatement
   : CREATE INDEX identifier ON identifier(columnOrList);
```

For the purpose of incremental view maintenance Feldera automatically
creates all required indexes by analyzing the query defining the view.
The SQL `CREATE INDEX` statement is used for a different purpose in
Feldera than in standard databases; the statement is used to specify
*unique keys* for output views.  Consider the following SQL program
fragment:

```sql
CREATE VIEW V AS SELECT id, ... FROM ...;

CREATE INDEX v_index ON V(id);
```

The `CREATE INDEX` statement creates an index named `v_index` over the
view `V`, treating column `id` as a primary key.  The index does *not*
enforce uniqueness of the unique columns at runtime.  This information
can be used by some output connectors, which can refer to `v_index` to
[provide more efficient updates](../connectors/unique_keys.md).
Indexes only have effect for views that are not `LOCAL`.

A view can have multiple indexes.

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
The window bounds must be non-negative constant values.

## Table functions

Table functions are invoked using the syntax `TABLE(function(arguments))`.

See [Table functions](table.md#table-functions-1)

## `ASOF` joins

An `ASOF JOIN` operation combines rows from two tables based on
timestamp values.  For each row in the left table, the join
finds at most a single row in the right table that has the "closest"
timestamp value. The matched row on the right side is the closest
match whose timestamp column is compared using the comparison operator in
the `MATCH_CONDITION` clause.  Currently, only `>=` is supported.
The comparison is performed using SQL semantics, which returns `false`
when comparing `NULL` values with any other values.  Thus a `NULL`
timestamp in the left table will not match any timestamps in the right table.

Currently, only the `LEFT` form of the `ASOF JOIN` is supported.  In this case,
when there is no match for a row in the left table, the columns from
the right table are null-padded.

```sql
SELECT *
FROM left_table LEFT ASOF JOIN right_table
MATCH_CONDITION ( left_table.timecol >= right_table.timecol )
ON left_table.col = right_table.col
```
