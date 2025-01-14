# Aggregate Operations

## Standard aggregate operations

A `SELECT` expression in the [SQL grammar](grammar.md) can contain one
or more aggregation functions.  Aggregate functions are specified
using the following grammar:

<a id="filter"></a>
```
aggregateCall:
      agg '(' [ ALL | DISTINCT ] value [, value ]* ')'
      [ WITHIN DISTINCT '(' expression [, expression ]* ')' ]
      [ FILTER '(' WHERE condition ')' ]
  |   agg '(' '*' ')' [ FILTER (WHERE condition) ]
```

where `agg` is one of the operators in the following table.

If `FILTER` is present, the aggregate function only considers rows for
which condition evaluates to `TRUE`.

If `DISTINCT` is present, duplicate argument values are eliminated
before being passed to the aggregate function.

If `WITHIN DISTINCT` is present, argument values are made distinct
within each value of specified keys before being passed to the
aggregate function.

*Important*: the aggregate result type is the same as the type of the
value aggregated.  Since an aggregate combines multiple values, this
can cause overflows in the computation, which can cause runtime
exceptions.  We recommend to use explicit casts in SQL programs
converting the aggregated values to a data type wide enough to store
all intermediate aggregation results.  Example:

Instead of `SELECT SUM(col)`, you should write `SELECT SUM(CAST col AS
DECIMAL(10, 4))` if you expect 10-digit results to be possible.

<table>
  <tr>
    <th>Aggregate</th>
    <th>Description</th>
  </tr>
  <tr>
     <td><a id="array_agg"></a><code>ARRAY_AGG([ ALL | DISTINCT ] value [ RESPECT NULLS | IGNORE NULLS ] )</code></td>
     <td>Gathers all values in an array.  The order of the values in the array is unspecified (but it is deterministic).</td>
  </tr>
  <tr>
     <td><a id="avg"></a><code>AVG( [ ALL | DISTINCT ] numeric)</code></td>
     <td>Returns the average (arithmetic mean) of numeric across all input values</td>
  </tr>
  <tr>
     <td><a id="arg_max"></a><code>ARG_MAX(value, compared)</code></td>
     <td>Returns a <code>value</code> for one of the rows containing the maximum value of <code>compared</code> in the group.
         The rule for selecting the value is not specified if there are multiple rows with the same maximum value.</td>
  </tr>
  <tr>
     <td><a id="arg_min"></a><code>ARG_MIN(value, compared)</code></td>
     <td>Returns a  <code>value</code> for one of the rows containing the minimum value of <code>compared</code> in the group.
         This rule for selecting the value is not specified if there are multiple rows with the same minimum value.</td>
  </tr>
  <tr>
     <td><a id="bit_and"></a><code>BIT_AND( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the bitwise AND of all non-null input values, or null if none; integer and binary types are supported</td>
  </tr>
  <tr>
     <td><a id="bit_or"></a><code>BIT_OR( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the bitwise OR of all non-null input values, or null if none; integer and binary types are supported</td>
  </tr>
  <tr>
     <td><a id="bit_xor"></a><code>BIT_XOR( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the bitwise XOR of all non-null input values, or null if none; integer and binary types are supported</td>
  </tr>
  <tr>
     <td><a id="countstar"></a><code>COUNT(*)</code></td>
     <td>Returns the number of input rows</td>
  </tr>
  <tr>
     <td><a id="count"></a><code>COUNT( [ ALL | DISTINCT ] value [, value ]*)</code></td>
     <td>Returns the number of input rows for which value is not null.  If the argument contains multiple expressions, it counts only expressions where *all* fields are non-null.</td>
  </tr>
  <tr>
     <td><a id="countif"></a><code>COUNTIF( boolean )</code></td>
     <td>Returns the number of input rows for which the condition is true.</td>
  </tr>
  <tr>
     <td><a id="every"></a><code>EVERY(condition)</code></td>
     <td>Returns <code>TRUE</code> if all of the values of condition are <code>TRUE</code></td>
  </tr>
  <tr>
     <td><a id="logical_and"></a><code>LOGICAL_AND</code> or <code>BOOL_AND</code></td>
     <td>Same as <code>EVERY</code></td>
  </tr>
  <tr>
     <td><a id="logical_or"></a><code>LOGICAL_OR</code> or <code>BOOL_OR</code></td>
     <td>Same as <code>SOME</code></td>
  </tr>
  <tr>
     <td><a id="max"></a><code>MAX( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the maximum value of value across all input values</td>
  </tr>
  <tr>
     <td><a id="min"></a><code>MIN( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the minimum value of value across all input values</td>
  </tr>
  <tr>
     <td><a id="some"></a><code>SOME(condition)</code></td>
     <td>Returns <code>TRUE</code> if one or more of the values of condition is <code>TRUE</code></td>
  </tr>
  <tr>
     <td><a id="sum"></a><code>SUM( [ ALL | DISTINCT ] numeric)</code></td>
     <td>Returns the sum of numeric across all input values</td>
  </tr>
  <tr>
     <td><a id="stddev"></a><code>STDDEV( [ ALL | DISTINCT ] value)</code></td>
     <td>Synonym for <code>STDDEV_SAMP</code></td>
  </tr>
  <tr>
     <td><a id="stddev_pop"></a><code>STDDEV_POP( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the population standard deviation of numeric across all input values</td>
  </tr>
  <tr>
     <td><a id="stddev_samp"></a><code>STDDEV_SAMP( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the sample standard deviation of numeric across all input values</td>
  </tr>
</table>

Comparisons like `MAX`, `MIN`, `ARG_MIN`, and `ARG_MAX` are defined
for all data types, and they use the standard [comparison
operations](comparisons.md).

If `FILTER` is specified, then only the input rows for which the
filter_clause evaluates to true are fed to the aggregate function;
other rows are discarded. For example:

```sql
SELECT
    count(*) AS unfiltered,
    count(*) FILTER (WHERE i < 5) AS filtered
FROM TABLE
```

In addition, the following two constructors act as aggregates:

<a id="array"></a>
<a id="map"></a>
| Constructor        | Description                                                                                                                           | Example                                                                                                    |
|--------------------|---------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `ARRAY(sub-query)` | Creates an array from the result of a sub-query. If the subquery returns a tuple, the array will be an array of tuples.               | `SELECT ARRAY(SELECT empno FROM emp)` or `SELECT ARRAY(SELECT empno, dept FROM emp)`                       |
| `MAP(sub-query)`   | Creates a map from the result of a sub-query that returns two columns. If multiple entries have the same key, the largest value wins. | `SELECT MAP(SELECT empno, deptno FROM emp)`                                                                |

## Window aggregate functions

A `SELECT` expression in the [SQL grammar](grammar.md) can also
contain a [window aggregate function](grammar.md#window-aggregates).
The following window aggregate functions are supported:

<table>
  <tr>
    <th>Aggregate</th>
    <th>Description</th>
  </tr>
  <tr>
    <td><a id="window-avg"></a>AVG(numeric)</td>
    <td>Returns the average (arithmetic mean) of numeric across all values in window</td>
  </tr>
  <tr>
    <td><a id="window-count"></a><code>COUNT(</code>value [, value ]*<code>)</code></td>
    <td>Returns the number of rows in window for which value is not null</td>
  </tr>
  <tr>
    <td><a id="window-countstar"></a><code>COUNT(*)</code></td>
    <td>Returns the number of rows in window</td>
  </tr>
  <tr>
    <td><a id="dense_rank"></a><code>DENSE_RANK()</code></td>
    <td>Returns the rank of the current row without gaps.  `DENSE_RANK` is currently only supported if
        the window is used to compute a TopK aggregate.</td>
  </tr>
  <tr>
    <td><a id="lag"></a><code>LAG(</code><em>expression</em>, [<em>offset</em>, [ <em>default</em> ] ]<code>)</code></td>
    <td>Returns <em>expression</em> evaluated at the row that is <em>offset</em> rows before the current row
        within the partition; if there is no such row, instead returns <em>default</em>.
        Both <em>offset</em> and <em>default</em> are evaluated with respect to the current row.
        If omitted, <em>offset</em> defaults to 1 and <em>default</em> to <code>NULL</code>.</td>
  </tr>
  <tr>
    <td><a id="lead"></a><code>LEAD(</code><em>expression</em>, [<em>offset</em>, [ <em>default</em> ] ]<code>)</code></td>
    <td>Returns <em>expression</em> evaluated at the row that is <em>offset</em> rows after the current row
        within the partition; if there is no such row, instead returns <em>default</em>.
        Both <em>offset</em> and <em>default</em> are evaluated with respect to the current row.
        If omitted, <em>offset</em> defaults to 1 and <em>default</em> to <code>NULL</code>.</td>
  </tr>
  <tr>
    <td><a id="window-max"></a><code>MAX</code>(<em>expression</em>)</td>
    <td>Returns the maximum value of <em>expression</em> across all values in window</td>
  </tr>
  <tr>
    <td><a id="window-min"></a><code>MIN</code>(<em>expression</em>)</td>
    <td>Returns the minimum value of <em>expression</em> across all values in window</td>
  </tr>
  <tr>
    <td><a id="rank"></a><code>RANK()</code></td>
    <td>Returns the rank of the current row with gaps.  `DENSE_RANK` is currently only supported if
        the window is used to compute a TopK aggregate.</td>
  </tr>
  <tr>
    <td><a id="row_number"></a><code>ROW_NUMBER()</code></td>
    <td>Returns the number of the current row within its partition, counting from 1.
    `ROW_NUMBER` is currently only supported if the window is used to compute a TopK aggregate.</td>
  </tr>
  <tr>
    <td><a id="window-sum"></a><code>SUM</code>(<em>numeric</em>)</td>
    <td>Returns the sum of <em>numeric</em> across all values in window</td>
  </tr>
</table>

Currently, the window aggregate functions `RANK`, `DENSE_RANK` and
`ROW_NUMBER` are only supported if the compiler detects that they are
being used to implement a TopK pattern.  This pattern is expressed in
SQL with the following structure:

```sql
SELECT * FROM (
   SELECT empno,
          row_number() OVER (ORDER BY empno) rn
   FROM empsalary) emp
WHERE rn < 3
```

## Pivots

The SQL `PIVOT` operation can be used to turn rows into columns.  It
usually replaces a `GROUP-BY` operation when the group keys are known
in advance.  Instead of producing one row for each group, `PIVOT` can
produce one *column* for each group.

### Syntax

```
PIVOT ( { aggregate_expression [ AS aggregate_expression_alias ] } [ , ... ]
    FOR column_with_data IN ( column_list ) )
```

### Parameters

- aggregate_expression
  Specifies an aggregate expression (`SUM`, `COUNT(DISTINCT )`, etc.).

- aggregate_expression_alias
  Specifies a column name for the aggregate expression.

- column_with_data
  A column that produces all the values that will become new
  column names.

- column_list
  Columns that show the pivoted data.

### Example

```sql
CREATE TABLE FURNITURE (
   type VARCHAR,
   year INTEGER,
   count INTEGER
);
INSERT INTO FURNITURE VALUES
  ('chair', 2020, 4),
  ('table', 2021, 3),
  ('chair', 2021, 4),
  ('desk', 2023, 1),
  ('table', 2023, 2);

SELECT year, type, SUM(count) FROM FURNITURE GROUP BY year,type;
year | type  | sum
-------------------
2020 | chair | 4
2021 | table | 3
2021 | chair | 4
2023 | desk  | 1
2023 | table | 2
(5 rows)

SELECT * FROM FURNITURE
PIVOT (
    SUM(count) AS ct
    FOR type IN ('desk' AS desks, 'table' AS tables, 'chair' as chairs)
);

year | desks | tables | chairs
------------------------------
2020 |       |        |    4
2021 |       |     3  |    4
2023 |     1 |     2  |
(3 rows)
```

Notice how the same information is presented in a tabular form where
we have a column for each type of object.  PIVOTs require all the
possible "type"s to be specified when the query is written.  Notice
that if we add an additional type, the `GROUP BY` query will produce a
correct result, while the `PIVOT` query will produce the same result.

```sql
INSERT INTO FURNITURE VALUES ('bed', 2020, 5);
SELECT year, type, SUM(count) FROM FURNITURE GROUP BY year,type;
year | type  | sum
-------------------
2020 | chair | 4
2020 | bed   | 5
2021 | table | 3
2021 | chair | 4
2023 | desk  | 1
2023 | table | 2
(6 rows)

SELECT * FROM FURNITURE
PIVOT (
    SUM(count) AS ct
    FOR type IN ('desk' AS desks, 'table' AS tables, 'chair' as chairs)
);

year | desks | tables | chairs
------------------------------
2020 |       |        |    4
2021 |       |     3  |    4
2023 |     1 |     2  |
(3 rows)
```
