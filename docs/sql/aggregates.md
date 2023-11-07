# Aggregate operations

## Standard aggregate functions

Aggregate functions are specified using the following grammar:

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

<table>
  <tr>
    <th>Aggregate</th>
    <th>Description</th>
  </tr>
  <tr>
     <td><code>AVG( [ ALL | DISTINCT ] numeric)</code></td>
     <td>Returns the average (arithmetic mean) of numeric across all input values</td>
  </tr>
  <tr>
     <td><code>COUNT(*)</code></td>
     <td>Returns the number of input rows</td>
  </tr>
  <tr>
     <td><code>COUNT( [ ALL | DISTINCT ] value [, value ]*)</code></td>
     <td>Returns the number of input rows for which value is not null (wholly not null if value is composite)</td>
  </tr>
  <tr>
     <td><code>EVERY(condition)</code></td>
     <td>Returns <code>TRUE</code> if all of the values of condition are <code>TRUE</code></td>
  </tr>
  <tr>
     <td><code>MAX( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the maximum value of value across all input values</td>
  </tr>
  <tr>
     <td><code>MIN( [ ALL | DISTINCT ] value)</code></td>
     <td>Returns the minimum value of value across all input values</td>
  </tr>
  <tr>
     <td><code>SOME(condition)</code></td>
     <td>Returns <code>TRUE</code> if one or more of the values of condition is <code>TRUE</code></td>
  </tr>
  <tr>
     <td><code>SUM( [ ALL | DISTINCT ] numeric)</code></td>
     <td>Returns the sum of numeric across all input values</td>
  </tr>
  <tr>
     <td><code>LOGICAL_OR</code> or <code>BOOL_OR</code></td>
     <td>Same as <code>SOME</code></td>
  </tr>
  <tr>
     <td><code>LOGICAL_AND</code> or <code>BOOL_AND</code></td>
     <td>Same as <code>EVERY</code></td>
  </tr>
</table>

If `FILTER` is specified, then only the input rows for which the
filter_clause evaluates to true are fed to the aggregate function;
other rows are discarded. For example:

```sql
SELECT
    count(*) AS unfiltered,
    count(*) FILTER (WHERE i < 5) AS filtered
FROM TABLE
```

## Window aggregate functions

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
</table>

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

