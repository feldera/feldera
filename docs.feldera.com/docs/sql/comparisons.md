# Comparison Operations

The following operations can take operands with multiple data types
but always return a Boolean value (sometimes nullable):
<table>
  <caption>Comparison Operations</caption>
  <tr>
    <th>Operation</th>
    <th>Definition</th>
    <th>Observation</th>
  </tr>
  <tr>
    <td><a id="eq"></a><code>=</code></td>
    <td>equality test</td>
    <td>rejected for <a href="#comparing-row-values"><code>ROW</code> values</a></td>
  </tr>
  <tr>
    <td><a id="ne"></a><code>&lt;&gt;</code></td>
    <td>inequality test</td>
    <td>rejected for <a href="#comparing-row-values"><code>ROW</code> values</a></td>
  </tr>
  <tr>
    <td><a id="neq"></a><code>!=</code></td>
    <td>inequality test, same as above</td>
    <td>rejected for <a href="#comparing-row-values"><code>ROW</code> values</a></td>
  </tr>
  <tr>
    <td><a id="gt"></a><code>&gt;</code></td>
    <td>greater than</td>
    <td></td>
  </tr>
  <tr>
    <td><a id="lt"></a><code>&lt;</code></td>
    <td>less than</td>
    <td></td>
  </tr>
  <tr>
    <td><a id="gte"></a><code>&gt;=</code></td>
    <td>greater or equal</td>
    <td></td>
  </tr>
  <tr>
    <td><a id="lte"></a><code>&lt;=</code></td>
    <td>less or equal</td>
    <td></td>
  </tr>
  <tr>
    <td><a id="isnull"></a><code>IS NULL</code></td>
    <td>true if operand is <code>NULL</code></td>
    <td></td>
  </tr>
  <tr>
    <td><a id="isnotnull"></a><code>IS NOT NULL</code></td>
    <td>true if operand is not <code>NULL</code></td>
    <td></td>
  </tr>
  <tr>
    <td><a id="nne"></a><code>&lt;=&gt;</code></td>
    <td>equality check that treats <code>NULL</code> values as equal</td>
    <td>result is not nullable</td>
  </tr>
  <tr>
    <td><a id="distinct"></a><code>IS DISTINCT FROM</code></td>
    <td>check if two values are not equal, treating <code>NULL</code> as equal</td>
    <td>result is not nullable</td>
  </tr>
  <tr>
    <td><a id="notdistinct"></a><code>IS NOT DISTINCT FROM</code></td>
    <td>check if two values are the same, treating <code>NULL</code> values as equal</td>
    <td>result is not nullable</td>
  </tr>
  <tr>
    <td><a id="between"></a><code>BETWEEN [ASYMMETRIC] ... AND ...</code></td>
    <td><code>x BETWEEN a AND b</code> is the same as <code>a &lt;= x AND x &lt;= b</code></td>
    <td>inclusive at both endpoints</td>
  </tr>
  <tr>
    <td><a id="notbetween"></a><code>NOT BETWEEN [ASYMMETRIC] ... AND ...</code></td>
    <td>The <code>NOT</code> of the previous operator</td>
    <td>not inclusive at either endpoint</td>
  </tr>
  <tr>
    <td><a id="symmetric-between"></a><code>BETWEEN SYMMETRIC ... AND ...</code></td>
    <td><code>x BETWEEN SYMMETRIC a AND b</code> is the same as <code>(a &lt;= x AND x &lt;= b) OR (b &lt;= x AND x &lt;= a)</code></td>
    <td>inclusive at both endpoints; order of endpoints does not matter</td>
  </tr>
  <tr>
    <td><a id="symmetric-notbetween"></a><code>NOT BETWEEN SYMMETRIC ... AND ...</code></td>
    <td>The <code>NOT</code> of the previous operator</td>
    <td>not inclusive at either endpoint</td>
  </tr>
  <tr>
    <td><a id="in"></a><code>... [NOT] IN ...</code></td>
    <td>checks whether value appears/does not appear in a list or set</td>
    <td></td>
  </tr>
  <tr>
    <td><a id="exists"></a><code>EXISTS query</code></td>
    <td>check whether query results have at least one row</td>
    <td></td>
  </tr>
</table>

Note that the SQL standard mandates `IS NULL` to return `true` for a
`ROW` object where all fields are `NULL` (similarly, `IS NOT NULL` is
required to return `false`).  Our compiler diverges from the standard,
returning `false` for `ROW(null) is null`.

## Comparing complex values

Comparison operations (`=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`, `<=>`,
`IS NULL`, `IS NOT NULL`) are defined on all data types, even generic
and recursive data types (including `ARRAY`, `MAP`, `ROW`, `VARIANT`,
user-defined types).  The one exception is that `=`, `<>`, and `!=`
are [rejected on `ROW` values](#comparing-row-values).

Equality needs no notion of order.  Two `ARRAY` values are equal when
they have the same length and equal elements at each index; two `MAP`
values are equal when they have exactly the same keys, each with an
equal value.

The ordering comparisons `<`, `<=`, `>` and `>=` do need one, and they
are lexicographic on the structure of the type: the two values are
walked in parallel, and the result is decided by the first position
where they differ.  Which positions are walked, and in what order,
depends on the type:

- the fields of a `ROW`, in the order they are declared;
- the elements of an `ARRAY`, by increasing index;
- the entries of a `MAP`, by increasing key; each entry contributes
  its key first and then its value.  A map has no order of its own, so
  the order in which a literal writes its entries never matters:
  `MAP['b', 1, 'a', 2]` and `MAP['a', 2, 'b', 1]` are the same value.

A `NULL` is smaller than any other value at the position where it
occurs, whatever the `NULLS FIRST` or `NULLS LAST` clause of an
enclosing `ORDER BY` says; that clause orders the rows, not the
insides of a value.  For example, `ARRAY[NULL] < ARRAY[1]` is `true`.
Map keys are never `NULL`, so this affects fields, array elements, and
map values.

### Comparing `ARRAY` and `MAP` values

For `ARRAY` and `MAP` values `=` is equivalent to `IS NOT DISTINCT
FROM`, and `<>` is equivalent to `IS DISTINCT FROM`: two `NULL`
elements compare as equal instead of producing `NULL`.  For example,
`ARRAY[1, NULL] = ARRAY[1, NULL]` is `true`, whereas the three-valued
logic of the SQL standard would give `NULL`.  Comparing two non-`NULL`
arrays or maps therefore never produces `NULL`.

The equivalence covers the elements, not the operands themselves.
When one operand is a `NULL` array or a `NULL` map, `=` produces
`NULL` as any other comparison does, while `IS NOT DISTINCT FROM`
produces `false`.

### Comparing `ROW` values {#comparing-row-values}

Feldera rejects `=`, `<>`, and `!=` between `ROW` values.

The reason is that the standard meaning of these operators on `ROW`
values surprises most users.  The standard compares the fields
pairwise under three-valued logic: the result is `false` as soon as
one pair of fields differs, `true` when every pair is equal, and
`NULL` otherwise.  A `NULL` field therefore does not simply make the
whole comparison `NULL`:

<table>
  <tr>
    <th>Comparison</th>
    <th>Standard result</th>
    <th>Why</th>
  </tr>
  <tr>
    <td><code>ROW(1, NULL) = ROW(2, NULL)</code></td>
    <td><code>false</code></td>
    <td>the first fields differ, so the rest does not matter</td>
  </tr>
  <tr>
    <td><code>ROW(1, NULL) = ROW(1, NULL)</code></td>
    <td><code>NULL</code></td>
    <td>the first fields are equal, the second pair is unknown</td>
  </tr>
  <tr>
    <td><code>ROW(1, NULL) &lt;&gt; ROW(2, NULL)</code></td>
    <td><code>true</code></td>
    <td>one pair differs, so the two rows are known to differ</td>
  </tr>
</table>

Two different values thus compare as `false`, while two identical ones
compare as `NULL`: a `ROW` value is not equal to itself once any field
is `NULL`.  A query that gets this wrong silently drops or keeps the
wrong rows, because `WHERE` treats `NULL` as `false`.  Rejecting the
comparison forces the choice to be explicit.

Write [`IS NOT DISTINCT FROM`](#notdistinct) in place of `=`, and [`IS
DISTINCT FROM`](#distinct) in place of `<>` and `!=`.  Both treat
`NULL` values as equal and always produce a Boolean, never `NULL`.
[`<=>`](#nne) is a shorthand for `IS NOT DISTINCT FROM`, and is
accepted as well:

```sql
CREATE TYPE point AS (x INT, y INT);
CREATE TABLE T(p point, q point);
-- Rejected:
--   CREATE VIEW v AS SELECT p = q, p <> q FROM T;
CREATE VIEW v AS SELECT p IS NOT DISTINCT FROM q, p IS DISTINCT FROM q FROM T;
CREATE VIEW w AS SELECT p <=> q FROM T;
```

A user-defined type declared with `CREATE TYPE ... AS (...)` is a
`ROW` type (see [user-defined types](types.md#user-defined-types)), so
the restriction applies to values of such types as well.

A program can request `ROW` equality without writing `=`.  The
compiler rejects each of the following forms; the last column gives
the accepted rewrite:

<table>
  <tr>
    <th>Rejected</th>
    <th>Why</th>
    <th>Write instead</th>
  </tr>
  <tr>
    <td><code>r = s</code></td>
    <td>explicit equality</td>
    <td><code>r IS NOT DISTINCT FROM s</code>, or <code>r &lt;=&gt; s</code></td>
  </tr>
  <tr>
    <td><code>r &lt;&gt; s</code>, <code>r != s</code></td>
    <td>explicit inequality</td>
    <td><code>r IS DISTINCT FROM s</code></td>
  </tr>
  <tr>
    <td><code>l JOIN r ON l.p = r.p</code></td>
    <td>the join condition is an equality test</td>
    <td><code>l JOIN r ON l.p IS NOT DISTINCT FROM r.p</code></td>
  </tr>
  <tr>
    <td><code>l NATURAL JOIN r</code>, <code>l JOIN r USING (p)</code></td>
    <td>equality on the shared <code>ROW</code> columns is implied</td>
    <td>an explicit <code>ON ... IS NOT DISTINCT FROM ...</code> condition</td>
  </tr>
  <tr>
    <td><code>r IN (v1, v2)</code></td>
    <td>expands to <code>r = v1 OR r = v2</code></td>
    <td><code>r IS NOT DISTINCT FROM v1 OR r IS NOT DISTINCT FROM v2</code></td>
  </tr>
  <tr>
    <td><code>r IN (SELECT p FROM s)</code></td>
    <td>expands to an equality test</td>
    <td><code>EXISTS (SELECT 1 FROM s WHERE s.p IS NOT DISTINCT FROM r)</code></td>
  </tr>
  <tr>
    <td><code>CASE r WHEN v THEN a ELSE b END</code></td>
    <td>expands to <code>r = v</code></td>
    <td><code>CASE WHEN r IS NOT DISTINCT FROM v THEN a ELSE b END</code></td>
  </tr>
  <tr>
    <td><code>NULLIF(r, v)</code></td>
    <td>returns <code>NULL</code> when <code>r = v</code></td>
    <td><code>CASE WHEN r IS NOT DISTINCT FROM v THEN NULL ELSE r END</code></td>
  </tr>
  <tr>
    <td><code>(a, b) = (c, d)</code>, <code>(a, b) &lt;&gt; (c, d)</code></td>
    <td>a row constructor builds a <code>ROW</code> value</td>
    <td><code>(a, b) IS [NOT] DISTINCT FROM (c, d)</code></td>
  </tr>
</table>

A join on `ROW` values matches two rows when their fields are pairwise
not distinct, so two `NULL` fields match, and so do two `NULL` rows:

```sql
CREATE TYPE point AS (x INT, y INT);
CREATE TABLE l(p point);
CREATE TABLE r(p point);
-- Rejected: SELECT * FROM l JOIN r ON l.p = r.p;
CREATE VIEW v AS SELECT * FROM l JOIN r ON l.p IS NOT DISTINCT FROM r.p;
```

An `IN` subquery becomes a join.  SQL has no `IS NOT DISTINCT FROM
ANY`, so rewrite the subquery as a correlated `EXISTS`:

```sql
-- Rejected: SELECT * FROM l WHERE l.p IN (SELECT p FROM r);
CREATE VIEW w AS SELECT * FROM l
WHERE EXISTS (SELECT 1 FROM r WHERE r.p IS NOT DISTINCT FROM l.p);
```

The following remain legal on `ROW` values:

- the ordering comparisons `<`, `<=`, `>`, `>=`;
- `IS [NOT] DISTINCT FROM`, `<=>`, and `IS [NOT] NULL`;
- constructs that group values rather than compare them, because they
  use distinctness and not equality: `GROUP BY`, `SELECT DISTINCT`,
  `PARTITION BY`, `UNION`, `INTERSECT`, and `EXCEPT`.

The restriction covers a comparison written directly between two row
constructors, such as `(a, b) = (c, d)`: a row constructor builds a
`ROW` value like any other.  Compare the fields, `a = c AND b = d`, if
that is what you mean.

## Other conditional operators

<table>
  <tr>
    <td><a id="case"></a><code>CASE value WHEN value1 [, value11 ]* THEN result1 [ WHEN valueN [, valueN1 ]* THEN resultN ]* [ ELSE resultZ ] END</code></td>
    <td>Simple case expression: returns the result corresponding to the first valueN that matches value.</td>
  </tr>
  <tr>
    <td><code>CASE WHEN condition1 THEN result1 [ WHEN conditionN THEN resultN ]* [ ELSE resultZ ] END</code></td>
    <td>Searched case: returns result corresponding to first condition that evaluates to 'true'.</td>
  </tr>
  <tr>
    <td><a id="coalesce"></a><code>COALESCE(value0, value1 [, valueN ]*)</code></td>
    <td>Returns the first non-null value. For example, <code>COALESCE(NULL, 5)</code> returns 5.</td>
  </tr>
  <tr>
    <td><a id="greatest"></a><code>GREATEST( expr [, expr ]* )</code></td>
    <td>The largest of a number of expressions; if any argument is <code>NULL</code>, the result is <code>NULL</code>.</td>
  </tr>
  <tr>
    <td><a id="greatest_ignore_nulls"></a><code>GREATEST_IGNORE_NULLS( expr [, expr ]* )</code></td>
    <td>The largest of a number of expressions; only if all arguments are <code>NULL</code>, the result is <code>NULL</code>; otherwise <code>NULL</code> values are ignored.</td>
  </tr>
  <tr>
    <td><a id="if"></a><code>IF( condition, ifTrue, ifFalse )</code></td>
    <td>Returns ifTrue if the condition evaluates to 'true', returns ifFalse otherwise.</td>
  </tr>
  <tr>
    <td><a id="ifnull"></a><code>IFNULL( left, right )</code></td>
    <td>Equivalent to <code>COALESCE(left, right)</code>.</td>
  </tr>
  <tr>
    <td><a id="least"></a><code>LEAST( expr [, expr ]* )</code></td>
    <td>The smallest of a number of expressions; if any argument is <code>NULL</code>, the result is <code>NULL</code>.</td>
  </tr>
  <tr>
    <td><a id="least_ignore_nulls"></a><code>LEAST_IGNORE_NULLS( expr [, expr ]* )</code></td>
    <td>The smallest of a number of expressions; only if all arguments are <code>NULL</code>, the result is <code>NULL</code>; otherwise <code>NULL</code> values are ignored.</td>
  </tr>
  <tr>
    <td><a id="nullif"></a><code>NULLIF(value0, value1)</code></td>
    <td>Returns `NULL` if value0 and value1 are the same. For example, <code>NULLIF(5, 5)</code> returns NULL; <code>NULLIF(5, 0)</code> returns 5.</td>
  </tr>
</table>