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
    <td></td>
  </tr>
  <tr>
    <td><a id="ne"></a><code>&lt;&gt;</code></td>
    <td>inequality test</td>
    <td></td>
  </tr>
  <tr>
    <td><a id="ne"></a><code>!=</code></td>
    <td>inequality test, same as above</td>
    <td></td>
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
    <td><a id="between"></a><code>BETWEEN ... AND ...</code></td>
    <td><code>x BETWEEN a AND b</code> is the same as <code>a &lt;= x AND x &lt;= b</code></td>
    <td>inclusive at both endpoints</td>
  </tr>
  <tr>
    <td><a id="notbetween"></a><code>NOT BETWEEN ... AND ...</code></td>
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

Comparison operations (`=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`, `<=>`,
`IS NULL`, `IS NOT NULL`) are defined on all data types, even generic
and recursive data types (including `ARRAY`, `MAP`, `ROW`, `VARIANT`,
user-defined types).  For complex types, comparisons are performed
lexicographically on the type structure.  In such comparisons fields
with `NULL` values are compared smaller than any other value.

# Other conditional operators

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
    <td>Returns `NULL` if the value0 and value1 are the same. For example, <code>NULLIF(5, 5)</code> returns NULL; <code>NULLIF(5, 0)</code> returns 5.</td>
  </tr>
</table>