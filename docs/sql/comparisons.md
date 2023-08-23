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
    <td><code>=</code></td>
    <td>equality test</td>
    <td></td>
  </tr>
  <tr>
    <td><code>&lt;&gt;</code></td>
    <td>inequality test</td>
    <td></td>
  </tr>
  <tr>
    <td><code>&gt;</code></td>
    <td>greater than</td>
    <td></td>
  </tr>
  <tr>
    <td><code>&lt;</code></td>
    <td>less than</td>
    <td></td>
  </tr>
  <tr>
    <td><code>&gt;=</code></td>
    <td>greater or equal</td>
    <td></td>
  </tr>
  <tr>
    <td><code>&lt;=</code></td>
    <td>less or equal</td>
    <td></td>
  </tr>
  <tr>
    <td><code>IS NULL</code></td>
    <td>true if operand is <code>NULL</code></td>
    <td></td>
  </tr>
  <tr>
    <td><code>IS NOT NULL</code></td>
    <td>true if operand is not <code>NULL</code></td>
    <td></td>
  </tr>
  <tr>
    <td><code>&lt;=&gt;</code></td>
    <td>equality check that treats <code>NULL</code> values as equal</td>
    <td>result is not nullable</td>
  </tr>
  <tr>
    <td><code>IS DISTINCT FROM</code></td>
    <td>check if two values are not equal, treating <code>NULL</code> as equal</td>
    <td>result is not nullable</td>
  </tr>
  <tr>
    <td><code>IS NOT DISTINCT FROM</code></td>
    <td>check if two values are the same, treating <code>NULL</code> values as equal</td>
    <td>result is not nullable</td>
  </tr>
  <tr>
    <td><code>BETWEEN ... AND ...</code></td>
    <td><code>x BETWEEN a AND b</code> is the same as <code>a &lt;= x AND x &lt;= b</code></td>
    <td>inclusive at both endpoints</td>
  </tr>
  <tr>
    <td><code>NOT BETWEEN ... AND ...</code></td>
    <td>The <code>NOT</code> of the previous operator</td>
    <td>not inclusive at either endpoint</td>
  </tr>
  <tr>
    <td><code>... IN ...</code></td>
    <td>checks whether value appears in a list or set</td>
    <td></td>
  </tr>
  <tr>
    <td><code>&lt;OP&gt; ANY SET</code></td>
    <td>check if any of the values in a set compares properly</td>
    <td>Example: 10 &lt;= ANY (VALUES 10, 20, 30)</td>
  </tr>
  <tr>
    <td><code>&lt;OP&gt; ALL SET</code></td>
    <td>check if all the values in a set compare properly</td>
    <td>Example: 10 &lt;= ALL (VALUES 10, 20, 30)</td>
  </tr>
  <tr>
    <td><code>EXISTS query</code></td>
    <td>check whether query results have at least one row</td>
    <td></td>
  </tr>
  <tr>
    <td><code>UNIQUE query</code></td>
    <td>check whether the result of a query contains no duplicates</td>
    <td>ignores <code>NULL</code> values</td>
  </tr>
</table>

# Other conditional operators

<table>
  <tr>
    <td><code>CASE value
        WHEN value1 [, value11 ]* THEN result1
        [ WHEN valueN [, valueN1 ]* THEN resultN ]*
        [ ELSE resultZ ]
        END</code>
    </td>
    <td>Simple case</td>
  </tr>
  <tr>
    <td><code>CASE WHEN condition1 THEN result1
           [ WHEN conditionN THEN resultN ]*
           [ ELSE resultZ ]
        END</code>
    </td>
    <td>Searched case</td>
  </tr>
  <tr>
    <td><code>NULLIF(value, value)</code></td>
    <td>Returns `NULL` if the values are the same.
        For example, <code>NULLIF(5, 5)</code> returns NULL; <code>NULLIF(5, 0)</code> returns 5.
    </td>
  </tr>
  <tr>
    <td><code>COALESCE(value, value [, value ]*)</code></td>
    <td>Provides a value if the first value is NULL.
        For example, <code>COALESCE(NULL, 5)</code> returns 5.
    </td>
  </tr>
</table>