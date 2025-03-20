# Operators

This table shows the operators associativity, starting from highest to lowest:

<table>
  <tr>
     <th>Operator</th>
     <th>Associativity</th>
     <th>Comments</th>
  </tr>
  <tr>
    <td><code>.</code></td>
    <td>left</td>
    <td>Field access</td>
  </tr>
  <tr>
    <td><code>::</code></td>
    <td>left</td>
    <td>Infix cast</td>
  </tr>
  <tr>
     <td><code>[ index ]</code></td>
     <td>left</td>
     <td>Collection element</td>
  </tr>
  <tr>
    <td><a id="plusminus"></a><code>+, -</code></td>
    <td>right</td>
    <td>Unary plus, minus</td>
  </tr>
  <tr>
    <td><a id="muldiv"></a><code>*, /, %, ||</code></td>
    <td>left</td>
    <td>arithmetic</td>
  </tr>
  <tr>
    <td><code>+, -</code></td>
    <td>left</td>
    <td>arithmetic</td>
  </tr>
  <tr>
    <td><a id="between"></a><code>BETWEEN, IN, LIKE, SIMILAR, OVERLAPS, CONTAINS</code></td>
    <td>N/A</td>
    <td></td>
  </tr>
  <tr>
    <td><code>&lt;, &gt;, =, &lt;=, &gt;=, &lt;&gt;, !=, &lt;=&gt;</code></td>
    <td>left</td>
    <td>comparisons</td>
  </tr>
  <tr>
    <td><a id="isnull"></a><code>IS NULL, IS FALSE, IS TRUE, IS UNKNOWN, IS NOT NULL, IS NOT TRUE, IS NOT FALSE, IS NOT UNKNOWN</code></td>
    <td>unary</td>
    <td></td>
  </tr>
  <tr>
    <td><code>NOT</code></td>
    <td>right</td>
    <td>Boolean</td>
  </tr>
  <tr>
    <td><code>AND</code></td>
    <td>left</td>
    <td>Boolean</td>
  </tr>
  <tr>
    <td><code>OR</code></td>
    <td>left</td>
    <td>Boolean</td>
  </tr>
</table>