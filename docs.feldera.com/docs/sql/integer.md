# Integer Operations

There are four supported integer datatypes, `TINYINT` (8 bits),
`SMALLINT` (16 bits), `INTEGER` (32 bits), and `BIGINT` (64
bits).

The legal operations are `+` (plus, unary and binary), `-` (minus,
unary and binary), `*` (multiplication), `/` (division), `%`
(modulus).

Modulus involving negative numbers happens as follows:

For: ``mod = x % y``
- if ``x >= 0`` and ``y > 0`` then: ``x - (floor(x / y) * y)``
- if ``x >= 0`` and ``y < 0`` then: ``x % abs(y)``
- if ``x < 0`` and ``y > 0`` then: ``- abs(x) % y``
- if ``x < 0`` and ``y > 0`` then: ``- abs(x) % abs(y)``

Example:

<table>
    <caption>mod = x % y</caption>
    <tr>
        <th>x</th>
        <th>y</th>
        <th>mod</th>
    </tr>
    <tr>
        <td> 8 </td>
        <td> 3 </td>
        <td> 2 </td>
    </tr>
    <tr>
        <td>  8 </td>
        <td> -3 </td>
        <td>  2 </td>
    </tr>
    <tr>
        <td> -8 </td>
        <td>  3 </td>
        <td> -2 </td>
    </tr>
    <tr>
        <td> -8 </td>
        <td> -3 </td>
        <td> -2 </td>
    </tr>
</table>

Casting a string to an integer type will produce a runtime error if the
string cannot be interpreted as a number.

Division or modulus by zero cause a runtime error.

Operations that cause integer overflow or underflow (example: multiplication or division
of minimum integer value by -1) produce run time errors.

## Predefined functions on integer values

<table>
  <caption>Predefined functions on integer values</caption>
  <tr>
    <th>Function</th>
    <th>Description</th>
  </tr>
  <tr>
    <td><a id="abs"></a><code>ABS(value)</code></td>
    <td>return absolute value.</td>
  </tr>
  <tr>
    <td><a id="mod"></a><code>MOD(left, right)</code></td>
    <td>integer modulus. Same as <code>left % right</code>.</td>
  </tr>
  <tr>
    <td><a id="sequence"></a><code>SEQUENCE(start, end)</code></td>
    <td>returns an array of integers from start to end (inclusive). If end &lt; start, an empty array is returned. If any of the arguments are NULL, NULL is returned.</td>
  </tr>
</table>

## Operations not supported

Non-deterministic functions, such as `RAND` are currently not
supported in DBSP.
