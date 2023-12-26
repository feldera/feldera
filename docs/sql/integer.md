# Integer Operations

There are four supported integer datatypes, `TINYINT` (8 bits),
`SMALLINT` (16 bits), `INTEGER` (32 bits), and `BIGINT` (64
bits).  These are represented as two's complement values, and
computations on these types obey the standard two's complement
semantics, including overflow.

The legal operations are `+` (plus, unary and binary), `-` (minus,
unary and binary), `*` (multiplication), `/` (division), `%`
(modulus).

The modulo operator treats modulo by -1 (`x % -1`) as a special
edge case, and returns 0 for it.

Casting a string to an integer type will produce a runtime error if the
string cannot be interpreted as a number.

Division or modulus by zero cause a runtime error.

## Predefined functions on integer values

<table>
  <caption>Predefined functions on integer values</caption>
  <tr>
    <th>Function</th>
    <th>Description</th>
  </tr>
  <tr>
    <td><code>ABS(value)</code></td>
    <td>return absolute value.</td>
  </tr>
  <tr>
    <td><code>MOD(left, right)</code></td>
    <td>integer modulus. Same as <code>left % right</code>.</td>
  </tr>
</table>

## Operations not supported

Non-deterministic functions, such as `RAND` are currently not
supported in DBSP.
