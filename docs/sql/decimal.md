# Decimal data type

A synonym for the ``decimal`` type is ``numeric``.

A decimal number is characterized by two magnitudes: the *precision*,
which his the total number of decimal digits represented, and the
*scale*, which is the count of digits in the fractional part, to the
right of the decimal point.  For example, the number 3.1415 has a
precision of 5 and a scale of 4.

The type ``NUMERIC(precision, scale)`` specifies both precision and
scale, both of which must be constants.

The type ``NUMERIC(precision)`` is the same as ``NUMERIC(precision, 0)``.

The type ``NUMERIC`` is the same as ``NUMERIC(MAX_PRECISION, 0)``. 

The maximum precision supported is 128 binary digits (38 decimal
digits).  The maximum scale supported is 10 decimal digits.

## Operations available for the ``decimal`` type

The legal operations are ``+`` (plus, unary and binary), ``-`` (minus,
unary and binary), ``*`` (multiplication), ``/`` (division), ``%``
(modulus).

Division or modulus by zero cause a runtime error.

Casting a string to a decimal value will produce a run time error if
parsing fails.

## Predefined functions on Decimal Values

<table>
  <tr>
    <td><code>ROUND(value)</code></td>
    <td>same as <code>ROUND(value, 0)</code></td>
  </tr>
  <tr>
    <td><code>ROUND(value, digits)</code></td>
    <td>where <code>digits</code> is an integer value. Round the value to the specified number of <em>decimal</em> digits after the decimal point.</td>
  </tr>
  <tr>
    <td><code>ABS(value)</code></td>
    <td>absolute value</td>
  </tr>
  <tr>
    <td><code>CEIL(value)</code></td>
    <td>Ceiling function: nearest integer value greater than or equal to argument (result is a decimal value)</td>
  </tr>
  <tr>
    <td><code>FLOOR(value)</code></td>
    <td>Floor function: nearest integer value less than or equal to argument (result is a decimal value)</td>
  </tr>
  <tr>
    <td><code>SIGN(value)</code></td>
    <td>The "sign" function: -1, 0, or 1 showing if value is &lt;0, =0, or &gt;0.</td>
  </tr>
  <tr>
    <td><code>POWER(base, exponent)</code></td>
    <td>The power function, raising base to the power exponent.</td>
  </tr>
  <tr>
    <td><code>SQRT(value)</code></td>
    <td>Square root of value. Produces a runtime error for negative values.</td>
  </tr>
  <tr>
    <td><code>LN(value)</code></td>
    <td>The natural logarithm of value. Produces a runtime error for values less than or equal to zero.</td>
  </tr>
  <tr>
    <td><code>LOG10(value)</code></td>
    <td>The logarithm base 10 of value. Produces a runtime error for values less than or equal to zero.</td>
  </tr>
</table>

