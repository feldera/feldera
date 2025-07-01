# Decimal Operations

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

> [!WARNING]
> This means that casting to ``DECIMAL`` or ``NUMERIC`` will round the value to a decimal with no fractional part.
> Example: ``SELECT CAST('0.5' AS DECIMAL)`` will return ``1`` as the scale is 0.

The maximum precision supported is 38 decimal digits.  The maximum
scale supported is 38 decimal digits.

## Operations available for the ``decimal`` type

The legal operations are ``+`` (plus, unary and binary), ``-`` (minus,
unary and binary), ``*`` (multiplication), ``/`` (division), ``%``
(modulus).

Modulus happens as follows:
For: ``mod = x % y``
- if ``x >= 0`` and ``y > 0`` then: ``x - (floor(x / y) * y)``
- if ``x >= 0`` and ``y < 0`` then: ``x % abs(y)``
- if ``x < 0`` and ``y > 0`` then: ``- abs(x) % y``
- if ``x < 0`` and ``y < 0`` then: ``- abs(x) % abs(y)``

Division or modulus by zero cause a runtime error.

Casting a string to a decimal value will produce a run time error if
parsing fails.

## Rounding while casting between Decimal types

Rounding is performed using [to nearest, ties away from zero](https://en.wikipedia.org/wiki/Rounding#Rounding_half_away_from_zero) strategy.

Example while casting from ``DECIMAL(8, 4)`` to ``DECIMAL(6, 2)``:

<table>
    <tr>
        <th>Input Value</th>
        <th>Output Value</th>
    </tr>
    <tr>
        <td>1234.1250</td>
        <td>1234.13</td>
    </tr>
    <tr>
        <td> -1234.1250</td>
        <td> -1234.13</td>
    </tr>
    <tr>
        <td>1234.1264</td>
        <td>1234.13</td>
    </tr>
    <tr>
        <td>1234.1234</td>
        <td>1234.12</td>
    </tr>
    <tr>
        <td> -1234.1264</td>
        <td> -1234.13</td>
    </tr>
    <tr>
        <td> -1234.1234</td>
        <td> -1234.12</td>
    </tr>
</table>

### Invalid casts between Decimal types

While casting to decimal types, if the current decimal number cannot be represented
with the specified precision and scale, a run time error is thrown.

Example:
Valid casts such as: ``CAST('1234.1234' AS DECIMAL(6, 2))`` will return ``1234.12``.
But invalid casts such as: ``CAST('1234.1234' AS DECIMAL(6, 3))`` will throw a run time error.

## Predefined functions on Decimal Values

<table>
  <tr>
    <td><a id="abs"></a><code>ABS(value)</code></td>
    <td>absolute value</td>
  </tr>
  <tr>
    <td><a id="bround"></a><code>BROUND(decimal, digits)</code></td>
    <td>Performs banker's rounding to the specified number of decimal digits after the decimal point.
        Negative values for "digits" are supported (result is a <code>DECIMAL</code> value).</td>
  </tr>
  <tr>
    <td><a id="ceil"></a><code>CEIL(value)</code></td>
    <td>Ceiling function: nearest integer value greater than or equal to argument (result is a <code>DECIMAL</code> value).</td>
  </tr>
  <tr>
    <td><a id="floor"></a><code>FLOOR(value)</code></td>
    <td>Floor function: nearest integer value less than or equal to argument (result is a <code>DECIMAL</code> value).</td>
  </tr>
  <tr>
    <td><a id="round"></a><code>ROUND(value)</code></td>
    <td>same as <code>ROUND(value, 0)</code>.</td>
  </tr>
  <tr>
    <td><a id="round2"></a><code>ROUND(value, digits)</code></td>
    <td>where <code>digits</code> is an integer value, which may be negative. Round the value to the specified number of <em>decimal</em> digits after the decimal point (result is a <code>DECIMAL</code> value).</td>
  </tr>
  <tr>
    <td><a id="sign"></a><code>SIGN(value)</code></td>
    <td>The "sign" function: -1, 0, or 1 showing if value is &lt;0, =0, or &gt;0 (result is a <code>DECIMAL</code> value).</td>
  </tr>
  <tr>
    <td><a id="trunc"></a><code>TRUNCATE(value)</code></td>
    <td>same as <code>TRUNCATE(value, 0)</code> (result is a <code>DECIMAL</code> value).</td>
  </tr>
  <tr>
    <td><a id="trunc2"></a><code>TRUNC(value [, digits] )</code></td>
    <td>same as <code>TRUNCATE(value [, digits])</code> (result is a <code>DECIMAL</code> value).</td>
  </tr>
  <tr>
    <td><a id="truncate"></a><code>TRUNCATE(value, digits)</code></td>
    <td>where <code>digits</code> is an integer value, which may be negative. Truncates the value to the specified number of <em>decimal</em> digits after the decimal point (result is a <code>DECIMAL</code> value).</td>
  </tr>
</table>

