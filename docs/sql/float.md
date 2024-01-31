# Floating point types

We support standard IEEE 754 floating point types.

`double` is a 64-bit standard FP value.  Accepted synonyms are
`float8` and `float64`.

`float` is a 32-bit standard FP value.  Accepted synonyms are
`float4`, and `float32`.

Floating point values include special values, such as `NaN` (not a
number), `-Infinity`, and `-Infinity`.  An alternative spelling for
`-Infinity` is `-inf`, and an alternative spelling for `Infinity` is
`inf`, and an alternative spelling for 'NaN' is 'nan'.  When written
as SQL literals, these values have to be surrounded by simple quotes:
`'inf'`.  Please note that these strings are case-sensitive and spaces
are ignored.

Infinity plus any finite value equals Infinity, as does Infinity plus
Infinity.  Infinity minus `Infinity` yields `NaN`.

`NaN` (not a number) value is used to represent undefined results.
An operation with a `NaN` input yields `NaN`.  The only exception
is when the operation's output does not depend on the `NaN` value:
an example is `NaN` raised to the zero power yields one.

In sorting order `NaN` is considered greater than all other values.

The legal operations are `+` (plus, unary and binary), `-` (minus,
unary and binary), `*` (multiplication), `/` (division).
(modulus).

Division by zero returns Infinity, (or `NaN` in case of `0e0 / 0e0`).
Modulus by zero return `NaN`.

Casting a string to a floating-point value will produce the value
`0` when parsing fails.

Casting a value that is out of the supported range to a floating
point type will produce a value that is `inf` or `-inf`.

Casting a floating-point value to string, `float` is rounded off
to 6 decimal places and `double` is rounded off to 15 decimal places.

Please note that numeric values with a decimal point have the
`decimal` type by default.  To write a floating-point literal you have
to include the `e` for exponent using the following grammar:

digits`.`digits[`e`[`+-`]digits]

[digits]`.`digits[`e`[`+-`]digits]

Alternatively, you can use an explicit cast:

```SQL
REAL '1.23'  -- string style
1.23::REAL   -- PostgreSQL style
```

## Predefined functions on Floating-point Values

<table>
  <tr>
    <td><code>ABS(value)</code></td>
    <td>absolute value</td>
  </tr>
  <tr>
    <td><code>CEIL(value)</code></td>
    <td>Ceiling function: nearest integer value greater than or equal to argument (result is a floating point value)</td>
  </tr>
  <tr>
    <td><code>FLOOR(value)</code></td>
    <td>Floor function: nearest integer value less than or equal to argument (result is a floating point value)</td>
  </tr>
  <tr>
    <td><code>TRUNCATE(value)</code></td>
    <td>Returns the integer portion of the number. This is true truncation, no rounding is performed.</td>
  </tr>
  <tr>
    <td><code>ROUND(value)</code></td>
    <td>Rounds to the nearest integer and returns it. Rounding follows "Bankers Rounding" (rounds 0.5 to the nearest even number) strategy.</td>
  </tr>
  <tr>
    <td><code>POWER(BASE, EXPONENT)</code></td>
    <td>The power function, raising <code>BASE</code> to the power <code>EXPONENT</code></td>
  </tr>
  <tr>
    <td><code>SQRT(value)</code></td>
    <td>Square root of value. Produces a runtime error for negative values.</td>
  </tr>
  <tr>
    <td><code>LN(value)</code></td>
    <td>The natural logarithm of value. Returns `-inf` for 0. Produces a runtime error for negative numbers.</td>
  </tr>
  <tr>
    <td><code>LOG(value, [, base])</code></td>
    <td>The logarithm of value to base, or base e if it is not present.  Produces a runtime error for negative values for either value or base. Returns `-inf` for base 0.</td>
  </tr>
  <tr>
    <td><code>LOG10(value)</code></td>
    <td>The logarithm base 10 of value. Returns `-inf` for 0. Produces a runtime error for negative numbers.</td>
  </tr>
  <tr>
    <td><code>IS_INF(value)</code></td>
    <td>Returns true if the value is infinite.</td>
  </tr>
  <tr>
    <td><code>IS_NAN(value)</code></td>
    <td>Returns true if the value is NaN. Note that two NaN values may not be equal.</td>
  </tr>
  <tr>
    <td><code>SIN(value)</code></td>
    <td>The sine of value as radians. <code>sin</code> only supports argument of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>COS(value)</code></td>
    <td>The cosine of value as radians. <code>cos</code> only supports argument of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>PI</code></td>
    <td>Returns the approximate value of <code>PI</code> as double. Note that <code>()</code> is not allowed. Example: <code>SELECT PI;</code></td>
  </tr>
  <tr>
    <td><code>TAN(value)</code></td>
    <td>The tangent of the value as radians. <code>tan</code> only supports arguments of type double, so all other types are cast to double. Returns a double. For undefined values of <code>tan</code> (like <code>tan(pi / 2)</code>) a fixed arbitrary value may be returned.</td>
  </tr>
  <tr>
    <td><code>COT(value)</code></td>
    <td>The cotangent of the value as radians. <code>cot</code> only supports arguments of type double, so all other types are cast to double. Returns a double. For undefined values of <code>cot</code> (like <code>cot(pi)</code>) a fixed arbitrary value may be returned.</td>
  </tr>
  <tr>
    <td><code>SEC(value)</code></td>
    <td>The secant of the value as radians. <code>sec</code> only supports arguments of type double, so all other types are cast to double. Returns a double. For undefined values of <code>sec</code> (like <code>sec(pi / 2)</code>) a fixed arbitrary value may be returned.</td>
  </tr>
  <tr>
    <td><code>CSC(value)</code></td>
    <td>The cosecant of the value as radians. <code>csc</code> only supports arguments of type double, so all other types are cast to double. Returns a double. For undefined values of <code>csc</code> (like <code>csc(pi)</code>) a fixed arbitrary value may be returned.</td>
  </tr>
  <tr>
    <td><code>ASIN(value)</code></td>
    <td>The arcsine of the value, returned as radians. The returned value is in the range <code>[-pi/2, pi/2]</code> or <code>NaN</code> if the value is outside the range of <code>[-1, 1]</code>. <code>asin</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>ACOS(value)</code></td>
    <td>The arccosine of the value, returned as radians. The returned value is in the range <code>[0, pi]</code> or <code>NaN</code> if the value is outside the range of <code>[-1, 1]</code>. <code>acos</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>ATAN(value)</code></td>
    <td>The arctangent of the value, returned as radians. The returned value is in the range <code>[-pi/2, pi/2]</code>.<code>atan</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>ATAN2(y, x)</code></td>
    <td>The arctangent of <code>y/x</code>, returned as radians. <code>atan2</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>DEGREES(value)</code></td>
    <td>Converts the given value in radians to degrees. <code>degrees</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>RADIANS(value)</code></td>
    <td>Converts the given value in degrees to radians. <code>radians</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>CBRT(value)</code></td>
    <td>Calculates the cube root of the given value. <code>cbrt</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>SINH(value)</code></td>
    <td>The hyperbolic sine of the value as radians. <code>sinh</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>COSH(value)</code></td>
    <td>The hyperbolic cosine of the value as radians. <code>cosh</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>TANH(value)</code></td>
    <td>The hyperbolic tangent of the value as radians. <code>tanh</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>COTH(value)</code></td>
    <td>The hyperbolic cotangent of the value as radians. <code>coth</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>SECH(value)</code></td>
    <td>The hyperbolic secant of the value as radians. <code>sech</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>CSCH(value)</code></td>
    <td>The hyperbolic cosecant of the value as radians. <code>csch</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>ASINH(value)</code></td>
    <td>The hyperbolic arcsine of the value, returned as radians. <code>asinh</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>ACOSH(value)</code></td>
    <td>The hyperbolic arccosine of the value, returned as radians. <code>acosh</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
  <tr>
    <td><code>ATANH(value)</code></td>
    <td>The hyperbolic arctangent of the value, returned as radians. <code>atanh</code> only supports arguments of type double, so all other types are cast to double. Returns a double.</td>
  </tr>
</table>
