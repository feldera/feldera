Decimal data type
=================

A synonym for the ``decimal`` type is ``numeric``.

A decimal number is characterized by two magnitudes: the *precision*,
which his the total number of decimal digits represented, and the
*scale*, which is the count of digits in the fractional part, to the
right of the decimal point.  For example, the number 3.1415 has a
precision of 5 and a scale of 4.

The type ``NUMERIC(precision, scale)`` specifies both precision and
scale, both of which must be constants.

The type ``NUMERIC(precision)`` is the same as ``NUMERIC(precision, 0)``.

The type ``NUMERIC`` specifies no limits on either precision or scale,
and thus will use the maximum supported values for both.

The maximum precision supported is 128 binary digits (38 decimal
digits).  The maximum scale supported is 10 decimal digits.


Operations available for the ``decimal`` type
---------------------------------------------

The legal operations are ``+`` (plus, unary and binary), ``-`` (minus,
unary and binary), ``*`` (multiplication), ``/`` (division), ``%``
(modulus).

Division or modulus by zero return ``NULL``.

Casting a string to a decimal value will produce the value ``0`` when
parsing fails.

Predefined functions on Decimal Values
--------------------------------------

.. list-table:: Predefined functions on decimal values
  :header-rows: 1

  * - ``ROUND(value)``
    - same as ``ROUND(value, 0)``
  * - ``ROUND(value, digits)``
    - where ``digits`` is an integer value.  Round the value to the
      specified number of *decimal* digits after the decimal point.
  * - ``ABS(value)``
    - absolute value
  * - ``CEIL(value)``
    - Ceiling function: nearest integer value greater than or equal to
      argument (result is a decimal value)
  * - ``FLOOR(value)``
    - Floor function: nearest integer value less than or equal to
      argument (result is a decimal value)
  * - ``SIGN(value)``
    - The "sign" function: -1, 0, or 1 showing if value is <0, =0, or >0.
  * - ``POWER(base, exponent)``
    - The power function, raising base to the power exponent.
  * - ``SQRT(value``
    - Square root of value.  Produces a runtime error for negative values.
  * - ``LN(value)``
    - The natural logarithm of value.  Produces a runtime error for
      values less than or equal to zero.
  * - ``LOG10(value)``
    - The logarithm base 10 of value.  Produces a runtime error for
      values less than or equal to zero.
      
