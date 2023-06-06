Floating point types
====================

We support standard IEEE 754 floating point types.

``double`` is a 64-bit standard FP value.

``float`` is a 32-bit standard FP value.

Floating point values include special values, such as ``NaN`` (not a
number), ``-Infinity``, and ``-Infinity``.  An alternative spelling
for ``-Infinity`` is ``-inf`, and an alternative spelling for
``Infinity`` is ``inf``.  When written as SQL literals, these values
have to be surrounded by simple quotes: ``'inf'``.

Infinity plus any finite value equals Infinity, as does Infinity plus
Infinity.  Infinity minus ``Infinity`` yields ``NaN``.

``NaN`` (not a number) value is used to represent undefined results.
An operation with a ``NaN`` input yields ``NaN``.  The only exception
is when the operation's output does not depend on the ``NaN`` value:
an example is ``NaN`` raised to the zero power yields one.

In sorting order ``NaN`` is considered greater than all other values.

The legal operations are ``+`` (plus, unary and binary), ``-`` (minus,
unary and binary), ``*`` (multiplication), ``/`` (division).
(modulus).

Division or modulus by zero return ``NaN``.

Casting a string to a floating-point value will produce the value
``0`` when parsing fails.


Predefined functions on Floating-point Values
---------------------------------------------

.. list-table:: Predefined functions on decimal values
  :header-rows: 1

  * - ``ABS(value)``
    - absolute value
  * - ``CEIL(value)``
    - Ceiling function: nearest integer value greater than or equal to
      argument (result is a floating point value)
  * - ``FLOOR(value)``
    - Floor function: nearest integer value less than or equal to
      argument (result is a floating point value)
  * - ``POWER(BASE, EXPONENT)``
    - The power function, raising ``BASE`` to the power ``EXPONENT``
  * - ``SQRT(value``
    - Square root of value.  Produces a runtime error for negative values.
  * - ``LN(value)``
    - The natural logarithm of value.  Produces a runtime error for
      values less than or equal to zero.
  * - ``LOG10(value)``
    - The logarithm base 10 of value.  Produces a runtime error for
      values less than or equal to zero.

