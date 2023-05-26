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
