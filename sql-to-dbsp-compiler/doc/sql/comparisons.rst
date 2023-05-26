Comparison Operations
=====================

The following operations can take operands with multiple data types
but always return a Boolean value (sometimes nullable):

.. list-table:: comparisons
   :header-rows: 1

  * - Operation
    - Definition
    - Observation
  * - ``=``
    - equality test
    -
  * - ``<>``
    - inequality test
    -
  * -  ``>``
    - greater than
    -
  * - ``<``
    - less than
    -
  * - ``>=``
    - greater or equal
    -
  * -  ``<=``
    - less or equal
    -
  * - ``IS NULL``
    - true if operand is ``NULL``
    -
  * -  ``IS NOT NULL``
    - true if operand is not ``NULL``
    -
  * -  ``<=>``
    - equality check that treats ``NULL`` values as equal
    - result is not nullable
  * -  ``IS DISTINCT FROM``
    - check if two values are not equal, treating ``NULL`` as equal
    - result is not nullable;
  * - ``IS NOT DISTINCT FROM``
    - check if two values are the same, treating ``NULL`` values as
      equal
    - result is not nullable;
  * - ``BETWEEN ... AND ...``
    - ``x BETWEEN a AND b`` is the same as ``a <= x AND x <= b``
    - inclusive at both endpoints
  * -  ``NOT BETWEEN ... AND ...``
    - The ``NOT`` of the previous operator
    - not inclusive at either endpoint
  * - ``... IN ...``
    - checks whether value appears in a list or set
    -
  * -  ``<OP> ANY SET``
    - check if any of the values in a set compares properly
    - Example: 10 <= ANY (VALUES 10, 20, 30)
  * -  ``<OP> ALL SET``
    - check if all the values in a set compare properly
    - Example: 10 <= ALL (VALUES 10, 20, 30)
  * -  ``EXISTS query``
    - check whether query results has at least one row
    -
  * -  ``UNIQUE query``
    - check whether the result of a query contains no duplicates
    - ignores ``NULL`` values
