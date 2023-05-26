Boolean Operations
==================

There are two Boolean literals: ``TRUE`` and ``FALSE``.

We support the following Boolean operations:

OR
--

The following truth table defines the ``OR`` operator:

.. list-table:: OR semantics

  * -
    - TRUE
    - FALSE
    - NULL
  * - TRUE
    - TRUE
    - TRUE
    - TRUE
  * - FALSE
    - TRUE
    - FALSE
    - NULL
  * - NULL
    - TRUE
    - NULL
    - NULL

AND
---

The following truth table defines the ``AND`` operator:

.. list-table:: AND semantics

  * -
    - TRUE
    - FALSE
    - NULL
  * - TRUE
    - TRUE
    - FALSE
    - NULL
  * - FALSE
    - FALSE
    - FALSE
    - FALSE
  * - NULL
    - NULL
    - FALSE
    - NULL

NOT
---

The following table defines the ``NOT`` operator:

.. list-table:: NOT semantics

  * - TRUE
    - FALSE
  * - FALSE
    - TRUE
  * - NULL
    - NULL

IS FALSE
--------

The following table defines the ``IS FALSE`` operator:

.. list-table:: IS FALSE semantics

  * - TRUE
    - FALSE
  * - FALSE
    - TRUE
  * - NULL
    - FALSE

IS NOT FALSE
------------

The following table defines the ``IS NOT FALSE`` operator:

.. list-table:: IS NOT FALSE semantics

  * - TRUE
    - FALSE
  * - FALSE
    - TRUE
  * - NULL
    - TRUE

IS TRUE
-------

The following table defines the ``IS TRUE`` operator:

.. list-table:: IS TRUE semantics

  * - TRUE
    - TRUE
  * - FALSE
    - FALSE
  * - NULL
    - FALSE

IS NOT TRUE
-----------

The following table defines the ``IS NOT TRUE`` operator:

.. list-table:: IS NOT TRUE semantics

  * - TRUE
    - FALSE
  * - FALSE
    - TRUE
  * - NULL
    - TRUE

Notice that not all Boolean operations produce ``NULL`` results when
an operand is ``NULL``.
