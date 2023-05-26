Supported SQL Constructs
========================

SQL as a language has been standardized for a long time.
Unfortunately, the standard leaves underspecified many important
behaviors.  Thus each SQL implementation is slightly different.

The SQL to DBSP compiler is implemented on top of the `Apache Calcite
<ohttps://calcite.apache.org/>`_ infrastructure.  While Calcite is a
very flexible and customizable platform, it makes several choices
regarding the SQL language semantics.  Our implementation mostly
follows these choices.  This document describes specifics of our
implementation.

.. toctree::
   :maxdepth: 2

   sql/structure.rst
   sql/types.rst
   sql/boolean.rst
   sql/comparisons.rst
   sql/integer.rst
   sql/float.rst
   sql/decimal.rst
   sql/fp.rst
   sql/string.rst
   sql/datetime.rst
   sql/array.rst
