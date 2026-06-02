"""PySpark builders for Delta table test fixtures.

Each module here writes a Delta table that exercises a feature only Delta
Spark can produce (deletion vectors, column-mapping schema evolution). They
run as subprocesses via ``tests.utils.ensure_delta_spark_fixture`` and keep
their PySpark imports function-local, so importing this package never pulls in
the JVM/Spark stack.
"""
