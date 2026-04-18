"""pyfeldera — Self-contained Feldera Python package.

Bundles the pipeline-manager binary, SQL compiler JAR, and Rust runtime
crate sources so that ``pip install pyfeldera`` is all you need to run
a full Feldera instance (given Java + Rust on the host).

Quick start::

    from pyfeldera import FelderaServer

    server = FelderaServer(bind_address="127.0.0.1", port=8080)
    server.start()
    server.wait_for_healthy()
    # ... interact via the feldera SDK or dbt-feldera ...
    server.stop()
"""

from pyfeldera.server import FelderaServer

__all__ = ["FelderaServer"]
__version__ = "0.1.0"
