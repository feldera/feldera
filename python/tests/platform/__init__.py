"""
Platform tests package.

This file makes the `tests.platform` directory a proper Python package so that
relative imports like `from .helper import ...` work when running pytest with
`PYTHONPATH` pointing at the repository `python` directory.
"""
