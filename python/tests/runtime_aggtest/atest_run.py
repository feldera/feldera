from tests.runtime_aggtest.atest_register_tests import discover_tests
from typing import List, Optional


def run(
    pipeline_name_prefix: str,
    class_name: str,
    prefix_matches: Optional[List[str]] = None,
):
    """Run the registered tests"""
    if prefix_matches is None:
        prefix_matches = ["test_"]
    ta = discover_tests(class_name, prefix_matches)
    ta.run_tests(pipeline_name_prefix)
