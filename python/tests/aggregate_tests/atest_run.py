from tests.aggregate_tests.atest_register_tests import discover_tests


def run(class_name: str, dir_name):
    """Run the registered tests"""
    ta = discover_tests(class_name, dir_name)
    ta.run_tests()
