## Add here import statements for all files with tests


from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.orderby_tests.sqlite_runner import discover_sqlite_tests  # noqa: F403
from tests.orderby_tests.test_check import *  # noqa: F403
from tests.orderby_tests.orderby import * # noqa: F403


def main():
    """Run SQLite tests to populate expected results, then run Feldera tests with updated data"""

    print("\nRunning SQLite tests")
    ta = discover_sqlite_tests(
        "orderby_", "orderby_tests"
    )  # runs SQLite and updates .data

    print("\nRunning Feldera tests")
    ta.run_tests()  # run Feldera tests using SAME accumulator and objects with updated .data


if __name__ == "__main__":
    main()
