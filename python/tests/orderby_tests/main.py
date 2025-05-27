## Add here import statements for all files with tests


from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.orderby_tests.sqlite_runner import discover_and_run_sqlite_tests  # noqa: F403
from tests.orderby_tests.test_check import *  # noqa: F403


def main():
    """Run SQLite tests to populate expected results, then run DBSP tests with updated data"""
    print("\nRunning SQLite tests")
    ta = discover_and_run_sqlite_tests(
        "orderby_", "orderby_tests"
    )  # runs SQLite and updates .data

    print("\nRunning DBSP tests")
    ta.run_tests()  # run DBSP tests using SAME accumulator and objects with updated .data


if __name__ == "__main__":
    main()
