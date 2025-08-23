## Add here import statements for all files with tests


from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from sqlite_runner import discover_sqlite_tests  # noqa: F403
from orderby_tbl_sqlite import *  # noqa: F403
from orderby_tbl_manual import *  # noqa: F403
from orderby_int import *  # noqa: F403
from orderby_varchar import *  # noqa: F403
from orderby_binary_ts import *  # noqa: F403
from orderby_arr_time import *  # noqa: F403


def main():
    """Run SQLite tests to populate expected results, then run Feldera tests with updated data"""

    print("\nRunning SQLite tests")
    ta = discover_sqlite_tests(
        "orderby_", ["orderby_", "test_"], True
    )  # runs SQLite and updates .data

    print("\nRunning Feldera tests")
    ta.run_tests(
        "orderby_tests"
    )  # run Feldera tests using SAME accumulator and objects with updated .data


if __name__ == "__main__":
    main()
