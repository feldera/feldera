## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_date_time_fn import *  # noqa: F403
from test_numeric_type_fn import *  # noqa: F403
from test_window_agg import *  # noqa: F403
from tests.runtime_aggtest.illarg_tests.test_illegal_tbl import *  # noqa: F403


def main():
    run(
        "illarg_tests2",
        "illarg_",
        prefix_matches=[
            "test_",
            "tests.runtime_aggtest.illarg_tests.test_",
            "tests.runtime_aggtest.illarg_tests2.test_",
        ],
    )


if __name__ == "__main__":
    main()
