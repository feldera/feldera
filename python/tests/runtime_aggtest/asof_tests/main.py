## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_asof_tbl import *  # noqa: F403
from test_asof import *  # noqa: F403
from test_asof_multijoins import *  # noqa: F403


def main():
    run("asof_tests", "asof_")


if __name__ == "__main__":
    main()
