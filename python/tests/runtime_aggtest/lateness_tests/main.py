## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_lateness_check import *  # noqa: F403


def main():
    run("lateness_tests", "lateness_")


if __name__ == "__main__":
    main()
