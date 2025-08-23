## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from neg_arithmetic import *  # noqa: F403
from neg_table import *  # noqa: F403


def main():
    run("negative_tests", "neg_")


if __name__ == "__main__":
    main()
