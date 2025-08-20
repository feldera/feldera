## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_run import run  # noqa: F403
from tests.negative_tests.neg_arithmetic import *  # noqa: F403
from tests.negative_tests.neg_table import *  # noqa: F403


def main():
    run("neg_", "negative_tests")


if __name__ == "__main__":
    main()
