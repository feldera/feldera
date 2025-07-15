## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_run import run  # noqa: F403
from tests.aggregate_tests3.test_unsigned_int_tbl import *  # noqa: F403
from tests.aggregate_tests3.test_un_int_avg import *  # noqa: F403
from tests.aggregate_tests3.test_un_int_sum import *  # noqa: F403


def main():
    run("aggtst_", "aggregate_tests3")


if __name__ == "__main__":
    main()
