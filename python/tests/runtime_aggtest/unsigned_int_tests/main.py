## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_un_int_arith_fn import *  # noqa: F403
from test_un_int_supported_functions import *  # noqa: F403
from test_un_int_tbl import *  # noqa: F403


def main():
    run("un_int_tests", "un_int_")


if __name__ == "__main__":
    main()
