## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_arr_map_type_fn import *  # noqa: F403
from test_date_time_fn import *  # noqa: F403
from test_numeric_type_fn import *  # noqa: F403
from test_str_bin_type_fn import *  # noqa: F403
from test_str_unicode_fn import *  # noqa: F403
from test_check_negative_tests import *  # noqa: F403
from test_illegal_tbl import *  # noqa: F403


def main():
    run("illarg_tests", "illarg_")


if __name__ == "__main__":
    main()
