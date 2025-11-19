## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_arr import *  # noqa: F403
from test_map import *  # noqa: F403
from test_row import *  # noqa: F403
from test_user_defined import *  # noqa: F403
from test_arr_of_arr import *  # noqa: F403
from test_arr_of_map import *  # noqa: F403
from test_arr_of_row import *  # noqa: F403
from test_arr_of_udt import *  # noqa: F403
from test_row_of_arr import *  # noqa: F403
from test_row_of_map import *  # noqa: F403
from test_row_of_row import *  # noqa: F403
from test_row_of_udt import *  # noqa: F403
from test_udt_of_arr import *  # noqa: F403
from test_udt_of_udt import *  # noqa: F403
from test_udt_of_row import *  # noqa: F403
from test_arr_arr_unnest import *  # noqa: F403
from test_arr_map_unnest import *  # noqa: F403
from test_arr_row_unnest import *  # noqa: F403
from test_arr_udt_unnest import *  # noqa: F403
from test_arr_unnest import *  # noqa: F403
from test_row_arr_unnest import *  # noqa: F403
from test_udt_arr_unnest import *  # noqa: F403


def main():
    run("complex_type_tests", "cmpxtst_")


if __name__ == "__main__":
    main()
