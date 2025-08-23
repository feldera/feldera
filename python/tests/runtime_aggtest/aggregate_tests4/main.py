## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_int_table import *  # noqa: F403
from test_varchar_table4 import *  # noqa: F403
from test_atbl_charn4 import *  # noqa: F403
from test_array import *  # noqa: F403
from test_array_tbl import *  # noqa: F403
from test_array_arg_max import *  # noqa: F403
from test_array_arg_min import *  # noqa: F403
from test_array_arr_agg import *  # noqa: F403
from test_array_count import *  # noqa: F403
from test_array_count_col import *  # noqa: F403
from test_array_every import *  # noqa: F403
from test_array_some import *  # noqa: F403
from test_array_max import *  # noqa: F403
from test_array_min import *  # noqa: F403
from test_varchar_argmax import *  # noqa: F403
from test_varchar_argmin import *  # noqa: F403
from test_varchar_arr_agg import *  # noqa: F403
from test_varchar_count import *  # noqa: F403
from test_varchar_count_col import *  # noqa: F403
from test_varchar_every import *  # noqa: F403
from test_varchar_max import *  # noqa: F403
from test_varchar_min import *  # noqa: F403
from test_varchar_some import *  # noqa: F403
from test_atbl_varcharn4 import *  # noqa: F403
from test_varcharn_arragg import *  # noqa: F403
from test_varcharn_count import *  # noqa: F403
from test_varcharn_count_col import *  # noqa: F403
from test_varcharn_min import *  # noqa: F403
from test_varcharn_max import *  # noqa: F403
from test_varcharn_argmax import *  # noqa: F403
from test_varcharn_argmin import *  # noqa: F403
from test_varcharn_every import *  # noqa: F403
from test_varcharn_some import *  # noqa: F403
from test_uuid import *  # noqa: F403
from test_map_tbl import *  # noqa: F403
from test_map_arg_max import *  # noqa: F403
from test_map_arg_min import *  # noqa: F403
from test_map_arr_agg import *  # noqa: F403
from test_map_count import *  # noqa: F403
from test_map_count_col import *  # noqa: F403
from test_map_max import *  # noqa: F403
from test_map_min import *  # noqa: F403
from test_map_some import *  # noqa: F403
from test_map_every import *  # noqa: F403


def main():
    run("aggregate_tests4", "aggtst_")


if __name__ == "__main__":
    main()
