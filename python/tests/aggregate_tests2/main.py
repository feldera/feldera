## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_loader import run  # noqa: F403
from tests.aggregate_tests2.test_varchar_argmax import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_argmin import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_arr_agg import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_count import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_count_col import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_every import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_max import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_min import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_table import *  # noqa: F403
from tests.aggregate_tests2.test_varchar_some import *  # noqa: F403
from tests.aggregate_tests2.test_atbl_varcharn import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_arragg import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_count import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_count_col import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_min import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_max import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_argmax import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_argmin import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_every import *  # noqa: F403
from tests.aggregate_tests2.test_varcharn_some import *  # noqa: F403
from tests.aggregate_tests2.test_atbl_charn import *  # noqa: F403
from tests.aggregate_tests2.test_charn_argmax import *  # noqa: F403
from tests.aggregate_tests2.test_charn_argmin import *  # noqa: F403
from tests.aggregate_tests2.test_charn_arr_agg import *  # noqa: F403
from tests.aggregate_tests2.test_charn_count_col import *  # noqa: F403
from tests.aggregate_tests2.test_charn_count import *  # noqa: F403
from tests.aggregate_tests2.test_charn_max import *  # noqa: F403
from tests.aggregate_tests2.test_charn_min import *  # noqa: F403
from tests.aggregate_tests2.test_charn_every import *  # noqa: F403
from tests.aggregate_tests2.test_charn_some import *  # noqa: F403
from tests.aggregate_tests2.test_atbl_interval import *  # noqa: F403
from tests.aggregate_tests2.test_interval_arg_max import *  # noqa: F403
from tests.aggregate_tests2.test_interval_arg_min import *  # noqa: F403
from tests.aggregate_tests2.test_interval_count import *  # noqa: F403
from tests.aggregate_tests2.test_interval_count_col import *  # noqa: F403
from tests.aggregate_tests2.test_interval_max import *  # noqa: F403
from tests.aggregate_tests2.test_interval_min import *  # noqa: F403
from tests.aggregate_tests2.test_interval_every import *  # noqa: F403
from tests.aggregate_tests2.test_interval_some import *  # noqa: F403
from tests.aggregate_tests2.test_time_arg_max import *  # noqa: F403
from tests.aggregate_tests2.test_time_arg_min import *  # noqa: F403
from tests.aggregate_tests2.test_time_arr_agg import *  # noqa: F403
from tests.aggregate_tests2.test_time_count_col import *  # noqa: F403
from tests.aggregate_tests2.test_time_count import *  # noqa: F403
from tests.aggregate_tests2.test_time_max import *  # noqa: F403
from tests.aggregate_tests2.test_time_min import *  # noqa: F403
from tests.aggregate_tests2.test_time_every import *  # noqa: F403
from tests.aggregate_tests2.test_time_some import *  # noqa: F403
from tests.aggregate_tests2.test_time_tbl import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_arg_max import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_arg_min import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_arr_agg import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_count_col import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_count import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_every import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_max import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_min import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_some import *  # noqa: F403
from tests.aggregate_tests2.test_timestamp_tbl import *  # noqa: F403
from tests.aggregate_tests2.test_date_arg_max import *  # noqa: F403
from tests.aggregate_tests2.test_date_arg_min import *  # noqa: F403
from tests.aggregate_tests2.test_date_arr_agg import *  # noqa: F403
from tests.aggregate_tests2.test_date_count_col import *  # noqa: F403
from tests.aggregate_tests2.test_date_count import *  # noqa: F403
from tests.aggregate_tests2.test_date_every import *  # noqa: F403
from tests.aggregate_tests2.test_date_some import *  # noqa: F403
from tests.aggregate_tests2.test_date_max import *  # noqa: F403
from tests.aggregate_tests2.test_date_min import *  # noqa: F403
from tests.aggregate_tests2.test_date_tbl import *  # noqa: F403
from tests.aggregate_tests2.test_uuid import *  # noqa: F403


def main():
    run("aggtst_", "aggregate_tests2")


if __name__ == "__main__":
    main()
