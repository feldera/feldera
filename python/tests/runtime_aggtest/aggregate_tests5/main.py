## Add here import statements for all files with tests

import tests.runtime_aggtest.aggtst_base as base  # noqa: F403
from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403

from test_table import *  # noqa: F403
from test_atbl_varcharn_append import *  # noqa: F403
from test_atbl_charn_append import *  # noqa: F403
from test_arg_max_append import *  # noqa: F403
from test_arg_min_append import *  # noqa: F403
from test_max_append import *  # noqa: F403
from test_min_append import *  # noqa: F403
from test_charn_argmax_append import *  # noqa: F403
from test_charn_argmin_append import *  # noqa: F403
from test_charn_max_append import *  # noqa: F403
from test_charn_min_append import *  # noqa: F403
from test_date_arg_max_append import *  # noqa: F403
from test_date_arg_min_append import *  # noqa: F403
from test_date_max_append import *  # noqa: F403
from test_date_min_append import *  # noqa: F403
from test_decimal_arg_max_append import *  # noqa: F403
from test_decimal_arg_min_append import *  # noqa: F403
from test_decimal_max_append import *  # noqa: F403
from test_decimal_min_append import *  # noqa: F403
from test_time_arg_max_append import *  # noqa: F403
from test_time_arg_min_append import *  # noqa: F403
from test_time_max_append import *  # noqa: F403
from test_time_min_append import *  # noqa: F403
from test_timestamp_arg_max_append import *  # noqa: F403
from test_timestamp_arg_min_append import *  # noqa: F403
from test_timestamp_max_append import *  # noqa: F403
from test_timestamp_min_append import *  # noqa: F403
from test_varchar_argmax_append import *  # noqa: F403
from test_varchar_argmin_append import *  # noqa: F403
from test_varchar_max_append import *  # noqa: F403
from test_varchar_min_append import *  # noqa: F403
from test_varcharn_argmax_append import *  # noqa: F403
from test_varcharn_argmin_append import *  # noqa: F403
from test_varcharn_max_append import *  # noqa: F403
from test_varcharn_min_append import *  # noqa: F403

base.APPEND_ONLY = True


def main():
    run("aggregate_tests5", "aggtst_")


if __name__ == "__main__":
    main()
