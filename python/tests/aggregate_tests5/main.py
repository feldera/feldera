## Add here import statements for all files with tests

import tests.aggregate_tests.aggtst_base as base  # noqa: F403
from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_run import run  # noqa: F403

from tests.aggregate_tests5.table import *  # noqa: F403
from tests.aggregate_tests5.test_atbl_varcharn_append import *  # noqa: F403
from tests.aggregate_tests5.test_atbl_charn_append import *  # noqa: F403
from tests.aggregate_tests5.test_arg_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_arg_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_charn_argmax_append import *  # noqa: F403
from tests.aggregate_tests5.test_charn_argmin_append import *  # noqa: F403
from tests.aggregate_tests5.test_charn_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_charn_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_date_arg_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_date_arg_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_date_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_date_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_decimal_arg_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_decimal_arg_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_decimal_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_decimal_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_time_arg_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_time_arg_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_time_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_time_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_timestamp_arg_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_timestamp_arg_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_timestamp_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_timestamp_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_varchar_argmax_append import *  # noqa: F403
from tests.aggregate_tests5.test_varchar_argmin_append import *  # noqa: F403
from tests.aggregate_tests5.test_varchar_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_varchar_min_append import *  # noqa: F403
from tests.aggregate_tests5.test_varcharn_argmax_append import *  # noqa: F403
from tests.aggregate_tests5.test_varcharn_argmin_append import *  # noqa: F403
from tests.aggregate_tests5.test_varcharn_max_append import *  # noqa: F403
from tests.aggregate_tests5.test_varcharn_min_append import *  # noqa: F403

base.APPEND_ONLY = True


def main():
    run("aggtst_", "aggregate_tests5")


if __name__ == "__main__":
    main()
