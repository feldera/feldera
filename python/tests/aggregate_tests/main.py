## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_run import run  # noqa: F403
from tests.aggregate_tests.test_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_avg import *  # noqa: F403
from tests.aggregate_tests.test_bit_and import *  # noqa: F403
from tests.aggregate_tests.test_bit_or import *  # noqa: F403
from tests.aggregate_tests.test_bit_xor import *  # noqa: F403
from tests.aggregate_tests.test_count import *  # noqa: F403
from tests.aggregate_tests.test_count_col import *  # noqa: F403
from tests.aggregate_tests.test_decimal_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_decimal_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_decimal_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_decimal_avg import *  # noqa: F403
from tests.aggregate_tests.test_decimal_count import *  # noqa: F403
from tests.aggregate_tests.test_decimal_count_col import *  # noqa: F403
from tests.aggregate_tests.test_decimal_max import *  # noqa: F403
from tests.aggregate_tests.test_decimal_min import *  # noqa: F403
from tests.aggregate_tests.test_decimal_every import *  # noqa: F403
from tests.aggregate_tests.test_decimal_some import *  # noqa: F403
from tests.aggregate_tests.test_decimal_sum import *  # noqa: F403
from tests.aggregate_tests.test_decimal_stddev_samp import *  # noqa: F403
from tests.aggregate_tests.test_decimal_stddev_pop import *  # noqa: F403
from tests.aggregate_tests.test_decimal_table import *  # noqa: F403
from tests.aggregate_tests.test_every import *  # noqa: F403
from tests.aggregate_tests.test_int_table import *  # noqa: F403
from tests.aggregate_tests.test_max import *  # noqa: F403
from tests.aggregate_tests.test_min import *  # noqa: F403
from tests.aggregate_tests.test_some import *  # noqa: F403
from tests.aggregate_tests.test_stddev_pop import *  # noqa: F403
from tests.aggregate_tests.test_stddev_samp import *  # noqa: F403
from tests.aggregate_tests.test_row_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_row_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_row_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_row_count_col import *  # noqa: F403
from tests.aggregate_tests.test_row_max import *  # noqa: F403
from tests.aggregate_tests.test_row_min import *  # noqa: F403
from tests.aggregate_tests.test_row_some import *  # noqa: F403
from tests.aggregate_tests.test_row_every import *  # noqa: F403
from tests.aggregate_tests.test_row_tbl import *  # noqa: F403
from tests.aggregate_tests.test_empty_set import *  # noqa: F403
from tests.aggregate_tests.test_float_max import *  # noqa: F403
from tests.aggregate_tests.test_float_min import *  # noqa: F403
from tests.aggregate_tests.test_float_tbl import *  # noqa: F403


def main():
    run("aggtst_", "aggregate_tests")


if __name__ == "__main__":
    main()
