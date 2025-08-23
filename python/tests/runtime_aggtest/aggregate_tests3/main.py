## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_unsigned_int_tbl import *  # noqa: F403
from test_un_int_array_agg import *  # noqa: F403
from test_un_int_count import *  # noqa: F403
from test_un_int_count_col import *  # noqa: F403
from test_un_int_countif import *  # noqa: F403
from test_un_int_arg_max import *  # noqa: F403
from test_un_int_arg_min import *  # noqa: F403
from test_un_int_avg import *  # noqa: F403
from test_un_int_max import *  # noqa: F403
from test_un_int_min import *  # noqa: F403
from test_un_int_sum import *  # noqa: F403
from test_un_int_every import *  # noqa: F403
from test_un_int_some import *  # noqa: F403
from test_un_int_stddev import *  # noqa: F403
from test_un_int_stddev_pop import *  # noqa: F403
from test_varbinary_arg_min import *  # noqa: F403
from test_varbinary_arg_max import *  # noqa: F403
from test_varbinary_arr_agg import *  # noqa: F403
from test_varbinary_count_col import *  # noqa: F403
from test_varbinary_count import *  # noqa: F403
from test_varbinary_max import *  # noqa: F403
from test_varbinary_min import *  # noqa: F403
from test_varbinary_every import *  # noqa: F403
from test_varbinary_some import *  # noqa: F403
from test_varbinary_tbl import *  # noqa: F403
from test_binary_arg_max import *  # noqa: F403
from test_binary_arg_min import *  # noqa: F403
from test_binary_arr_agg import *  # noqa: F403
from test_binary_count_col import *  # noqa: F403
from test_binary_count import *  # noqa: F403
from test_binary_max import *  # noqa: F403
from test_binary_min import *  # noqa: F403
from test_binary_every import *  # noqa: F403
from test_binary_some import *  # noqa: F403
from test_binary_tbl import *  # noqa: F403


def main():
    run("aggregate_tests3", "aggtst_")


if __name__ == "__main__":
    main()
