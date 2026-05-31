## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403

from test_float_tbl import *  # noqa: F403
from test_float_arg_max import *  # noqa: F403
from test_float_arg_min import *  # noqa: F403
from test_float_arr_agg import *  # noqa: F403
from test_float_avg import *  # noqa: F403
from test_float_count import *  # noqa: F403
from test_float_count_col import *  # noqa: F403
from test_float_every import *  # noqa: F403
from test_float_max import *  # noqa: F403
from test_float_min import *  # noqa: F403
from test_float_stddev import *  # noqa: F403
from test_float_some import *  # noqa: F403
from test_float_sum import *  # noqa: F403


def main():
    run("aggregate_tests7", "aggtst_")


if __name__ == "__main__":
    main()
