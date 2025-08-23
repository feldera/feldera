## Add here import statements for all files with tests

import tests.runtime_aggtest.aggtst_base as base  # noqa: F403
from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403

from table import *  # noqa: F403
from test_array_arg_max_append import *  # noqa: F403
from test_array_arg_min_append import *  # noqa: F403
from test_array_max_append import *  # noqa: F403
from test_array_min_append import *  # noqa: F403
from test_binary_arg_max_append import *  # noqa: F403
from test_binary_arg_min_append import *  # noqa: F403
from test_binary_max_append import *  # noqa: F403
from test_binary_min_append import *  # noqa: F403
from test_interval_arg_max_append import *  # noqa: F403
from test_interval_arg_min_append import *  # noqa: F403
from test_interval_max_append import *  # noqa: F403
from test_interval_min_append import *  # noqa: F403
from test_map_arg_max_append import *  # noqa: F403
from test_map_arg_min_append import *  # noqa: F403
from test_map_max_append import *  # noqa: F403
from test_map_min_append import *  # noqa: F403
from test_row_arg_max_append import *  # noqa: F403
from test_row_arg_min_append import *  # noqa: F403
from test_row_max_append import *  # noqa: F403
from test_row_min_append import *  # noqa: F403
from test_un_int_arg_max_append import *  # noqa: F403
from test_un_int_arg_min_append import *  # noqa: F403
from test_un_int_max_append import *  # noqa: F403
from test_un_int_min_append import *  # noqa: F403
from test_varbinary_arg_max_append import *  # noqa: F403
from test_varbinary_arg_min_append import *  # noqa: F403
from test_varbinary_max_append import *  # noqa: F403
from test_varbinary_min_append import *  # noqa: F403

base.APPEND_ONLY = True


def main():
    run("aggregate_tests6", "aggtst_")


if __name__ == "__main__":
    main()
