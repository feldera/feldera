## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_run import run  # noqa: F403
from tests.aggregate_tests6.table import *  # noqa: F403
from tests.aggregate_tests6.test_array_arg_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_array_arg_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_array_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_array_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_binary_arg_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_binary_arg_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_binary_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_binary_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_interval_arg_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_interval_arg_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_interval_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_interval_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_map_arg_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_map_arg_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_map_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_map_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_row_arg_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_row_arg_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_row_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_row_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_un_int_arg_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_un_int_arg_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_un_int_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_un_int_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_varbinary_arg_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_varbinary_arg_min_append import *  # noqa: F403
from tests.aggregate_tests6.test_varbinary_max_append import *  # noqa: F403
from tests.aggregate_tests6.test_varbinary_min_append import *  # noqa: F403


def main():
    run("aggtst_", "aggregate_tests6")


if __name__ == "__main__":
    main()
