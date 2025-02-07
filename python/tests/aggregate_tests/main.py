import sys
import inspect
from types import ModuleType

from tests.aggregate_tests.aggtst_base import DEBUG, TstAccumulator

######################
## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.test_array import *  # noqa: F403
from tests.aggregate_tests.test_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_atbl_charn import *  # noqa: F403
from tests.aggregate_tests.test_atbl_varcharn import *  # noqa: F403
from tests.aggregate_tests.test_avg import *  # noqa: F403
from tests.aggregate_tests.test_binary_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_binary_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_binary_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_binary_count_col import *  # noqa: F403
from tests.aggregate_tests.test_binary_count import *  # noqa: F403
from tests.aggregate_tests.test_binary_max import *  # noqa: F403
from tests.aggregate_tests.test_binary_min import *  # noqa: F403
from tests.aggregate_tests.test_binary_every import *  # noqa: F403
from tests.aggregate_tests.test_binary_some import *  # noqa: F403
from tests.aggregate_tests.test_binary_tbl import *  # noqa: F403
from tests.aggregate_tests.test_bit_and import *  # noqa: F403
from tests.aggregate_tests.test_bit_or import *  # noqa: F403
from tests.aggregate_tests.test_bit_xor import *  # noqa: F403
from tests.aggregate_tests.test_count import *  # noqa: F403
from tests.aggregate_tests.test_count_col import *  # noqa: F403
from tests.aggregate_tests.test_date_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_date_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_date_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_date_count_col import *  # noqa: F403
from tests.aggregate_tests.test_date_count import *  # noqa: F403
from tests.aggregate_tests.test_date_every import *  # noqa: F403
from tests.aggregate_tests.test_date_some import *  # noqa: F403
from tests.aggregate_tests.test_date_max import *  # noqa: F403
from tests.aggregate_tests.test_date_min import *  # noqa: F403
from tests.aggregate_tests.test_date_tbl import *  # noqa: F403

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
from tests.aggregate_tests.test_varchar_argmax import *  # noqa: F403
from tests.aggregate_tests.test_varchar_argmin import *  # noqa: F403
from tests.aggregate_tests.test_varchar_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_varchar_count import *  # noqa: F403
from tests.aggregate_tests.test_varchar_count_col import *  # noqa: F403
from tests.aggregate_tests.test_varchar_every import *  # noqa: F403
from tests.aggregate_tests.test_varchar_max import *  # noqa: F403
from tests.aggregate_tests.test_varchar_min import *  # noqa: F403
from tests.aggregate_tests.test_varchar_table import *  # noqa: F403
from tests.aggregate_tests.test_varchar_some import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_arragg import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_count import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_count_col import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_min import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_max import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_argmax import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_argmin import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_every import *  # noqa: F403
from tests.aggregate_tests.test_varcharn_some import *  # noqa: F403
from tests.aggregate_tests.test_charn_argmax import *  # noqa: F403
from tests.aggregate_tests.test_charn_argmin import *  # noqa: F403
from tests.aggregate_tests.test_charn_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_charn_count_col import *  # noqa: F403
from tests.aggregate_tests.test_charn_count import *  # noqa: F403
from tests.aggregate_tests.test_charn_max import *  # noqa: F403
from tests.aggregate_tests.test_charn_min import *  # noqa: F403
from tests.aggregate_tests.test_charn_every import *  # noqa: F403
from tests.aggregate_tests.test_charn_some import *  # noqa: F403
from tests.aggregate_tests.test_time_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_time_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_time_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_time_count_col import *  # noqa: F403
from tests.aggregate_tests.test_time_count import *  # noqa: F403
from tests.aggregate_tests.test_time_max import *  # noqa: F403
from tests.aggregate_tests.test_time_min import *  # noqa: F403
from tests.aggregate_tests.test_time_every import *  # noqa: F403
from tests.aggregate_tests.test_time_some import *  # noqa: F403
from tests.aggregate_tests.test_time_tbl import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_count_col import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_count import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_every import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_max import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_min import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_some import *  # noqa: F403
from tests.aggregate_tests.test_timestamp_tbl import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_count_col import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_count import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_max import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_min import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_every import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_some import *  # noqa: F403
from tests.aggregate_tests.test_varbinary_tbl import *  # noqa: F403
from tests.aggregate_tests.test_atbl_interval import *  # noqa: F403
from tests.aggregate_tests.test_interval_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_interval_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_interval_count import *  # noqa: F403
from tests.aggregate_tests.test_interval_count_col import *  # noqa: F403
from tests.aggregate_tests.test_interval_max import *  # noqa: F403
from tests.aggregate_tests.test_interval_min import *  # noqa: F403
from tests.aggregate_tests.test_interval_every import *  # noqa: F403
from tests.aggregate_tests.test_interval_some import *  # noqa: F403
from tests.aggregate_tests.test_array_tbl import *  # noqa: F403
from tests.aggregate_tests.test_array_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_array_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_array_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_array_count import *  # noqa: F403
from tests.aggregate_tests.test_array_count_col import *  # noqa: F403
from tests.aggregate_tests.test_array_every import *  # noqa: F403
from tests.aggregate_tests.test_array_some import *  # noqa: F403
from tests.aggregate_tests.test_array_max import *  # noqa: F403
from tests.aggregate_tests.test_array_min import *  # noqa: F403
from tests.aggregate_tests.test_map_tbl import *  # noqa: F403
from tests.aggregate_tests.test_map_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_map_arg_min import *  # noqa: F403
from tests.aggregate_tests.test_map_arr_agg import *  # noqa: F403
from tests.aggregate_tests.test_map_count import *  # noqa: F403
from tests.aggregate_tests.test_map_count_col import *  # noqa: F403
from tests.aggregate_tests.test_map_max import *  # noqa: F403
from tests.aggregate_tests.test_map_min import *  # noqa: F403
from tests.aggregate_tests.test_map_some import *  # noqa: F403
from tests.aggregate_tests.test_map_every import *  # noqa: F403
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
from tests.aggregate_tests.test_uuid import *  # noqa: F403


def register_tests_in_module(module, ta: TstAccumulator):
    """Registers all the tests in the specified module.
    Tests are classes that start with aggtst_.
    (As a consequence, a test may be disabled by renaming it
    not to start with 'aggtst_'.)
    They must all derive from TstView or TstTable"""
    for name, obj in inspect.getmembers(module):
        if name.startswith("aggtst_"):
            if inspect.isclass(obj):
                cls = getattr(module, name)
                instance = cls()
                instance.register(ta)
                if DEBUG:
                    print(f"Registering {name}")


def run():
    """Find all tests loaded by the current module and register them"""
    ta = TstAccumulator()
    loaded = []
    for key, module in sys.modules.items():
        if isinstance(module, ModuleType):
            if not module.__name__.startswith("tests.aggregate_tests"):
                continue
            loaded.append(module)
    for module in loaded:
        register_tests_in_module(module, ta)
    ta.run_tests()


def main():
    run()


if __name__ == "__main__":
    main()
