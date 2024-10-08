import sys
import inspect
from types import ModuleType

from tests.aggregate_tests.aggtst_base import DEBUG, TstAccumulator

######################
## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.test_array import *  # noqa: F403
from tests.aggregate_tests.test_arg_max import *  # noqa: F403
from tests.aggregate_tests.test_avg import *  # noqa: F403
from tests.aggregate_tests.test_bit_and import *  # noqa: F403
from tests.aggregate_tests.test_bit_or import *  # noqa: F403
from tests.aggregate_tests.test_bit_xor import *  # noqa: F403
from tests.aggregate_tests.test_count import *  # noqa: F403
from tests.aggregate_tests.test_count_col import *  # noqa: F403

# from tests.aggregate_tests.test_decimal_avg import *  # noqa: F403
# from tests.aggregate_tests.test_decimal_sum import *  # noqa: F403
# from tests.aggregate_tests.test_decimal_table import *  # noqa: F403
from tests.aggregate_tests.test_every import *  # noqa: F403
from tests.aggregate_tests.test_int_table import *  # noqa: F403
from tests.aggregate_tests.test_max import *  # noqa: F403
from tests.aggregate_tests.test_min import *  # noqa: F403
from tests.aggregate_tests.test_some import *  # noqa: F403
from tests.aggregate_tests.test_stddev_pop import *  # noqa: F403
from tests.aggregate_tests.test_stddev_samp import *  # noqa: F403
from tests.aggregate_tests.test_sum import *  # noqa: F403


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
