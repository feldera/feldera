import sys
import inspect
from types import ModuleType

from tests.aggregate_tests.aggtst_base import DEBUG, TstAccumulator

######################
## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.arithmetic_tests.test_tables import *  # noqa: F403
from tests.arithmetic_tests.test_date import *  # noqa: F403
from tests.arithmetic_tests.test_interval import *  # noqa: F403
from tests.arithmetic_tests.test_time import *  # noqa: F403
from tests.arithmetic_tests.test_timestamp import *  # noqa: F403


# Column naming conventions for the views:
# ytm = YEAR to MONTH, ytm_str = YEAR to MONTH converted to string
# dth = DAY TO HOUR, dth_str = DAY TO HOUR converted to string
# dtm = DAY TO MINUTE, dtm_str = DAY TO MINUTE converted to string
# dts = DAY to SECOND, dts_str = DAY to SECOND converted to string
# htm = HOUR to MINUTE, htm_str = HOUR to MINUTE converted to string
# hts = HOUR to SECOND, hts_str = HOUR to SECOND converted to string
# mts = MINUTE to SECOND, mts_str = MINUTE to SECOND converted to string


def register_tests_in_module(module, ta: TstAccumulator):
    """Registers all the tests in the specified module.
    Tests are classes that start with arithtst_.
    (As a consequence, a test may be disabled by renaming it
    not to start with 'arithtst_'.)
    They must all derive from TstView or TstTable"""
    for name, obj in inspect.getmembers(module):
        if name.startswith("arithtst_"):
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
            if not module.__name__.startswith("tests.arithmetic_tests"):
                continue
            loaded.append(module)
    for module in loaded:
        register_tests_in_module(module, ta)
    ta.run_tests()


def main():
    run()


if __name__ == "__main__":
    main()
