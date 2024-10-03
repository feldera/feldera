from feldera import PipelineBuilder, Pipeline

######################
## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *
from tests.aggregate_tests.test_array import *
from tests.aggregate_tests.test_avg import *
from tests.aggregate_tests.test_bit_and import *
from tests.aggregate_tests.test_bit_or import *
from tests.aggregate_tests.test_bit_table import *
from tests.aggregate_tests.test_bit_xor import *
from tests.aggregate_tests.test_count import *
from tests.aggregate_tests.test_count_col import *
from tests.aggregate_tests.test_decimal_avg import *
from tests.aggregate_tests.test_decimal_sum import *
from tests.aggregate_tests.test_decimal_table import *
from tests.aggregate_tests.test_every import *
from tests.aggregate_tests.test_int_table import *
from tests.aggregate_tests.test_max import *
from tests.aggregate_tests.test_min import *
from tests.aggregate_tests.test_some import *
from tests.aggregate_tests.test_stddev_pop import *
from tests.aggregate_tests.test_stddev_samp import *
from tests.aggregate_tests.test_sum import *

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

def main():
    """Find all tests loaded by the current module and register them"""
    ta = TstAccumulator()
    current_module = sys.modules[__name__]
    loaded = []
    for key in current_module.__dict__.keys():
        module = current_module.__dict__[key]
        if isinstance(module, ModuleType):
            if not module.__name__.startswith("tests.aggregate_tests"):
                continue
            loaded.append(module);
    for module in loaded:
        register_tests_in_module(module, ta)
    ta.run_tests()

if __name__ == '__main__':
    main()
