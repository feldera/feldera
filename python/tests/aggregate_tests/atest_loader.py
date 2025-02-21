import sys
import inspect
from types import ModuleType

from tests.aggregate_tests.aggtst_base import DEBUG, TstAccumulator


def register_tests_in_module(module, ta: TstAccumulator, class_name: str):
    """Registers all the tests in the specified module.
    Tests are classes that start with class_name(eg: 'aggtst_').
    (As a consequence, a test may be disabled by renaming it
    not to start with pre-defined class names,
    i.e.'aggtst_', 'arithtst_', 'cmpxtst_' .)
    They must all derive from TstView or TstTable"""
    for name, obj in inspect.getmembers(module):
        if name.startswith(class_name):
            if inspect.isclass(obj):
                cls = getattr(module, name)
                instance = cls()
                instance.register(ta)
                if DEBUG:
                    print(f"Registering {name}")


def run(class_name:str, dir_name):
    """Find all tests loaded by the current module and register them"""
    ta = TstAccumulator()
    loaded = []
    for key, module in sys.modules.items():
        if isinstance(module, ModuleType):
            if not module.__name__.startswith(f"tests.{dir_name}"):
                continue
            loaded.append(module)
    for module in loaded:
        register_tests_in_module(module, ta, class_name)
    ta.run_tests()
