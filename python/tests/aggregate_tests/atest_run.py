import sys
from types import ModuleType

from tests.aggregate_tests.aggtst_base import TstAccumulator
from tests.aggregate_tests.atest_register_tests import register_tests_in_module


def run(class_name: str, dir_name):
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
