from tests.aggregate_tests.aggtst_base import DEBUG, TstAccumulator
import inspect


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
