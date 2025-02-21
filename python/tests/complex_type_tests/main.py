## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_loader import *  # noqa: F403
from tests.complex_type_tests.test_arr import *  # noqa: F403
from tests.complex_type_tests.test_map import *  # noqa: F403
from tests.complex_type_tests.test_row import *  # noqa: F403
from tests.complex_type_tests.test_user_defined import *  # noqa: F403


def main():
    run("cmpxtst_", "complex_type_tests")


if __name__ == "__main__":
    main()
