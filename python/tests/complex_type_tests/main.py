## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_loader import run  # noqa: F403
from tests.complex_type_tests.test_arr import *  # noqa: F403
from tests.complex_type_tests.test_map import *  # noqa: F403
from tests.complex_type_tests.test_row import *  # noqa: F403
from tests.complex_type_tests.test_user_defined import *  # noqa: F403
from tests.complex_type_tests.test_arr_of_arr import *  # noqa: F403
from tests.complex_type_tests.test_arr_of_map import *  # noqa: F403
from tests.complex_type_tests.test_arr_of_row import *  # noqa: F403
from tests.complex_type_tests.test_arr_of_udt import *  # noqa: F403
from tests.complex_type_tests.test_row_of_arr import *  # noqa: F403
from tests.complex_type_tests.test_row_of_map import *  # noqa: F403
from tests.complex_type_tests.test_row_of_row import *  # noqa: F403
from tests.complex_type_tests.test_row_of_udt import *  # noqa: F403
from tests.complex_type_tests.test_udt_of_arr import *  # noqa: F403
from tests.complex_type_tests.test_udt_of_udt import *  # noqa: F403
from tests.complex_type_tests.test_udt_of_row import *  # noqa: F403
from tests.complex_type_tests.test_udt_of_udt import *  # noqa: F403


def main():
    run("cmpxtst_", "complex_type_tests")


if __name__ == "__main__":
    main()
