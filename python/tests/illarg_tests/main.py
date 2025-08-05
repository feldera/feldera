## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_run import run  # noqa: F403
from tests.illarg_tests.arr_map_type_fn import *  # noqa: F403
from tests.illarg_tests.numeric_type_fn import *  # noqa: F403
from tests.illarg_tests.str_type_fn import *  # noqa: F403
from tests.illarg_tests.check_negative_tests import *  # noqa: F403
from tests.illarg_tests.illegal_tbl import *  # noqa: F403


def main():
    run("illarg_", "illarg_tests")


if __name__ == "__main__":
    main()
