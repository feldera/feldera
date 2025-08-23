## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from arr_map_type_fn import *  # noqa: F403
from numeric_type_fn import *  # noqa: F403
from str_bin_type_fn import *  # noqa: F403
from str_unicode_fn import *  # noqa: F403
from check_negative_tests import *  # noqa: F403
from illegal_tbl import *  # noqa: F403


def main():
    run("illarg_tests", "illarg_", ["arr_", "check_", "illegal_", "numeric_", "str_"])


if __name__ == "__main__":
    main()
