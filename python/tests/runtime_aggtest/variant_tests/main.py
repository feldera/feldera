## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from dtype_and_variant import *  # noqa: F403
from cpmx_variant import *  # noqa: F403
from arr_of_cmpx_type import *  # noqa: F403
from row_of_cmpx_type import *  # noqa: F403
from udt_of_cmpx_type import *  # noqa: F403


def main():
    run("variant_tests", "varnttst_", ["arr_", "cpmx_", "dtype_", "row_", "udt_"])


if __name__ == "__main__":
    main()
