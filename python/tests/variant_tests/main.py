## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_loader import run  # noqa: F403
from tests.variant_tests.dtype_and_variant import *  # noqa: F403
from tests.variant_tests.cpmx_variant import *  # noqa: F403


def main():
    run("varnttst_", "variant_tests")


if __name__ == "__main__":
    main()
