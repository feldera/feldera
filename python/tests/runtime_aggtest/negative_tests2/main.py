## Add here import statements for all files with tests

from tests.runtime_aggtest.aggtst_base import *  # noqa: F403
from tests.runtime_aggtest.atest_run import run  # noqa: F403
from test_agg_arithmetic import *  # noqa: F403
from test_div_by_zero import *  # noqa: F403
from tests.runtime_aggtest.negative_tests.test_neg_table import *  # noqa: F403


def main():
    run(
        "negative_tests2",
        "neg_",
        prefix_matches=[
            "test_",
            "tests.runtime_aggtest.negative_tests.test_",
            "tests.runtime_aggtest.negative_tests2.test_",
        ],
    )


if __name__ == "__main__":
    main()
