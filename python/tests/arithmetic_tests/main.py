## Add here import statements for all files with tests

from tests.aggregate_tests.aggtst_base import *  # noqa: F403
from tests.aggregate_tests.atest_loader import *  # noqa: F403
from tests.arithmetic_tests.test_tables import *  # noqa: F403
from tests.arithmetic_tests.test_date import *  # noqa: F403
from tests.arithmetic_tests.test_interval import *  # noqa: F403
from tests.arithmetic_tests.test_time import *  # noqa: F403
from tests.arithmetic_tests.test_timestamp import *  # noqa: F403


# Column naming conventions for the views:
# ytm = YEAR to MONTH, ytm_str = YEAR to MONTH converted to string
# dth = DAY TO HOUR, dth_str = DAY TO HOUR converted to string
# dtm = DAY TO MINUTE, dtm_str = DAY TO MINUTE converted to string
# dts = DAY to SECOND, dts_str = DAY to SECOND converted to string
# htm = HOUR to MINUTE, htm_str = HOUR to MINUTE converted to string
# hts = HOUR to SECOND, hts_str = HOUR to SECOND converted to string
# mts = MINUTE to SECOND, mts_str = MINUTE to SECOND converted to string


def main():
    run("arithtst_", "arithmetic_tests")


if __name__ == "__main__":
    main()
