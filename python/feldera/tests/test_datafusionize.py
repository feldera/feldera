import unittest

from feldera.testutils import datafusionize


class TestDatafusionize(unittest.TestCase):
    def test_datafusionize(self):
        # Test SORT_ARRAY replacement
        query = "SELECT SORT_ARRAY(col1) FROM table1"
        result = datafusionize(query)
        assert "array_sort(col1)" in result

        # Test TRUNCATE replacement
        query = "SELECT TRUNCATE(value, 2) FROM table2"
        result = datafusionize(query)
        assert "trunc(value, 2)" in result

        # Test TIMESTAMP_TRUNC replacement
        query = "SELECT TIMESTAMP_TRUNC(MAKE_TIMESTAMP(2023, 1, 15, 10, 30, 0), DAY) FROM table3"
        result = datafusionize(query)
        assert "DATE_TRUNC('DAY', TO_TIMESTAMP(2023, 1, 15, 10, 30, 0))" in result

        query = "TIMESTAMP_TRUNC(MAKE_TIMESTAMP(order_group_last_activity_time), hour) AS window_start_time,"
        result = datafusionize(query)
        assert (
            "DATE_TRUNC('hour', TO_TIMESTAMP(order_group_last_activity_time)) AS window_start_time,"
            in result
        )

        # Test case insensitive matching
        query = "SELECT sort_array(col) FROM table WHERE truncate(val) > 0"
        result = datafusionize(query)
        assert "array_sort(col)" in result
        assert "trunc(val)" in result


if __name__ == "__main__":
    unittest.main()
