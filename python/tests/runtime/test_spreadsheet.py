# Tests the spreadsheet example from the packaged demos

import unittest

import time
from feldera import PipelineBuilder
from tests import TEST_CLIENT, unique_pipeline_name

def make_value(id: int, raw: str, computed: str) -> dict:
    return {
        "id": id,
        "background": 0,
        "raw_value": raw,
        "computed_value": computed,
        "insert_delete": 1
    }

class TestUDF(unittest.TestCase):
    def test_local(self):
        dir = "../demo/packaged/sql/";
        # Read the file ../demo/packaged/sql/10-spreadsheet.sql
        with open(dir + "10-spreadsheet.sql", "r") as f:
            sql = f.read()
        with open(dir + "10-spreadsheet.udf.rs", "r") as f:
            udfs = f.read()
        with open(dir + "10-spreadsheet.udf.toml", "r") as f:
            toml = f.read()

        pipeline = PipelineBuilder(
            TEST_CLIENT, name=unique_pipeline_name("test_spreadsheet"), sql=sql, udf_rust=udfs,
            udf_toml=toml
        ).create_or_replace()

        pipeline.start_paused()
        out = pipeline.listen("spreadsheet_view")
        pipeline.resume()

        # What is the right way to wait for ingestion to complete?
        time.sleep(2)
        pipeline.stop(force=True)

        output = sorted(out.to_dict(), key=lambda x: x["id"])
        expected = [
            make_value(0, "=A39999999", "42"),
            make_value(1, "=A0", "42"),
            make_value(2, "=A0+B0", "84"),
            make_value(12, "Reference", "Reference"),
            make_value(13, "Logic", "Logic"),
            make_value(14, "Functions", "Functions"),
            make_value(15, "Datetime", "Datetime"),
            make_value(39, "=XOR(0,1)", "TRUE"),
            make_value(40, "=ABS(-1)", "1"),
            make_value(41, "2019-03-01T02:00:00.000Z", "2019-03-01T02:00:00.000Z"),
            make_value(65, "=2>=1", "TRUE"),
            make_value(66, "=AVERAGE(1,2,3,1,2,3)", "2"),
            make_value(67, "2019-08-30T02:00:00.000Z", "2019-08-30T02:00:00.000Z"),
            make_value(91, "=OR(1>1,1<>1)", "FALSE"),
            make_value(92, "={1,2,3}+{1,2,3}", "{2,4,6}"),
            make_value(93, "=DAYS(P1, P2)", "-182"),
            make_value(117, "=AND(\"test\",\"True\", 1, true)", "TRUE"),
            make_value(118, "=SUM(1,2,3)", "6"),
            make_value(119, "=P1+5", "2019-03-06 02:00:00 +00:00"),
            make_value(170, "=PRODUCT(ABS(1),2*1, 3,4*1)", "24"),
            make_value(196, "=RIGHT(\"apple\", 3)", "ple"),
            make_value(222, "=LEFT(\"apple\", 3)", "app"),
            make_value(1039999974, "42", "42")
        ]
        assert output == expected


if __name__ == "__main__":
    unittest.main()
