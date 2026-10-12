import threading
import unittest
from types import SimpleNamespace

import pandas as pd

from feldera.output_handler import OutputHandler
from feldera.rest.sql_view import SQLView


class FakeClient:
    """Answers the schema lookup that `OutputHandler` does at construction."""

    def get_pipeline(self, pipeline_name, field_selector):
        view = SQLView(
            "v",
            [{"name": "x", "case_sensitive": False, "columntype": {"type": "INTEGER"}}],
        )
        return SimpleNamespace(tables=[], views=[view])


class TestOutputHandler(unittest.TestCase):
    def test_reading_while_changes_arrive_loses_nothing(self):
        handler = OutputHandler(FakeClient(), "p", "v")
        # The function that the listener thread calls for each received chunk.
        deliver = handler.handler.callback
        num_chunks = 20_000

        def receive_chunks():
            for i in range(num_chunks):
                deliver(pd.DataFrame({"x": [i], "insert_delete": [1]}), i)

        receiver = threading.Thread(target=receive_chunks)
        receiver.start()
        num_read = 0
        while receiver.is_alive():
            num_read += len(handler.to_dict())
        receiver.join()
        num_read += len(handler.to_dict())

        assert num_read == num_chunks


if __name__ == "__main__":
    unittest.main()
