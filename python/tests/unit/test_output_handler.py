import threading
import unittest
from types import SimpleNamespace

import pandas as pd

from feldera.output_handler import OutputHandler
from feldera.rest.errors import FelderaTimeoutError
from feldera.rest.sql_view import SQLView


class FakeClient:
    """Supplies the view schema that `OutputHandler.__init__` fetches with
    `get_pipeline`, and reports every token complete at a fixed step."""

    def __init__(self, step: int = 5):
        self.step = step

    def get_pipeline(self, pipeline_name, field_selector):
        view = SQLView(
            "v",
            [{"name": "x", "case_sensitive": False, "columntype": {"type": "INTEGER"}}],
        )
        return SimpleNamespace(tables=[], views=[view])

    def wait_for_token(self, pipeline_name, token, timeout_s=None):
        pass

    def completion_status(self, pipeline_name, token):
        return {"status": "complete", "step": self.step}


def _data_chunk(sequence_number: int, x: int) -> dict:
    return {
        "sequence_number": sequence_number,
        "snapshot": False,
        "json_data": [{"insert": {"x": x}}],
    }


def _step_chunk(sequence_number: int, processed_steps: int) -> dict:
    return {
        "sequence_number": sequence_number,
        "snapshot": False,
        "processed_steps": processed_steps,
    }


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


class TestWaitForToken(unittest.TestCase):
    """The tests feed chunks to the handler by hand instead of `start()`."""

    def test_returns_once_the_token_step_ended(self):
        handler = OutputHandler(FakeClient(step=5), "p", "v")
        stream = handler.handler
        read = []

        def waiter():
            handler.wait_for_token("tok", timeout_s=10)
            read.append(handler.to_dict())

        thread = threading.Thread(target=waiter)
        thread.start()

        # A data chunk, a keep-alive and an earlier step do not end the wait.
        stream.to_callback(_data_chunk(3, x=1))
        stream.to_callback({"sequence_number": 4, "snapshot": False})
        stream.to_callback(_step_chunk(5, processed_steps=4))
        thread.join(0.2)
        self.assertTrue(thread.is_alive())

        stream.to_callback(_step_chunk(6, processed_steps=5))
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(read), 1)
        self.assertEqual(read[0][0]["x"], 1)
        self.assertEqual(read[0][0]["insert_delete"], 1)

    def test_data_is_buffered_before_its_step_is_published(self):
        handler = OutputHandler(FakeClient(), "p", "v")
        stream = handler.handler
        steps_when_data_arrived = []
        buffer_data = stream.callback

        def spy(df, seq_no):
            steps_when_data_arrived.append(handler.processed_steps)
            buffer_data(df, seq_no)

        stream.callback = spy
        chunk = _data_chunk(7, x=1)
        chunk["processed_steps"] = 7
        stream.to_callback(chunk)

        # The count was still unpublished while the data was buffered, so a
        # waiter that the count wakes finds the data.
        self.assertEqual(steps_when_data_arrived, [0])
        self.assertEqual(handler.processed_steps, 7)

    def test_wakes_when_the_stream_ends(self):
        handler = OutputHandler(FakeClient(step=5), "p", "v")
        errors = []

        def waiter():
            try:
                handler.wait_for_token("tok")
            except RuntimeError as e:
                errors.append(e)

        thread = threading.Thread(target=waiter)
        thread.start()
        handler.handler.end_callback()
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIn("ended before the changes of step 5", str(errors[0]))

    def test_times_out_without_the_step(self):
        handler = OutputHandler(FakeClient(step=5), "p", "v")
        handler.handler.to_callback(_step_chunk(4, processed_steps=4))
        with self.assertRaises(FelderaTimeoutError):
            handler.wait_for_token("tok", timeout_s=0.05)


if __name__ == "__main__":
    unittest.main()
