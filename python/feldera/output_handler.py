import pandas as pd
from typing import Optional

from queue import Queue
from feldera import FelderaClient
from feldera._callback_runner import CallbackRunner


class OutputHandler:
    def __init__(
        self,
        client: FelderaClient,
        pipeline_name: str,
        view_name: str,
        queue: Optional[Queue],
    ):
        """
        Initializes the output handler, but doesn't start it.
        To start the output handler, call the `.OutputHandler.start` method.
        """

        self.client: FelderaClient = client
        self.pipeline_name: str = pipeline_name
        self.view_name: str = view_name
        self.queue: Optional[Queue] = queue
        self.buffer: list[pd.DataFrame] = []

        # the callback that is passed to the `CallbackRunner`
        def callback(df: pd.DataFrame, _: int):
            if not df.empty:
                self.buffer.append(df)

        # sets up the callback runner
        self.handler = CallbackRunner(
            self.client, self.pipeline_name, self.view_name, callback, queue
        )

    def start(self):
        """
        Starts the output handler in a separate thread
        """

        self.handler.start()

    def to_pandas(self, clear_buffer: bool = True):
        """
        Returns the output of the pipeline as a pandas DataFrame

        :param clear_buffer: Whether to clear the buffer after getting the output.
        """

        if len(self.buffer) == 0:
            return pd.DataFrame()
        res = pd.concat(self.buffer, ignore_index=True)
        if clear_buffer:
            self.buffer.clear()

        return res

    def to_dict(self, clear_buffer: bool = True):
        """
        Returns the output of the pipeline as a list of python dictionaries

        :param clear_buffer: Whether to clear the buffer after getting the output.
        """

        return self.to_pandas(clear_buffer).to_dict(orient="records")
