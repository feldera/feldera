import pandas as pd

from queue import Queue
from feldera import FelderaClient
from feldera._callback_runner import CallbackRunner


class OutputHandler:
    def __init__(self, client: FelderaClient, pipeline_name: str, view_name: str, queue: Queue):
        """
        Initializes the output handler, but doesn't start it.
        To start the output handler, call the `.OutputHandler.start` method.
        """

        self.client: FelderaClient = client
        self.pipeline_name: str = pipeline_name
        self.view_name: str = view_name
        self.queue: Queue = queue
        self.buffer: list[pd.DataFrame] = []

        # the callback that is passed to the `CallbackRunner`
        def callback(df: pd.DataFrame, _: int):
            if not df.empty:
                self.buffer.append(df)

        # sets up the callback runner
        self.handler = CallbackRunner(self.client, self.pipeline_name, self.view_name, callback, queue)

    def start(self):
        """
        Starts the output handler in a separate thread
        """

        self.handler.start()

    def to_pandas(self):
        """
        Returns the output of the pipeline as a pandas DataFrame
        """

        self.handler.join()

        if len(self.buffer) == 0:
            return pd.DataFrame()
        return pd.concat(self.buffer, ignore_index=True)
