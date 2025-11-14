import pandas as pd

from typing import Optional
from threading import Event

from feldera import FelderaClient
from feldera._callback_runner import CallbackRunner


class OutputHandler:
    def __init__(
        self,
        client: FelderaClient,
        pipeline_name: str,
        view_name: str,
    ):
        """
        Initializes the output handler, but doesn't start it.
        To start the output handler, call the `.OutputHandler.start` method.
        """

        self.client: FelderaClient = client
        self.pipeline_name: str = pipeline_name
        self.view_name: str = view_name
        self.buffer: list[pd.DataFrame] = []
        self.exception: Optional[BaseException] = None
        self.event = Event()

        # the callback that is passed to the `CallbackRunner`
        def callback(df: pd.DataFrame, _: int):
            if not df.empty:
                self.buffer.append(df)

        def exception_callback(exception: BaseException):
            self.exception = exception

        # sets up the callback runner
        self.handler = CallbackRunner(
            self.client,
            self.pipeline_name,
            self.view_name,
            callback,
            exception_callback,
            self.event,
        )

    def start(self):
        """
        Starts the output handler in a separate thread
        """

        self.handler.start()
        _ = self.event.wait()

    def to_pandas(self, clear_buffer: bool = True):
        """
        Returns the output of the pipeline as a pandas DataFrame

        :param clear_buffer: Whether to clear the buffer after getting the output.
        """

        if self.exception is not None:
            raise self.exception
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
