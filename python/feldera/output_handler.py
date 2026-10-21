import time

import pandas as pd

from typing import Optional
from threading import Condition, Event

from feldera import FelderaClient
from feldera._callback_runner import CallbackRunner
from feldera.rest.errors import FelderaTimeoutError


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
        # Guards `buffer`, `processed_steps`, `exception` and `ended`, and is
        # notified when one of the last three changes.
        self.changed = Condition()
        # The number of pipeline steps whose changes this handle has received.
        self.processed_steps: int = 0
        self.exception: Optional[BaseException] = None
        # True once the stream ended without an exception.
        self.ended: bool = False
        self.event = Event()

        # the callback that is passed to the `CallbackRunner`
        def callback(df: pd.DataFrame, _: int):
            if not df.empty:
                with self.changed:
                    self.buffer.append(df)

        def progress_callback(processed_steps: int):
            with self.changed:
                self.processed_steps = max(self.processed_steps, processed_steps)
                self.changed.notify_all()

        def exception_callback(exception: BaseException):
            with self.changed:
                self.exception = exception
                self.changed.notify_all()

        def end_callback():
            with self.changed:
                self.ended = True
                self.changed.notify_all()

        # sets up the callback runner
        self.handler = CallbackRunner(
            self.client,
            self.pipeline_name,
            self.view_name,
            callback,
            exception_callback,
            self.event,
            progress_callback,
            end_callback,
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
        with self.changed:
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

    def wait_for_token(self, token: str, timeout_s: Optional[float] = None):
        """
        Blocks until this handle has received every change that the
        completion token covers.

        :meth:`.Pipeline.wait_for_token` returns once the pipeline has handed
        the changes to the view's output connectors; the changes can still be
        on their way to this handle.  This method waits until the handle has
        received them, so :meth:`to_pandas` afterwards returns every change
        for the token, along with any later change that may have arrived.  If
        the input caused no view changes, the method still waits until the
        stream has passed the token's step; an empty result received afterwards
        means that the input caused no view changes.

        The handle receives only changes made after it started listening, so
        create it before sending the input.  For a token that completed
        before that, this method returns only at the pipeline's next step.

        :param token: A completion token, as returned by
            :meth:`.Pipeline.input_json` or :meth:`.Pipeline.input_pandas`.
        :param timeout_s: The amount of time in seconds to wait; `None` waits
            without limit.
        :raises FelderaTimeoutError: when the changes have not arrived after
            `timeout_s` seconds.
        """

        end = time.monotonic() + timeout_s if timeout_s else None
        if self.exception is not None:
            raise self.exception
        # Once the token is complete, the stream holds the changes of every
        # step up to the token's step, followed by the chunk that marks the
        # end of that step's output.
        self.client.wait_for_token(self.pipeline_name, token, timeout_s)
        step: int = self.client.completion_status(self.pipeline_name, token)["step"]
        with self.changed:
            while self.processed_steps < step:
                if self.exception is not None:
                    raise self.exception
                if self.ended:
                    raise RuntimeError(
                        f"the change stream of '{self.view_name}' ended before"
                        f" the changes of step {step}, which the token waits"
                        " for, arrived"
                    )
                remaining = None
                if end is not None:
                    remaining = end - time.monotonic()
                    if remaining <= 0:
                        raise FelderaTimeoutError(
                            f"timeout error: the changes of step {step} of"
                            f" '{self.view_name}', which the token waits for,"
                            f" did not arrive within {timeout_s} seconds"
                        )
                self.changed.wait(remaining)
