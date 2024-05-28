from enum import Enum
from threading import Thread
from typing import Callable, Optional
from queue import Queue, Empty

import pandas as pd
from feldera import FelderaClient
from feldera._helpers import dataframe_from_response


class _CallbackRunnerInstruction(Enum):
    PipelineStarted = 1
    RanToCompletion = 2


class CallbackRunner(Thread):
    def __init__(
            self,
            client: FelderaClient,
            pipeline_name: str,
            view_name: str,
            callback: Callable[[pd.DataFrame, int], None],
            queue: Optional[Queue],
    ):
        super().__init__()
        self.client: FelderaClient = client
        self.pipeline_name: str = pipeline_name
        self.view_name: str = view_name
        self.callback: Callable[[pd.DataFrame, int], None] = callback
        self.queue: Optional[Queue] = queue

    def run(self):
        """
        The main loop of the thread. Listens for data and calls the callback function on each chunk of data received.

        :meta private:
        """

        # by default, we assume that the pipeline has been started
        ack: _CallbackRunnerInstruction = _CallbackRunnerInstruction.PipelineStarted

        # if there is Queue, we wait for the instruction to start the pipeline
        # this means that we are listening to the pipeline before running it, therefore, all data should be received
        if self.queue:
            ack: _CallbackRunnerInstruction = self.queue.get()

        match ack:

            # if the pipeline has actually been started, we start a listener
            case _CallbackRunnerInstruction.PipelineStarted:

                # listen to the pipeline
                gen_obj = self.client.listen_to_pipeline(self.pipeline_name, self.view_name, format="json")

                # if there is a queue set up, inform the main thread that the listener has been started, and it can
                # proceed with starting the pipeline
                if self.queue:
                    # stop blocking the main thread on `join` for the previous message
                    self.queue.task_done()

                for chunk in gen_obj:
                    chunk: dict = chunk
                    data: list[dict] = chunk.get("json_data")
                    seq_no: int = chunk.get("sequence_number")

                    if data is not None:
                        self.callback(dataframe_from_response([data]), seq_no)

                    if self.queue:
                        try:
                            # if a non-blocking way, check if the queue has received further instructions
                            # this should be a RanToCompletion instruction, which means that the pipeline has been
                            # completed
                            again_ack = self.queue.get_nowait()

                            # if the queue has received a message
                            if again_ack:

                                match again_ack:
                                    case _CallbackRunnerInstruction.RanToCompletion:
                                        # stop blocking the main thread on `join` and return from this thread
                                        self.queue.task_done()

                                        return

                                    case _CallbackRunnerInstruction.PipelineStarted:
                                        # if the pipeline has been started again, which shouldn't happen,
                                        # ignore it and continue listening, call `task_done` to avoid blocking the main
                                        # thread on `join`
                                        self.queue.task_done()

                                        continue
                        except Empty:
                            # if the queue is empty, continue listening
                            continue

            case _CallbackRunnerInstruction.RanToCompletion:
                if self.queue:
                    self.queue.task_done()
                return
