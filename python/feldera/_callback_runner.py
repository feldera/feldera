from threading import Thread
from typing import Callable, Optional
from queue import Queue, Empty

import pandas as pd
from feldera import FelderaClient
from feldera._helpers import dataframe_from_response
from feldera.output_handler import _OutputHandlerInstruction


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

        ack: _OutputHandlerInstruction = _OutputHandlerInstruction.PipelineStarted

        if self.queue:
            ack: _OutputHandlerInstruction = self.queue.get()

        match ack:
            case _OutputHandlerInstruction.PipelineStarted:
                gen_obj = self.client.listen_to_pipeline(self.pipeline_name, self.view_name, format="json")
                if self.queue:
                    self.queue.task_done()

                for chunk in gen_obj:
                    chunk: dict = chunk
                    data: list[dict] = chunk.get("json_data")
                    seq_no: int = chunk.get("sequence_number")

                    if data is not None:
                        self.callback(dataframe_from_response([data]), seq_no)

                    if self.queue:
                        try:
                            again_ack = self.queue.get_nowait()
                            if again_ack:
                                match again_ack:
                                    case _OutputHandlerInstruction.RanToCompletion:
                                        self.queue.task_done()
                                        return
                                    case _OutputHandlerInstruction.PipelineStarted:
                                        self.queue.task_done()
                                        continue
                        except Empty:
                            continue

            case _OutputHandlerInstruction.RanToCompletion:
                if self.queue:
                    self.queue.task_done()
                return
