import pandas as pd


from threading import Thread
from queue import Queue, Empty
from feldera import FelderaClient
from enum import Enum


class _OutputHandlerInstruction(Enum):
    PipelineStarted = 1
    RanToCompletion = 2


class OutputHandler(Thread):
    def __init__(self, client: FelderaClient, pipeline_name: str, view_name: str, queue: Queue):
        super().__init__()
        self.client: FelderaClient = client
        self.pipeline_name: str = pipeline_name
        self.view_name: str = view_name
        self.queue: Queue = queue
        self.buffer: list[list[dict]] = []

    def run(self):
        """
        The main loop of the thread. It listens to the pipeline and appends the data to the buffer.
        Doesn't do integration, just takes the data and ignores if they are `insert`s or `delete`s.

        :meta private:
        """

        ack: _OutputHandlerInstruction = self.queue.get()

        match ack:
            case _OutputHandlerInstruction.PipelineStarted:
                gen_obj = self.client.listen_to_pipeline(self.pipeline_name, self.view_name, format="json")
                self.queue.task_done()

                for chunk in gen_obj:
                    chunk: dict = chunk
                    data: list[dict] = chunk.get("json_data")

                    if data:
                        self.buffer.append(data)

                    try:
                        again_ack: _OutputHandlerInstruction = self.queue.get(block=False)
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
                self.queue.task_done()
                return

    def to_pandas(self):
        """
        Converts the output of the pipeline to a pandas DataFrame
        """
        self.join()
        return pd.DataFrame([
            {**item['insert'], 'insert_delete': 1} if 'insert' in item else {**item['delete'], 'insert_delete': -1}
            for sublist in self.buffer for item in sublist
        ])
