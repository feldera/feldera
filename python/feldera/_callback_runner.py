from threading import Thread
from typing import Callable, Optional

import pandas as pd
from feldera import FelderaClient
from feldera._helpers import dataframe_from_response
from feldera.enums import PipelineFieldSelector


class CallbackRunner(Thread):
    def __init__(
        self,
        client: FelderaClient,
        pipeline_name: str,
        view_name: str,
        callback: Callable[[pd.DataFrame, int], None],
    ):
        super().__init__()
        self.daemon = True
        self.client: FelderaClient = client
        self.pipeline_name: str = pipeline_name
        self.view_name: str = view_name
        self.callback: Callable[[pd.DataFrame, int], None] = callback
        self.schema: Optional[dict] = None

    def run(self):
        """
        The main loop of the thread. Listens for data and calls the callback function on each chunk of data received.

        :meta private:
        """

        pipeline = self.client.get_pipeline(
            self.pipeline_name, PipelineFieldSelector.ALL
        )

        schemas = pipeline.tables + pipeline.views
        for schema in schemas:
            if schema.name == self.view_name:
                self.schema = schema
                break

        if self.schema is None:
            raise ValueError(
                f"Table or View {self.view_name} not found in the pipeline schema."
            )

        gen_obj = self.client.listen_to_pipeline(
            self.pipeline_name,
            self.view_name,
            format="json",
            case_sensitive=self.schema.case_sensitive,
        )

        iterator = gen_obj()

        for chunk in iterator:
            chunk: dict = chunk
            data: Optional[list[dict]] = chunk.get("json_data")
            seq_no: Optional[int] = chunk.get("sequence_number")
            if data is not None and seq_no is not None:
                self.callback(
                    dataframe_from_response([data], self.schema.fields), seq_no
                )
