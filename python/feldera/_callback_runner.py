from threading import Thread, Event
from typing import Callable, List, Optional, Mapping, Any

import pandas as pd
from feldera import FelderaClient
from feldera._helpers import dataframe_from_response
from feldera.enums import PipelineFieldSelector
from feldera.rest.sql_table import SQLTable
from feldera.rest.sql_view import SQLView
from feldera.rest.pipeline import Pipeline


class CallbackRunner(Thread):
    def __init__(
        self,
        client: FelderaClient,
        pipeline_name: str,
        view_name: str,
        callback: Callable[[pd.DataFrame, int], None],
        exception_callback: Callable[[BaseException], None],
        event: Event,
    ):
        """
        :param client: The :class:`.FelderaClient` to use.
        :param pipeline_name: The name of the current pipeline.
        :param view_name: The name of the view we are listening to.
        :param callback: The callback function to call on the data we receive.
        :param exception_callback: The callback function to call when an exception occurs.
        :param event: The event to wait for before starting the callback runner.
        """

        super().__init__()
        self.daemon = True
        self.client: FelderaClient = client
        self.pipeline_name: str = pipeline_name
        self.view_name: str = view_name
        self.callback: Callable[[pd.DataFrame, int], None] = callback
        self.exception_callback: Callable[[BaseException], None] = exception_callback
        self.event: Event = event

        self.pipeline: Pipeline = self.client.get_pipeline(
            self.pipeline_name, PipelineFieldSelector.ALL
        )

        view_schema = None

        schemas: List[SQLTable | SQLView] = self.pipeline.tables + self.pipeline.views
        for schema in schemas:
            if schema.name == self.view_name:
                view_schema = schema
                break

        if view_schema is None:
            raise ValueError(
                f"Table or View {self.view_name} not found in the pipeline schema."
            )

        self.schema: SQLTable | SQLView = view_schema

    def to_callback(self, chunk: Mapping[str, Any]):
        data: Optional[list[Mapping[str, Any]]] = chunk.get("json_data")
        seq_no: Optional[int] = chunk.get("sequence_number")
        if data is not None and seq_no is not None:
            self.callback(dataframe_from_response([data], self.schema.fields), seq_no)

    def run(self):
        """
        The main loop of the thread. Listens for data and calls the callback function on each chunk of data received.

        :meta private:
        """

        try:
            gen_obj = self.client.listen_to_pipeline(
                self.pipeline_name,
                self.view_name,
                format="json",
                case_sensitive=self.schema.case_sensitive,
            )

            iterator = gen_obj()

            # Trigger the HTTP call
            chunk = next(iterator)

            # Unblock the main thread
            self.event.set()

            self.to_callback(chunk)

            for chunk in iterator:
                self.to_callback(chunk)

        except BaseException as e:
            self.exception_callback(e)
