import logging
import time
from datetime import datetime
import pathlib

import pandas
from uuid import UUID

from typing import List, Dict, Callable, Optional, Generator, Mapping, Any
from collections import deque
from queue import Queue

from feldera.rest.errors import FelderaAPIError
from feldera.enums import (
    PipelineFieldSelector,
    PipelineStatus,
    ProgramStatus,
    CheckpointStatus,
    TransactionStatus,
    StorageStatus,
    DeploymentDesiredStatus,
    DeploymentResourcesDesiredStatus,
    DeploymentResourcesStatus,
    DeploymentRuntimeDesiredStatus,
    DeploymentRuntimeStatus,
)
from feldera.rest.pipeline import Pipeline as InnerPipeline
from feldera.rest.feldera_client import FelderaClient
from feldera._callback_runner import _CallbackRunnerInstruction, CallbackRunner
from feldera.output_handler import OutputHandler
from feldera._helpers import ensure_dataframe_has_columns, chunk_dataframe
from feldera.rest.sql_table import SQLTable
from feldera.rest.sql_view import SQLView
from feldera.runtime_config import RuntimeConfig
from feldera.stats import PipelineStatistics


class Pipeline:
    def __init__(self, client: FelderaClient):
        self.client: FelderaClient = client
        self._inner: InnerPipeline | None = None
        self.views_tx: List[Dict[str, Queue]] = []

    @staticmethod
    def _from_inner(inner: InnerPipeline, client: FelderaClient) -> "Pipeline":
        pipeline = Pipeline(client)
        pipeline._inner = inner
        return pipeline

    def __setup_output_listeners(self):
        """
        Internal function used to set up the output listeners.

        :meta private:
        """

        for view_queue in self.views_tx:
            for view_name, queue in view_queue.items():
                # sends a message to the callback runner to start listening
                queue.put(_CallbackRunnerInstruction.PipelineStarted)
                # block until the callback runner is ready
                queue.join()

    def refresh(self, field_selector: PipelineFieldSelector):
        """
        Calls the backend to get the updated, latest version of the pipeline.

        :param field_selector: Choose what pipeline information to refresh; see PipelineFieldSelector enum definition.

        :raises FelderaConnectionError: If there is an issue connecting to the backend.
        """

        self._inner = self.client.get_pipeline(self.name, field_selector)

    def status(self) -> PipelineStatus:
        """
        Return the current status of the pipeline.
        """

        try:
            self.refresh(PipelineFieldSelector.STATUS)
            return PipelineStatus.from_str(self._inner.deployment_status)

        except FelderaAPIError as err:
            if err.status_code == 404:
                return PipelineStatus.NOT_FOUND
            else:
                raise err

    def stats(self) -> PipelineStatistics:
        """Gets the pipeline metrics and performance counters."""

        return PipelineStatistics.from_dict(self.client.get_pipeline_stats(self.name))

    def logs(self) -> Generator[str, None, None]:
        """Gets the pipeline logs."""

        return self.client.get_pipeline_logs(self.name)

    def input_pandas(self, table_name: str, df: pandas.DataFrame, force: bool = False):
        """
        Push all rows in a pandas DataFrame to the pipeline.

        The pipeline must either be in RUNNING or PAUSED states to push data.
        An error will be raised if the pipeline is in any other state.

        The dataframe must have the same columns as the table in the pipeline.

        :param table_name: The name of the table to insert data into.
        :param df: The pandas DataFrame to be pushed to the pipeline.
        :param force: `True` to push data even if the pipeline is paused. `False` by default.

        :raises ValueError: If the table does not exist in the pipeline.
        :raises RuntimeError: If the pipeline is not in a valid state to push data.
        :raises RuntimeError: If the pipeline is paused and force is not set to `True`.
        """

        status = self.status()
        if status not in [
            PipelineStatus.RUNNING,
            PipelineStatus.PAUSED,
        ]:
            raise RuntimeError("Pipeline must be running or paused to push data")

        if not force and status == PipelineStatus.PAUSED:
            raise RuntimeError("Pipeline is paused, set force=True to push data")

        ensure_dataframe_has_columns(df)

        pipeline = self.client.get_pipeline(self.name, PipelineFieldSelector.ALL)
        if table_name.lower() != "now" and table_name.lower() not in [
            tbl.name.lower() for tbl in pipeline.tables
        ]:
            raise ValueError(
                f"Cannot push to table '{table_name}': table with this name"
                f" does not exist in the '{self.name}' pipeline"
            )
        else:
            # consider validating the schema here
            for datum in chunk_dataframe(df):
                self.client.push_to_pipeline(
                    self.name,
                    table_name,
                    "json",
                    datum.to_json(orient="records", date_format="epoch"),
                    json_flavor="pandas",
                    array=True,
                    serialize=False,
                    force=force,
                )
            return

    def input_json(
        self,
        table_name: str,
        data: Dict | list,
        update_format: str = "raw",
        force: bool = False,
        wait: bool = True,
    ):
        """
        Push this JSON data to the specified table of the pipeline.

        The pipeline must either be in RUNNING or PAUSED states to push data.
        An error will be raised if the pipeline is in any other state.

        :param table_name: The name of the table to push data into.
        :param data: The JSON encoded data to be pushed to the pipeline. The data should be in the form:
            `{'col1': 'val1', 'col2': 'val2'}` or `[{'col1': 'val1', 'col2': 'val2'}, {'col1': 'val1', 'col2': 'val2'}]`
        :param update_format: The update format of the JSON data to be pushed to the pipeline. Must be one of:
            "raw", "insert_delete". https://docs.feldera.com/formats/json#the-insertdelete-format
        :param force: `True` to push data even if the pipeline is paused. `False` by default.
        :param wait: If True, blocks until this input has been processed by the pipeline

        :raises ValueError: If the update format is invalid.
        :raises FelderaAPIError: If the pipeline is not in a valid state to push data.
        :raises RuntimeError: If the pipeline is paused and `force` is not set to `True`.
        """

        status = self.status()
        if not force and status == PipelineStatus.PAUSED:
            raise RuntimeError("Pipeline is paused, set force=True to push data")

        if update_format not in ["raw", "insert_delete"]:
            raise ValueError("update_format must be one of raw or insert_delete")

        array = True if isinstance(data, list) else False
        self.client.push_to_pipeline(
            self.name,
            table_name,
            "json",
            data,
            update_format=update_format,
            array=array,
            force=force,
            wait=wait,
        )

    def pause_connector(self, table_name: str, connector_name: str):
        """
        Pause the specified input connector.

        Connectors allow feldera to fetch data from a source or write data to a sink.
        This method allows users to **PAUSE** a specific **INPUT** connector.
        All connectors are RUNNING by default.

        Refer to the connector documentation for more information:
            https://docs.feldera.com/connectors/#input-connector-orchestration

        :param table_name: The name of the table that the connector is attached to.
        :param connector_name: The name of the connector to pause.

        :raises FelderaAPIError: If the connector is not found, or if the pipeline is not running.
        """

        self.client.pause_connector(self.name, table_name, connector_name)

    def resume_connector(self, table_name: str, connector_name: str):
        """
        Resume the specified connector.

        Connectors allow feldera to fetch data from a source or write data to a sink.
        This method allows users to **RESUME / START** a specific **INPUT** connector.
        All connectors are RUNNING by default.

        Refer to the connector documentation for more information:
            https://docs.feldera.com/connectors/#input-connector-orchestration

        :param table_name: The name of the table that the connector is attached to.
        :param connector_name: The name of the connector to resume.

        :raises FelderaAPIError: If the connector is not found, or if the pipeline is not running.
        """

        self.client.resume_connector(self.name, table_name, connector_name)

    def listen(self, view_name: str) -> OutputHandler:
        """
        Follow the change stream (i.e., the output) of the provided view.
        Returns an output handler to read the changes.

        When the pipeline is stopped, these listeners are dropped.

        You must call this method before starting the pipeline to get the entire output of the view.
        If this method is called once the pipeline has started, you will only get the output from that point onwards.

        :param view_name: The name of the view to listen to.
        """

        queue: Optional[Queue] = None

        if self.status() not in [PipelineStatus.PAUSED, PipelineStatus.RUNNING]:
            queue = Queue(maxsize=1)
            self.views_tx.append({view_name: queue})

        handler = OutputHandler(self.client, self.name, view_name, queue)
        handler.start()

        return handler

    def foreach_chunk(
        self, view_name: str, callback: Callable[[pandas.DataFrame, int], None]
    ):
        """
        Run the given callback on each chunk of the output of the specified view.

        You must call this method before starting the pipeline to operate on the entire output.
        You can call this method after the pipeline has started, but you will only get the output from that point onwards.

        :param view_name: The name of the view.
        :param callback: The callback to run on each chunk. The callback should take two arguments:

                - **chunk**  -> The chunk as a pandas DataFrame
                - **seq_no** -> The sequence number. The sequence number is a monotonically increasing integer that
                  starts from 0. Note that the sequence number is unique for each chunk, but not necessarily contiguous.

        Please note that the callback is run in a separate thread, so it should be thread-safe.
        Please note that the callback should not block for a long time, as by default, backpressure is enabled and
        will block the pipeline.

        .. note::
            - The callback must be thread-safe as it will be run in a separate thread.

        """

        queue: Optional[Queue] = None

        if self.status() not in [PipelineStatus.RUNNING, PipelineStatus.PAUSED]:
            queue = Queue(maxsize=1)
            self.views_tx.append({view_name: queue})

        handler = CallbackRunner(self.client, self.name, view_name, callback, queue)
        handler.start()

    def wait_for_completion(
        self, force_stop: bool = False, timeout_s: Optional[float] = None
    ):
        """
        Block until the pipeline has completed processing all input records.

        This method blocks until (1) all input connectors attached to the
        pipeline have finished reading their input data sources and issued
        end-of-input notifications to the pipeline, and (2) all inputs received
        from these connectors have been fully processed and corresponding
        outputs have been sent out through the output connectors.

        This method will block indefinitely if at least one of the input
        connectors attached to the pipeline is a streaming connector, such as
        Kafka, that does not issue the end-of-input notification.

        :param force_stop: If True, the pipeline will be forcibly stopped after
            completion. False by default. No checkpoints will be made.
        :param timeout_s: Optional. The maximum time (in seconds) to wait for
            the pipeline to complete. The default is None, which means wait
            indefinitely.

        :raises RuntimeError: If the pipeline returns unknown metrics.
        """

        if self.status() not in [
            PipelineStatus.RUNNING,
            PipelineStatus.INITIALIZING,
            PipelineStatus.PROVISIONING,
        ]:
            raise RuntimeError("Pipeline must be running to wait for completion")

        start_time = time.monotonic()

        while True:
            if timeout_s is not None:
                elapsed = time.monotonic() - start_time
                if elapsed > timeout_s:
                    raise TimeoutError(
                        f"timeout ({timeout_s}s) reached while waiting for"
                        f" pipeline '{self.name}' to complete"
                    )
                logging.debug(
                    f"waiting for pipeline {self.name} to complete: elapsed"
                    f" time {elapsed}s, timeout: {timeout_s}s"
                )

            pipeline_complete: bool = self.is_complete()
            if pipeline_complete is None:
                raise RuntimeError(
                    "received unknown metrics from the pipeline, pipeline_complete is None"
                )
            elif pipeline_complete:
                break

            time.sleep(1)

        if force_stop:
            self.stop(force=True)

    def is_complete(self) -> bool:
        """
        Check if the pipeline has completed processing all input records.

        Returns True if (1) all input connectors attached to the
        pipeline have finished reading their input data sources and issued
        end-of-input notifications to the pipeline, and (2) all inputs received
        from these connectors have been fully processed and corresponding
        outputs have been sent out through the output connectors.
        """

        return self.stats().global_metrics.pipeline_complete

    def start(self, wait: bool = True, timeout_s: Optional[float] = None):
        """
        .. _start:

        Starts this pipeline.

        - The pipeline must be in STOPPED state to start.
        - If the pipeline is in any other state, an error will be raised.
        - If the pipeline is in PAUSED state, use `.meth:resume` instead.

        :param timeout_s: The maximum time (in seconds) to wait for the
            pipeline to start.
        :param wait: Set True to wait for the pipeline to start. True by default

        :raises RuntimeError: If the pipeline is not in STOPPED state.
        """

        status = self.status()
        if status != PipelineStatus.STOPPED:
            raise RuntimeError(
                f"""Cannot start pipeline '{self.name}' in state \
'{str(status.name)}'. The pipeline must be in STOPPED state before it can be \
started. You can either stop the pipeline using the `Pipeline.stop()` \
method or use `Pipeline.resume()` to resume a paused pipeline."""
            )

        if not wait:
            if len(self.views_tx) > 0:
                raise ValueError(
                    "cannot start with 'wait=False' when output listeners are configured. Try setting 'wait=True'."
                )

            self.client.start_pipeline(self.name, wait=wait)

            return

        self.client.start_pipeline_as_paused(self.name, wait=wait, timeout_s=timeout_s)
        self.__setup_output_listeners()
        self.resume(timeout_s=timeout_s)

    def restart(self, timeout_s: Optional[float] = None):
        """
        Restarts the pipeline.

        This method forcibly **STOPS** the pipeline regardless of its current
        state and then starts it again. No checkpoints are made when stopping
        the pipeline.

        :param timeout_s: The maximum time (in seconds) to wait for the
            pipeline to restart.
        """

        self.stop(force=True, timeout_s=timeout_s)
        self.start(timeout_s=timeout_s)

    def wait_for_idle(
        self,
        idle_interval_s: float = 5.0,
        timeout_s: float = 600.0,
        poll_interval_s: float = 0.2,
    ):
        """
        Wait for the pipeline to become idle and then returns.

        Idle is defined as a sufficiently long interval in which the number of
        input and processed records reported by the pipeline do not change, and
        they equal each other (thus, all input records present at the pipeline
        have been processed).

        :param idle_interval_s: Idle interval duration (default is 5.0 seconds).
        :param timeout_s: Timeout waiting for idle (default is 600.0 seconds).
        :param poll_interval_s: Polling interval, should be set substantially
            smaller than the idle interval (default is 0.2 seconds).
        :raises ValueError: If idle interval is larger than timeout, poll interval
            is larger than timeout, or poll interval is larger than idle interval.
        :raises RuntimeError: If the metrics are missing or the timeout was
            reached.
        """
        if idle_interval_s > timeout_s:
            raise ValueError(
                f"idle interval ({idle_interval_s}s) cannot be larger than"
                f" timeout ({timeout_s}s)"
            )
        if poll_interval_s > timeout_s:
            raise ValueError(
                f"poll interval ({poll_interval_s}s) cannot be larger than"
                f" timeout ({timeout_s}s)"
            )
        if poll_interval_s > idle_interval_s:
            raise ValueError(
                f"poll interval ({poll_interval_s}s) cannot be larger "
                f"than idle interval ({idle_interval_s}s)"
            )

        start_time_s = time.monotonic()
        idle_started_s = None
        prev = (0, 0)
        while True:
            now_s = time.monotonic()

            # Metrics retrieval
            metrics = self.stats().global_metrics
            total_input_records = metrics.total_input_records
            total_processed_records = metrics.total_processed_records
            if metrics.total_input_records is None:
                raise RuntimeError(
                    "total_input_records is missing from the pipeline metrics"
                )
            if metrics.total_processed_records is None:
                raise RuntimeError(
                    """total_processed_records is missing from the pipeline \
metrics"""
                )

            # Idle check
            unchanged = (
                prev[0] == total_input_records and prev[1] == total_processed_records
            )
            equal = total_input_records == total_processed_records
            prev = (total_input_records, total_processed_records)
            if unchanged and equal:
                if idle_started_s is None:
                    idle_started_s = now_s
            else:
                idle_started_s = None
            if idle_started_s is not None and now_s - idle_started_s >= idle_interval_s:
                return

            # Timeout
            if now_s - start_time_s >= timeout_s:
                raise RuntimeError(f"waiting for idle reached timeout ({timeout_s}s)")
            time.sleep(poll_interval_s)

    def activate(self, wait: bool = True, timeout_s: Optional[float] = None):
        """
        Activates the pipeline when starting from STANDBY mode. Only applicable
        when the pipeline is starting from a checkpoint in object store.

        :param wait: Set True to wait for the pipeline to activate. True by
            default
        :param timeout_s: The maximum time (in seconds) to wait for the
            pipeline to pause.
        """

        self.client.activate_pipeline(self.name, wait=wait, timeout_s=timeout_s)

    def start_paused(self, wait: bool = True, timeout_s: Optional[float] = None):
        """
        Starts the pipeline in the paused state.
        """

        self.client.start_pipeline_as_paused(self.name, wait=wait, timeout_s=timeout_s)

    def start_standby(self, wait: bool = True, timeout_s: Optional[float] = None):
        """
        Starts the pipeline in the standby state.
        """

        self.client.start_pipeline_as_standby(self.name, wait=wait, timeout_s=timeout_s)

    def pause(self, wait: bool = True, timeout_s: Optional[float] = None):
        """
        Pause the pipeline.

        The pipeline can only transition to the PAUSED state from the RUNNING
        state. If the pipeline is already paused, it will remain in the PAUSED
        state.

        :param wait: Set True to wait for the pipeline to pause. True by default
        :param timeout_s: The maximum time (in seconds) to wait for the
            pipeline to pause.
        """

        self.client.pause_pipeline(self.name, wait=wait, timeout_s=timeout_s)

    def stop(self, force: bool, wait: bool = True, timeout_s: Optional[float] = None):
        """
        Stops the pipeline.

        Stops the pipeline regardless of its current state.

        :param force: Set True to immediately scale compute resources to zero.
            Set False to automatically checkpoint before stopping.
        :param wait: Set True to gracefully shutdown listeners and wait for the
            pipeline to stop. True by default.
        :param timeout_s: The maximum time (in seconds) to wait for the
            pipeline to stop.
        """

        if wait:
            for view_queue in self.views_tx:
                for _, queue in view_queue.items():
                    # sends a message to the callback runner to stop listening
                    queue.put(_CallbackRunnerInstruction.RanToCompletion)

            if len(self.views_tx) > 0:
                while self.views_tx:
                    view = self.views_tx.pop()
                    for view_name, queue in view.items():
                        # block until the callback runner has been stopped
                        queue.join()

        time.sleep(3)
        self.client.stop_pipeline(
            self.name, force=force, wait=wait, timeout_s=timeout_s
        )

    def resume(self, wait: bool = True, timeout_s: Optional[float] = None):
        """
        Resumes the pipeline from the PAUSED state. If the pipeline is already
        running, it will remain in the RUNNING state.

        :param wait: Set True to wait for the pipeline to resume. True by default
        :param timeout_s: The maximum time (in seconds) to wait for the
            pipeline to resume.
        """

        self.client.resume_pipeline(self.name, wait=wait, timeout_s=timeout_s)

    def start_transaction(self) -> int:
        """
        Start a new transaction.

        :return: Transaction ID.

        :raises FelderaAPIError: If the pipeline fails to start a transaction, e.g., if the pipeline is not running or
            there is already an active transaction.
        """

        return self.client.start_transaction(self.name)

    def commit_transaction(
        self,
        transaction_id: Optional[int] = None,
        wait: bool = True,
        timeout_s: Optional[float] = None,
    ):
        """
        Commit the currently active transaction.

        :param transaction_id: If provided, the function verifies that the currently active transaction matches this ID.
            If the active transaction ID does not match, the function raises an error.

        :param wait: If True, the function blocks until the transaction either commits successfully or the timeout is reached.
            If False, the function initiates the commit and returns immediately without waiting for completion. The default value is True.

        :param timeout_s: Maximum time (in seconds) to wait for the transaction to commit when `wait` is True.
            If None, the function will wait indefinitely.

        :raises RuntimeError: If there is currently no transaction in progress.
        :raises ValueError: If the provided `transaction_id` does not match the current transaction.
        :raises TimeoutError: If the transaction does not commit within the specified timeout (when `wait` is True).
        :raises FelderaAPIError: If the pipeline fails to commit a transaction.
        """

        self.client.commit_transaction(self.name, transaction_id, wait, timeout_s)

    def transaction_status(self) -> TransactionStatus:
        """
        Get pipeline's transaction handling status.

        :return: Current transaction handling status of the pipeline.

        :raises FelderaAPIError: If pipeline's status couldn't be read, e.g., because the pipeline is not currently running.
        """

        return self.stats().global_metrics.transaction_status

    def transaction_id(self) -> Optional[int]:
        """
        Gets the ID of the currently active transaction or None if there is no active transaction.

        :return: The ID of the transaction.
        """

        transaction_id = self.stats().global_metrics.transaction_id

        if transaction_id == 0:
            return None
        else:
            return transaction_id

    def delete(self, clear_storage: bool = False):
        """
        Deletes the pipeline.

        The pipeline must be stopped, and the storage cleared before it can be
        deleted.

        :param clear_storage: True if the storage should be cleared before
            deletion. False by default

        :raises FelderaAPIError: If the pipeline is not in STOPPED state or the
            storage is still bound.
        """

        if clear_storage:
            self.clear_storage()
        self.client.delete_pipeline(self.name)

    @staticmethod
    def get(name: str, client: FelderaClient) -> "Pipeline":
        """
        Get the pipeline if it exists.

        :param name: The name of the pipeline.
        :param client: The FelderaClient instance.
        """

        try:
            inner = client.get_pipeline(name, PipelineFieldSelector.ALL)
            return Pipeline._from_inner(inner, client)
        except FelderaAPIError as err:
            if err.status_code == 404:
                err.message = f"Pipeline with name {name} not found"
                raise err

    def checkpoint(self, wait: bool = False, timeout_s=300) -> int:
        """
        Checkpoints this pipeline.

        :param wait: If true, will block until the checkpoint completes.
        :param timeout_s: The maximum time (in seconds) to wait for the
            checkpoint to complete.

        :return: The checkpoint sequence number.

        :raises FelderaAPIError: If enterprise features are not enabled.
        """

        seq = self.client.checkpoint_pipeline(self.name)

        if not wait:
            return seq

        start = time.time()

        while True:
            elapsed = time.monotonic() - start
            if elapsed > timeout_s:
                raise TimeoutError(
                    f"""timeout ({timeout_s}s) reached while waiting for \
pipeline '{self.name}' to make checkpoint '{seq}'"""
                )
            status = self.checkpoint_status(seq)
            if status == CheckpointStatus.InProgress:
                time.sleep(0.1)
                continue

            return seq

    def checkpoint_status(self, seq: int) -> CheckpointStatus:
        """
        Checks the status of the given checkpoint.

        :param seq: The checkpoint sequence number.
        """

        resp = self.client.checkpoint_pipeline_status(self.name)
        success = resp.get("success")
        if seq == success:
            return CheckpointStatus.Success

        fail = resp.get("failure") or {}
        if seq == fail.get("sequence_number"):
            failure = CheckpointStatus.Failure
            failure.error = fail.get("error", "")
            return failure

        if (success is None) or seq > success:
            return CheckpointStatus.InProgress

        if seq < success:
            return CheckpointStatus.Unknown

    def sync_checkpoint(self, wait: bool = False, timeout_s=300) -> str:
        """
        Syncs this checkpoint to object store.

        :param wait: If true, will block until the checkpoint sync operation
            completes.
        :param timeout_s: The maximum time (in seconds) to wait for the
            checkpoint to complete syncing.

        :raises FelderaAPIError: If no checkpoints have been made.
        :raises RuntimeError: If syncing the checkpoint fails.
        """

        uuid = self.client.sync_checkpoint(self.name)

        if not wait:
            return uuid

        start = time.time()

        while True:
            elapsed = time.monotonic() - start
            if elapsed > timeout_s:
                raise TimeoutError(
                    f"""timeout ({timeout_s}s) reached while waiting for \
pipeline '{self.name}' to sync checkpoint '{uuid}'"""
                )
            status = self.sync_checkpoint_status(uuid)
            if status == CheckpointStatus.Failure:
                raise RuntimeError(
                    f"failed to sync checkpoint '{uuid}': ", status.get_error()
                )

            if status in [CheckpointStatus.InProgress, CheckpointStatus.Unknown]:
                time.sleep(0.1)
                continue

            break

        return uuid

    def sync_checkpoint_status(self, uuid: str) -> CheckpointStatus:
        """
        Checks the status of the given checkpoint sync operation.
        If the checkpoint is currently being synchronized, returns
        `CheckpointStatus.Unknown`.

        Failures are not raised as runtime errors and must be explicitly
        checked.

        :param uuid: The checkpoint uuid.
        """

        resp = self.client.sync_checkpoint_status(self.name)
        success = resp.get("success")

        fail = resp.get("failure") or {}

        if uuid == success:
            return CheckpointStatus.Success

        fail = resp.get("failure") or {}
        if uuid == fail.get("uuid"):
            failure = CheckpointStatus.Failure
            failure.error = fail.get("error", "")
            logging.error(f"failed to sync checkpoint '{uuid}': {failure.error}")
            return failure

        if (success is None) or UUID(uuid) > UUID(success):
            return CheckpointStatus.InProgress

        return CheckpointStatus.Unknown

    def query(self, query: str) -> Generator[Mapping[str, Any], None, None]:
        """
        Executes an ad-hoc SQL query on this pipeline and returns a generator
        that yields the rows of the result as Python dictionaries. For
        ``INSERT`` and ``DELETE`` queries, consider using :meth:`.execute`
        instead. All floating-point numbers are deserialized as Decimal objects
        to avoid precision loss.

        Note:
            You can only ``SELECT`` from materialized tables and views.

        Important:
            This method is lazy. It returns a generator and is not evaluated
            until you consume the result.

        :param query: The SQL query to be executed.
        :return: A generator that yields the rows of the result as Python
            dictionaries.

        :raises FelderaAPIError: If the pipeline is not in a RUNNING or PAUSED
            state.
        :raises FelderaAPIError: If querying a non materialized table or view.
        :raises FelderaAPIError: If the query is invalid.
        """

        return self.client.query_as_json(self.name, query)

    def query_parquet(self, query: str, path: str):
        """
        Executes an ad-hoc SQL query on this pipeline and saves the result to
        the specified path as a parquet file. If the extension isn't `parquet`,
        it will be automatically appended to `path`.

        Note:
            You can only ``SELECT`` from materialized tables and views.

        :param query: The SQL query to be executed.
        :param path: The path of the parquet file.

        :raises FelderaAPIError: If the pipeline is not in a RUNNING or PAUSED
            state.
        :raises FelderaAPIError: If querying a non materialized table or view.
        :raises FelderaAPIError: If the query is invalid.
        """

        self.client.query_as_parquet(self.name, query, path)

    def query_tabular(self, query: str) -> Generator[str, None, None]:
        """
        Executes a SQL query on this pipeline and returns the result as a
        formatted string.

        Note:
            You can only ``SELECT`` from materialized tables and views.

        Important:
            This method is lazy. It returns a generator and is not evaluated
            until you consume the result.

        :param query: The SQL query to be executed.
        :return: A generator that yields a string representing the query result
            in a human-readable, tabular format.

        :raises FelderaAPIError: If the pipeline is not in a RUNNING or PAUSED
            state.
        :raises FelderaAPIError: If querying a non materialized table or view.
        :raises FelderaAPIError: If the query is invalid.
        """

        return self.client.query_as_text(self.name, query)

    def query_hash(self, query: str):
        """
        Executes an ad-hoc SQL query on this pipeline and returns the result
        as a hash of the result set. This is useful for quickly checking
        if the result set has changed without retrieving the entire result.

        Note:
            For a stable hash, the query must be deterministic which means
            it should be sorted.

        :param query: The SQL query to be executed.

        :raises FelderaAPIError: If the pipeline is not in a RUNNING or PAUSED
            state.
        :raises FelderaAPIError: If querying a non materialized table or view.
        :raises FelderaAPIError: If the query is invalid.
        """
        return self.client.query_as_hash(self.name, query)

    def execute(self, query: str):
        """
        Executes an ad-hoc SQL query on the current pipeline, discarding its
        result. Unlike the :meth:`.query` method which returns a generator for
        retrieving query results lazily, this method processes the query
        eagerly and fully before returning.

        This method is suitable for SQL operations like ``INSERT`` and
        ``DELETE``, where the user needs confirmation of successful query
        execution, but does not require the query result. If the query fails,
        an exception will be raised.

        Important:
            If you try to ``INSERT`` or ``DELETE`` data from a table while the
            pipeline is paused, it will block until the pipeline is resumed.

        :param query: The SQL query to be executed.

        :raises FelderaAPIError: If the pipeline is not in a RUNNING state.
        :raises FelderaAPIError: If the query is invalid.
        """

        gen = self.query_tabular(query)
        deque(gen, maxlen=0)

    def clear_storage(self):
        """
        Clears the storage of the pipeline if it is currently in use.
        This action cannot be canceled, and will delete all the pipeline
        storage.
        """

        if self.storage_status() == StorageStatus.INUSE:
            self.client.clear_storage(self.name)

    @property
    def name(self) -> str:
        """
        Return the name of the pipeline.
        """

        return self._inner.name

    def program_code(self) -> str:
        """
        Return the program SQL code of the pipeline.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.program_code

    def modify(
        self,
        sql: Optional[str] = None,
        udf_rust: Optional[str] = None,
        udf_toml: Optional[str] = None,
        program_config: Optional[Mapping[str, Any]] = None,
        runtime_config: Optional[Mapping[str, Any]] = None,
        description: Optional[str] = None,
    ):
        """
        Modify the pipeline.

        Modify the values of pipeline attributes: SQL code, UDF Rust code,
        UDF Rust dependencies (TOML), program config, runtime config, and
        description. Only the provided attributes will be modified. Other
        attributes will remain unchanged.

        The pipeline must be in the STOPPED state to be modified.

        :raises FelderaAPIError: If the pipeline is not in a STOPPED state.
        """

        self.client.patch_pipeline(
            name=self._inner.name,
            sql=sql,
            udf_rust=udf_rust,
            udf_toml=udf_toml,
            program_config=program_config,
            runtime_config=runtime_config,
            description=description,
        )

    def storage_status(self) -> StorageStatus:
        """
        Return the storage status of the pipeline.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return StorageStatus.from_str(self._inner.storage_status)

    def program_status(self) -> ProgramStatus:
        """
        Return the program status of the pipeline.

        Program status is the status of compilation of this SQL program.
        We first compile the SQL program to Rust code, and then compile the
        Rust code to a binary.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return ProgramStatus.from_value(self._inner.program_status)

    def program_status_since(self) -> datetime:
        """
        Return the timestamp when the current program status was set.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return datetime.fromisoformat(self._inner.program_status_since)

    def udf_rust(self) -> str:
        """
        Return the Rust code for UDFs.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.udf_rust

    def udf_toml(self) -> str:
        """
        Return the Rust dependencies required by UDFs (in the TOML format).
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.udf_toml

    def program_config(self) -> Mapping[str, Any]:
        """
        Return the program config of the pipeline.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.program_config

    def runtime_config(self) -> RuntimeConfig:
        """
        Return the runtime config of the pipeline.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return RuntimeConfig.from_dict(self._inner.runtime_config)

    def set_runtime_config(self, runtime_config: RuntimeConfig):
        """Updates the runtime config of the pipeline.  The pipeline
        must be stopped.  Changing some pipeline configuration, such
        as the number of workers, requires storage to be cleared.

        For example, to set 'min_batch_size_records' on a pipeline::

            runtime_config = pipeline.runtime_config()
            runtime_config.min_batch_size_records = 500
            pipeline.set_runtime_config(runtime_config)

        """

        self.client.patch_pipeline(
            name=self._inner.name, runtime_config=runtime_config.to_dict()
        )

    def id(self) -> str:
        """
        Return the ID of the pipeline.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return self._inner.id

    def description(self) -> str:
        """
        Return the description of the pipeline.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return self._inner.description

    def tables(self) -> List[SQLTable]:
        """
        Return the tables of the pipeline.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.tables

    def views(self) -> List[SQLView]:
        """
        Return the views of the pipeline.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.views

    def created_at(self) -> datetime:
        """
        Return the creation time of the pipeline.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return datetime.fromisoformat(self._inner.created_at)

    def version(self) -> int:
        """
        Return the version of the pipeline.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return self._inner.version

    def program_version(self) -> int:
        """
        Return the program version of the pipeline.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return self._inner.program_version

    def deployment_status_since(self) -> datetime:
        """
        Return the timestamp when the current deployment status of the pipeline
        was set.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return datetime.fromisoformat(self._inner.deployment_status_since)

    def deployment_config(self) -> Mapping[str, Any]:
        """
        Return the deployment config of the pipeline.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.deployment_config

    def deployment_desired_status(self) -> DeploymentDesiredStatus:
        """
        Return the desired deployment status of the pipeline.
        This is the next state that the pipeline should transition to.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return DeploymentDesiredStatus.from_str(self._inner.deployment_desired_status)

    def deployment_resources_desired_status(self) -> DeploymentResourcesDesiredStatus:
        """
        Return the desired status of the the deployment resources.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return DeploymentResourcesDesiredStatus.from_str(
            self._inner.deployment_resources_desired_status
        )

    def deployment_resources_status(self) -> DeploymentResourcesStatus:
        """
        Return the status of the deployment resources.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return DeploymentResourcesStatus.from_str(
            self._inner.deployment_resources_status
        )

    def deployment_runtime_desired_status(self) -> DeploymentRuntimeDesiredStatus:
        """
        Return the deployment runtime desired status.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return DeploymentRuntimeDesiredStatus.from_str(
            self._inner.deployment_runtime_desired_status
        )

    def deployment_runtime_status(self) -> DeploymentRuntimeStatus:
        """
        Return the deployment runtime status.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return DeploymentRuntimeStatus.from_str(self._inner.deployment_runtime_status)

    def deployment_error(self) -> Mapping[str, Any]:
        """
        Return the deployment error of the pipeline.
        Returns an empty string if there is no error.
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return self._inner.deployment_error

    def deployment_location(self) -> str:
        """
        Return the deployment location of the pipeline.
        Deployment location is the location where the pipeline can be reached
        at runtime (a TCP port number or a URI).
        """

        self.refresh(PipelineFieldSelector.STATUS)
        return self._inner.deployment_location

    def program_info(self) -> Mapping[str, Any]:
        """
        Return the program info of the pipeline.
        This is the output returned by the SQL compiler, including: the list of
        input and output connectors, the generated Rust code for the pipeline,
        and the SQL program schema.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.program_info

    def program_error(self) -> Mapping[str, Any]:
        """
        Return the program error of the pipeline.
        If there are no errors, the `exit_code` field inside both
        `sql_compilation` and `rust_compilation` will be 0.
        """

        self.refresh(PipelineFieldSelector.ALL)
        return self._inner.program_error

    def errors(self) -> List[Mapping[str, Any]]:
        """
        Returns a list of all errors in this pipeline.
        """
        errors = []
        perr = self.program_error()
        for e in perr.keys():
            err = perr.get(e)
            if err and err.get("exit_code", 0) != 0:
                errors.append({e: err})
        derr = self.deployment_error()
        if derr:
            errors.append(derr)
        return errors

    def support_bundle(
        self,
        output_path: Optional[str] = None,
        *,
        circuit_profile: bool = True,
        heap_profile: bool = True,
        metrics: bool = True,
        logs: bool = True,
        stats: bool = True,
        pipeline_config: bool = True,
        system_config: bool = True,
    ) -> bytes:
        """
        Generate a support bundle containing diagnostic information from this pipeline.

        This method collects various diagnostic data from the pipeline including
        circuit profile, heap profile, metrics, logs, stats, and connector statistics,
        and packages them into a single ZIP file for support purposes.

        :param output_path: Optional path to save the support bundle file. If None,
            the support bundle is only returned as bytes.
        :param circuit_profile: Whether to collect circuit profile data (default: True)
        :param heap_profile: Whether to collect heap profile data (default: True)
        :param metrics: Whether to collect metrics data (default: True)
        :param logs: Whether to collect logs data (default: True)
        :param stats: Whether to collect stats data (default: True)
        :param pipeline_config: Whether to collect pipeline configuration data (default: True)
        :param system_config: Whether to collect system configuration data (default: True)
        :return: The support bundle as bytes (ZIP archive)
        :raises FelderaAPIError: If the pipeline does not exist or if there's an error
        """

        # Build query parameters
        params = {}
        if not circuit_profile:
            params["circuit_profile"] = "false"
        if not heap_profile:
            params["heap_profile"] = "false"
        if not metrics:
            params["metrics"] = "false"
        if not logs:
            params["logs"] = "false"
        if not stats:
            params["stats"] = "false"
        if not pipeline_config:
            params["pipeline_config"] = "false"
        if not system_config:
            params["system_config"] = "false"

        support_bundle_bytes = self.client.get_pipeline_support_bundle(
            self.name, params=params
        )

        if output_path is not None:
            path = pathlib.Path(output_path)

            # Ensure the file has .zip extension
            if path.suffix != ".zip":
                path = path.with_suffix(".zip")

            with open(path, "wb") as f:
                f.write(support_bundle_bytes)

            print(f"Support bundle written to {path}")

        return support_bundle_bytes
