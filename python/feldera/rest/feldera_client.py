import json
import logging
import pathlib
import time
from decimal import Decimal
from typing import Any, Dict, Generator, Mapping, Optional
from urllib.parse import quote

import requests

from feldera.enums import BootstrapPolicy, PipelineFieldSelector, PipelineStatus
from feldera.rest._helpers import determine_client_version
from feldera.rest._httprequests import HttpRequests
from feldera.rest.config import Config
from feldera.rest.errors import FelderaAPIError, FelderaTimeoutError
from feldera.rest.feldera_config import FelderaConfig
from feldera.rest.pipeline import Pipeline


def _validate_no_none_keys_in_map(data):
    def validate_no_none_keys(d: Dict[Any, Any]) -> None:
        for k, v in d.items():
            if isinstance(v, dict) and any(k is None for k in v.keys()):
                raise ValueError("keys of SQL MAP objects cannot be NULL")

    if isinstance(data, list):
        for datum in data:
            validate_no_none_keys(datum)
    elif isinstance(data, dict):
        validate_no_none_keys(data)
    else:
        return


def _prepare_boolean_input(value: bool) -> str:
    return "true" if value else "false"


class FelderaClient:
    """
    A client for the Feldera HTTP API.

    The client is initialized with the configuration needed for interacting with the
    Feldera HTTP API, which it uses in its calls. Its methods are implemented
    by issuing one or more HTTP requests to the API, and as such can provide higher
    level operations (e.g., support waiting for the success of asynchronous HTTP API
    functionality).
    """

    def __init__(
        self,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: Optional[float] = None,
        connection_timeout: Optional[float] = None,
        requests_verify: Optional[bool | str] = None,
    ) -> None:
        """
        Constructs a Feldera client.

        :param url: (Optional) URL to the Feldera API.
            The default is read from the `FELDERA_HOST` environment variable;
            if the variable is not set, the default is `"http://localhost:8080"`.
        :param api_key: (Optional) API key to access Feldera (format: `"apikey:..."`).
            The default is read from the `FELDERA_API_KEY` environment variable;
            if the variable is not set, the default is `None` (no API key is provided).
        :param timeout: (Optional) Duration in seconds that the client will wait to receive
            a response of an HTTP request, after which it times out.
            The default is `None` (wait indefinitely; no timeout is enforced).
        :param connection_timeout: (Optional) Duration in seconds that the client will wait
            to establish the connection of an HTTP request, after which it times out.
            The default is `None` (wait indefinitely; no timeout is enforced).
        :param requests_verify: (Optional) The `verify` parameter passed to the `requests` library,
            which is used internally to perform HTTP requests.
            See also: https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification .
            The default is based on the `FELDERA_HTTPS_TLS_CERT` or the `FELDERA_TLS_INSECURE` environment variable.
            By setting `FELDERA_HTTPS_TLS_CERT` to a path, the default becomes the CA bundle it points to.
            By setting `FELDERA_TLS_INSECURE` to `"1"`, `"true"` or `"yes"` (all case-insensitive), the default becomes
            `False` which means to disable TLS verification by default. If both variables are set, the former takes
            priority over the latter. If neither variable is set, the default is `True`.
        """

        self.config = Config(
            url=url,
            api_key=api_key,
            timeout=timeout,
            connection_timeout=connection_timeout,
            requests_verify=requests_verify,
        )
        self.http = HttpRequests(self.config)

        try:
            client_version = determine_client_version()
            server_config = self.get_config()
            if client_version != server_config.version:
                logging.warning(
                    f"Feldera client is on version {client_version} while server is at "
                    f"{server_config.version}. There could be incompatibilities."
                )
        except Exception as e:
            logging.error(f"Failed to connect to Feldera API: {e}")
            raise e

    @staticmethod
    def localhost(port: int = 8080) -> "FelderaClient":
        """
        Create a FelderaClient that connects to the local Feldera instance
        """

        return FelderaClient(f"http://127.0.0.1:{port}")

    def get_pipeline(
        self, pipeline_name: str, field_selector: PipelineFieldSelector
    ) -> Pipeline:
        """
        Get a pipeline by name

        :param pipeline_name: The name of the pipeline
        :param field_selector: Choose what pipeline information to refresh; see PipelineFieldSelector enum definition.
        """

        resp = self.http.get(
            f"/pipelines/{pipeline_name}?selector={field_selector.value}"
        )

        return Pipeline.from_dict(resp)

    def get_runtime_config(self, pipeline_name) -> Mapping[str, Any]:
        """
        Get the runtime config of a pipeline by name

        :param pipeline_name: The name of the pipeline
        """

        resp: Mapping[str, Any] = self.http.get(f"/pipelines/{pipeline_name}")

        runtime_config: Mapping[str, Any] | None = resp.get("runtime_config")
        if runtime_config is None:
            raise ValueError(f"Pipeline {pipeline_name} has no runtime config")

        return runtime_config

    def pipelines(
        self, selector: PipelineFieldSelector = PipelineFieldSelector.STATUS
    ) -> list[Pipeline]:
        """
        Get all pipelines
        """

        resp = self.http.get(
            path=f"/pipelines?selector={selector.value}",
        )

        return [Pipeline.from_dict(pipeline) for pipeline in resp]

    def __wait_for_compilation(self, name: str):
        wait = ["Pending", "CompilingSql", "SqlCompiled", "CompilingRust"]

        while True:
            p = self.get_pipeline(name, PipelineFieldSelector.STATUS)
            status = p.program_status

            if status == "Success":
                return self.get_pipeline(name, PipelineFieldSelector.ALL)
            elif status not in wait:
                p = self.get_pipeline(name, PipelineFieldSelector.ALL)

                # error handling for SQL compilation errors
                if status == "SqlError":
                    sql_errors = p.program_error["sql_compilation"]["messages"]
                    if sql_errors:
                        err_msg = f"Pipeline {name} failed to compile:\n"
                        for sql_error in sql_errors:
                            err_msg += (
                                f"{sql_error['error_type']}\n{sql_error['message']}\n"
                            )
                            err_msg += f"Code snippet:\n{sql_error['snippet']}"
                        raise RuntimeError(err_msg)

                error_message = f"The program failed to compile: {status}\n"

                rust_error = p.program_error.get("rust_compilation")
                if rust_error is not None:
                    error_message += f"Rust Error: {rust_error}\n"

                system_error = p.program_error.get("system_error")
                if system_error is not None:
                    error_message += f"System Error: {system_error}"

                raise RuntimeError(error_message)

            logging.debug("still compiling %s, waiting for 100 more milliseconds", name)
            time.sleep(0.1)

    def __wait_for_pipeline_state(
        self,
        pipeline_name: str,
        state: str,
        timeout_s: Optional[float] = None,
        start: bool = True,
    ):
        start_time = time.monotonic()

        while True:
            if timeout_s is not None:
                elapsed = time.monotonic() - start_time
                if elapsed > timeout_s:
                    raise TimeoutError(
                        f"Timed out waiting for pipeline {pipeline_name} to "
                        f"transition to '{state}' state"
                    )

            resp = self.get_pipeline(pipeline_name, PipelineFieldSelector.STATUS)
            status = resp.deployment_status

            if status.lower() == state.lower():
                break
            elif (
                status == "Stopped"
                and len(resp.deployment_error or {}) > 0
                and resp.deployment_desired_status == "Stopped"
            ):
                err_msg = "Unable to START the pipeline:\n" if start else ""
                raise RuntimeError(
                    f"""{err_msg}Unable to transition the pipeline to '{state}'.
Reason: The pipeline is in a STOPPED state due to the following error:
{resp.deployment_error.get("message", "")}"""
                )

            logging.debug(
                "still starting %s, waiting for 100 more milliseconds", pipeline_name
            )
            time.sleep(0.1)

    def __wait_for_pipeline_state_one_of(
        self,
        pipeline_name: str,
        states: list[str],
        timeout_s: float | None = None,
        start: bool = True,
    ) -> PipelineStatus:
        start_time = time.monotonic()
        states = [state.lower() for state in states]

        while True:
            if timeout_s is not None:
                elapsed = time.monotonic() - start_time
                if elapsed > timeout_s:
                    raise TimeoutError(
                        f"Timed out waiting for pipeline {pipeline_name} to"
                        f"transition to one of the states: {states}"
                    )

            resp = self.get_pipeline(pipeline_name, PipelineFieldSelector.STATUS)
            status = resp.deployment_status

            if status.lower() in states:
                return PipelineStatus.from_str(status)
            elif (
                status == "Stopped"
                and len(resp.deployment_error or {}) > 0
                and resp.deployment_desired_status == "Stopped"
            ):
                err_msg = "Unable to START the pipeline:\n" if start else ""
                raise RuntimeError(
                    f"""{err_msg}Unable to transition the pipeline to one of the states: {states}.
Reason: The pipeline is in a STOPPED state due to the following error:
{resp.deployment_error.get("message", "")}"""
                )
            logging.debug(
                "still starting %s, waiting for 100 more milliseconds", pipeline_name
            )
            time.sleep(0.1)

    def create_pipeline(self, pipeline: Pipeline, wait: bool = True) -> Pipeline:
        """
        Create a pipeline if it doesn't exist and wait for it to compile

        :param pipeline: The pipeline to create
        :param wait: Whether to wait for the pipeline to compile. True by default
        """

        body = {
            "name": pipeline.name,
            "program_code": pipeline.program_code,
            "udf_rust": pipeline.udf_rust,
            "udf_toml": pipeline.udf_toml,
            "program_config": pipeline.program_config,
            "runtime_config": pipeline.runtime_config,
            "description": pipeline.description or "",
        }

        self.http.post(
            path="/pipelines",
            body=body,
        )

        if not wait:
            return pipeline

        return self.__wait_for_compilation(pipeline.name)

    def create_or_update_pipeline(
        self, pipeline: Pipeline, wait: bool = True
    ) -> Pipeline:
        """
        Create a pipeline if it doesn't exist or update a pipeline and wait for
        it to compile

        :param pipeline: The pipeline to create or update
        :param wait: Whether to wait for the pipeline to compile. True by default
        :return: The created or updated pipeline
        """

        body = {
            "name": pipeline.name,
            "program_code": pipeline.program_code,
            "udf_rust": pipeline.udf_rust,
            "udf_toml": pipeline.udf_toml,
            "program_config": pipeline.program_config,
            "runtime_config": pipeline.runtime_config,
            "description": pipeline.description or "",
        }

        self.http.put(
            path=f"/pipelines/{pipeline.name}",
            body=body,
        )

        if not wait:
            return pipeline

        return self.__wait_for_compilation(pipeline.name)

    def patch_pipeline(
        self,
        name: str,
        sql: Optional[str] = None,
        udf_rust: Optional[str] = None,
        udf_toml: Optional[str] = None,
        program_config: Optional[Mapping[str, Any]] = None,
        runtime_config: Optional[Mapping[str, Any]] = None,
        description: Optional[str] = None,
    ):
        """
        Incrementally update pipeline

        :param name: The name of the pipeline
        """

        self.http.patch(
            path=f"/pipelines/{name}",
            body={
                "program_code": sql,
                "udf_rust": udf_rust,
                "udf_toml": udf_toml,
                "program_config": program_config,
                "runtime_config": runtime_config,
                "description": description,
            },
        )

    def testing_force_update_platform_version(self, name: str, platform_version: str):
        self.http.post(
            path=f"/pipelines/{name}/testing",
            params={"set_platform_version": platform_version},
        )

    def update_pipeline_runtime(self, name: str):
        """
        Recompile a pipeline with the Feldera runtime version included in the currently installed Feldera platform.

        :param name: The name of the pipeline
        """

        self.http.post(path=f"/pipelines/{name}/update_runtime")

    def delete_pipeline(self, name: str):
        """
        Deletes a pipeline by name

        :param name: The name of the pipeline
        """
        self.http.delete(
            path=f"/pipelines/{name}",
        )

    def get_pipeline_stats(self, name: str) -> dict:
        """
        Get the pipeline metrics and performance counters

        :param name: The name of the pipeline
        """

        resp = self.http.get(
            path=f"/pipelines/{name}/stats",
        )

        return resp

    def get_pipeline_logs(self, pipeline_name: str) -> Generator[str, None, None]:
        """
        Get the pipeline logs

        :param name: The name of the pipeline
        :return: A generator yielding the logs, one line at a time.
        """
        chunk: bytes
        with self.http.get(
            path=f"/pipelines/{pipeline_name}/logs",
            stream=True,
        ) as resp:
            for chunk in resp.iter_lines(chunk_size=50000000):
                if chunk:
                    yield chunk.decode("utf-8")

    def activate_pipeline(
        self, pipeline_name: str, wait: bool = True, timeout_s: Optional[float] = None
    ) -> Optional[PipelineStatus]:
        """

        :param pipeline_name: The name of the pipeline to activate
        :param wait: Set True to wait for the pipeline to activate. True by
            default
        :param timeout_s: The amount of time in seconds to wait for the
            pipeline to activate.
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/activate",
        )

        if not wait:
            return None

        return self.__wait_for_pipeline_state_one_of(
            pipeline_name, ["running", "AwaitingApproval"], timeout_s
        )

    def _inner_start_pipeline(
        self,
        pipeline_name: str,
        initial: str = "running",
        bootstrap_policy: Optional[BootstrapPolicy] = None,
        wait: bool = True,
        timeout_s: Optional[float] = None,
    ) -> Optional[PipelineStatus]:
        """

        :param pipeline_name: The name of the pipeline to start
        :param initial: The initial state to start the pipeline in. "running"
            by default.
        :param wait: Set True to wait for the pipeline to start. True by default
        :param timeout_s: The amount of time in seconds to wait for the
            pipeline to start.
        """

        params = {"initial": initial}
        if bootstrap_policy is not None:
            params["bootstrap_policy"] = bootstrap_policy.value

        self.http.post(
            path=f"/pipelines/{pipeline_name}/start",
            params=params,
        )

        if not wait:
            return None

        return self.__wait_for_pipeline_state_one_of(
            pipeline_name, [initial, "AwaitingApproval"], timeout_s
        )

    def start_pipeline(
        self,
        pipeline_name: str,
        bootstrap_policy: Optional[BootstrapPolicy] = None,
        wait: bool = True,
        timeout_s: Optional[float] = None,
    ) -> Optional[PipelineStatus]:
        """

        :param pipeline_name: The name of the pipeline to start
        :param wait: Set True to wait for the pipeline to start.
            True by default
        :param timeout_s: The amount of time in seconds to wait for the
            pipeline to start.
        """

        return self._inner_start_pipeline(
            pipeline_name, "running", bootstrap_policy, wait, timeout_s
        )

    def start_pipeline_as_paused(
        self,
        pipeline_name: str,
        bootstrap_policy: Optional[BootstrapPolicy] = None,
        wait: bool = True,
        timeout_s: float | None = None,
    ) -> Optional[PipelineStatus]:
        """
        :param pipeline_name: The name of the pipeline to start as paused.
        :param wait: Set True to wait for the pipeline to start as pause.
            True by default
        :param timeout_s: The amount of time in seconds to wait for the
            pipeline to start.
        """

        return self._inner_start_pipeline(
            pipeline_name, "paused", bootstrap_policy, wait, timeout_s
        )

    def start_pipeline_as_standby(
        self,
        pipeline_name: str,
        bootstrap_policy: Optional[BootstrapPolicy] = None,
        wait: bool = True,
        timeout_s: Optional[float] = None,
    ):
        """
        :param pipeline_name: The name of the pipeline to start as standby.
        :param wait: Set True to wait for the pipeline to start as standby.
            True by default
        :param timeout_s: The amount of time in seconds to wait for the
            pipeline to start.
        """

        self._inner_start_pipeline(
            pipeline_name, "standby", bootstrap_policy, wait, timeout_s
        )

    def resume_pipeline(
        self,
        pipeline_name: str,
        wait: bool = True,
        timeout_s: Optional[float] = None,
    ):
        """
        Resume a pipeline

        :param pipeline_name: The name of the pipeline to stop
        :param wait: Set True to wait for the pipeline to pause. True by default
        :param timeout_s: The amount of time in seconds to wait for the pipeline
            to pause.
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/resume",
        )

        if not wait:
            return

        self.__wait_for_pipeline_state(pipeline_name, "running", timeout_s)

    def pause_pipeline(
        self,
        pipeline_name: str,
        wait: bool = True,
        timeout_s: Optional[float] = None,
    ):
        """
        Pause a pipeline

        :param pipeline_name: The name of the pipeline to stop
        :param error_message: The error message to show if the pipeline is in
             STOPPED state due to a failure.
        :param wait: Set True to wait for the pipeline to pause. True by default
        :param timeout_s: The amount of time in seconds to wait for the pipeline
            to pause.
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/pause",
        )

        if not wait:
            return

        self.__wait_for_pipeline_state(pipeline_name, "paused", timeout_s)

    def approve_pipeline(
        self,
        pipeline_name: str,
    ):
        self.http.post(
            path=f"/pipelines/{pipeline_name}/approve",
        )

    def stop_pipeline(
        self,
        pipeline_name: str,
        force: bool,
        wait: bool = True,
        timeout_s: Optional[float] = None,
    ):
        """
        Stop a pipeline

        :param pipeline_name: The name of the pipeline to stop
        :param force: Set True to immediately scale compute resources to zero.
            Set False to automatically checkpoint before stopping.
        :param wait: Set True to wait for the pipeline to stop. True by default
        :param timeout_s: The amount of time in seconds to wait for the pipeline
            to stop.
        """

        params = {"force": str(force).lower()}

        self.http.post(
            path=f"/pipelines/{pipeline_name}/stop",
            params=params,
        )

        if not wait:
            return

        start = time.monotonic()

        while True:
            if timeout_s is not None and time.monotonic() - start > timeout_s:
                raise FelderaTimeoutError(
                    f"timeout error: pipeline '{pipeline_name}' did not stop in {timeout_s} seconds"
                )

            status = self.get_pipeline(
                pipeline_name, PipelineFieldSelector.STATUS
            ).deployment_status

            if status == "Stopped":
                return

            logging.debug(
                "still stopping %s, waiting for 100 more milliseconds",
                pipeline_name,
            )
            time.sleep(0.1)

    def clear_storage(self, pipeline_name: str, timeout_s: Optional[float] = None):
        """
        Clears the storage from the pipeline.
        This operation cannot be canceled.

        :param pipeline_name: The name of the pipeline
        :param timeout_s: The amount of time in seconds to wait for the storage
            to clear.
        """
        self.http.post(
            path=f"/pipelines/{pipeline_name}/clear",
        )

        start = time.monotonic()

        while True:
            if timeout_s is not None and time.monotonic() - start > timeout_s:
                raise FelderaTimeoutError(
                    f"timeout error: pipeline '{pipeline_name}' did not clear storage in {timeout_s} seconds"
                )
            status = self.get_pipeline(
                pipeline_name, PipelineFieldSelector.STATUS
            ).storage_status

            if status == "Cleared":
                return

            logging.debug(
                "still clearing %s, waiting for 100 more milliseconds",
                pipeline_name,
            )
            time.sleep(0.1)

    def start_transaction(self, pipeline_name: str) -> int:
        """
        Start a new transaction.

            Transaction ID.

        :param pipeline_name: The name of the pipeline.
        """

        resp = self.http.post(
            path=f"/pipelines/{pipeline_name}/start_transaction",
        )

        return int(resp.get("transaction_id"))

    def commit_transaction(
        self,
        pipeline_name: str,
        transaction_id: Optional[int] = None,
        wait: bool = True,
        timeout_s: Optional[float] = None,
    ):
        """
        Commits the currently active transaction.

        :param pipeline_name: The name of the pipeline.

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

        # TODO: implement this without using /stats when we have a better pipeline status reporting API.
        stats = self.get_pipeline_stats(pipeline_name)
        current_transaction_id = stats["global_metrics"]["transaction_id"]

        if current_transaction_id == 0:
            raise RuntimeError(
                "Attempting to commit a transaction, but there is no transaction in progress"
            )

        if transaction_id and current_transaction_id != transaction_id:
            raise ValueError(
                f"Specified transaction id {transaction_id} doesn't match current active transaction id {current_transaction_id}"
            )

        transaction_id = current_transaction_id

        self.http.post(
            path=f"/pipelines/{pipeline_name}/commit_transaction",
        )

        start_time = time.monotonic()

        if not wait:
            return

        while True:
            if timeout_s is not None:
                elapsed = time.monotonic() - start_time
                if elapsed > timeout_s:
                    raise TimeoutError("Timed out waiting for transaction to commit")

            stats = self.get_pipeline_stats(pipeline_name)
            if stats["global_metrics"]["transaction_id"] != transaction_id:
                return

            logging.debug("commit hasn't completed, waiting for 1 more second")
            time.sleep(1.0)

    def checkpoint_pipeline(self, pipeline_name: str) -> int:
        """
        Checkpoint a pipeline.

        :param pipeline_name: The name of the pipeline to checkpoint
        """

        resp = self.http.post(
            path=f"/pipelines/{pipeline_name}/checkpoint",
        )

        return int(resp.get("checkpoint_sequence_number"))

    def checkpoint_pipeline_status(self, pipeline_name: str) -> dict:
        """
        Gets the checkpoint status

        :param pipeline_name: The name of the pipeline to check the checkpoint status of.
        """

        return self.http.get(path=f"/pipelines/{pipeline_name}/checkpoint_status")

    def sync_checkpoint(self, pipeline_name: str) -> str:
        """
        Triggers a checkpoint synchronization for the specified pipeline.
        Check the status by calling `pipeline_sync_checkpoint_status`.

        :param pipeline_name: Name of the pipeline whose checkpoint should be synchronized.
        """

        resp = self.http.post(
            path=f"/pipelines/{pipeline_name}/checkpoint/sync",
        )

        return resp.get("checkpoint_uuid")

    def sync_checkpoint_status(self, pipeline_name: str) -> dict:
        """
        Gets the checkpoint sync status of the pipeline

        :param pipeline_name: The name of the pipeline to check the checkpoint synchronization status of.
        """

        return self.http.get(
            path=f"/pipelines/{pipeline_name}/checkpoint/sync_status",
        )

    def push_to_pipeline(
        self,
        pipeline_name: str,
        table_name: str,
        format: str,
        data: list[list | str | dict] | dict | str,
        array: bool = False,
        force: bool = False,
        update_format: str = "raw",
        json_flavor: Optional[str] = None,
        serialize: bool = True,
        wait: bool = True,
        wait_timeout_s: Optional[float] = None,
    ) -> str:
        """
        Insert data into a pipeline

        :param pipeline_name: The name of the pipeline
        :param table_name: The name of the table
        :param format: The format of the data, either "json" or "csv"
        :param array: True if updates in this stream are packed into JSON arrays, used in conjunction with the
            "json" format

        :param force: If True, the data will be inserted even if the pipeline is paused
        :param update_format: JSON data change event format, used in conjunction with the "json" format,
            the default value is "insert_delete", other supported formats: "weighted", "debezium", "snowflake", "raw"
        :param json_flavor: JSON encoding used for individual table records, the default value is "default", other supported encodings:
            "debezium_mysql", "snowflake", "kafka_connect_json_converter", "pandas"
        :param data: The data to insert
        :param serialize: If True, the data will be serialized to JSON. True by default
        :param wait: If True, blocks until this input has been processed by the pipeline
        :param wait_timeout_s: The timeout in seconds to wait for this set of
            inputs to be processed by the pipeline. None by default

        :returns: The completion token to this input.
        """

        if format not in ["json", "csv"]:
            raise ValueError("format must be either 'json' or 'csv'")

        if update_format not in [
            "insert_delete",
            "weighted",
            "debezium",
            "snowflake",
            "raw",
        ]:
            raise ValueError(
                "update_format must be one of 'insert_delete', 'weighted', 'debezium', 'snowflake', 'raw'"
            )

        if json_flavor is not None and json_flavor not in [
            "default",
            "debezium_mysql",
            "snowflake",
            "kafka_connect_json_converter",
            "pandas",
        ]:
            raise ValueError(
                "json_flavor must be one of 'default', 'debezium_mysql', 'snowflake', 'kafka_connect_json_converter', 'pandas'"
            )

        if update_format == "insert_delete":
            if array:
                for datum in data:
                    _validate_no_none_keys_in_map(datum.get("insert", {}))
                    _validate_no_none_keys_in_map(datum.get("delete", {}))
            else:
                data: Mapping[str, Any] = data
                _validate_no_none_keys_in_map(data.get("insert", {}))
                _validate_no_none_keys_in_map(data.get("delete", {}))
        else:
            _validate_no_none_keys_in_map(data)

        # python sends `True` which isn't accepted by the backend
        array: str = _prepare_boolean_input(array)
        force: str = _prepare_boolean_input(force)

        params = {
            "force": force,
            "format": format,
        }

        if format == "json":
            params["array"] = array
            params["update_format"] = update_format

        if json_flavor is not None:
            params["json_flavor"] = json_flavor

        content_type = "application/json"

        if format == "csv":
            content_type = "text/csv"
            data = bytes(str(data), "utf-8")

        resp = self.http.post(
            path=f"/pipelines/{quote(pipeline_name, safe='')}/ingress/{quote(table_name, safe='')}",
            params=params,
            content_type=content_type,
            body=data,
            serialize=serialize,
        )

        token = resp.get("token")
        if token is None:
            raise FelderaAPIError("response did not contain a completion token", resp)

        if not wait:
            return token

        self.wait_for_token(pipeline_name, token, timeout_s=wait_timeout_s)

        return token

    def completion_token_processed(self, pipeline_name: str, token: str) -> bool:
        """
        Check whether the pipeline has finished processing all inputs received from the connector before
        the token was generated.

        :param pipeline_name: The name of the pipeline
        :param token: The token to check for completion
        :return: True if the pipeline has finished processing all inputs represented by the token, False otherwise
        """

        params = {
            "token": token,
        }

        resp = self.http.get(
            path=f"/pipelines/{quote(pipeline_name, safe='')}/completion_status",
            params=params,
        )

        status: Optional[str] = resp.get("status")

        if status is None:
            raise FelderaAPIError(
                f"got empty status when checking for completion status for token: {token}",
                resp,
            )

        return status.lower() == "complete"

    def wait_for_token(
        self, pipeline_name: str, token: str, timeout_s: Optional[float] = None
    ):
        """
        Blocks until all records represented by this completion token have
        been processed.

        :param pipeline_name: The name of the pipeline
        :param token: The token to check for completion
        :param timeout_s: The amount of time in seconds to wait for the pipeline
            to process these records.
        """

        start = time.monotonic()
        end = start + timeout_s if timeout_s else None
        initial_backoff = 0.1
        max_backoff = 5
        exponent = 1.2
        retries = 0

        while True:
            if end:
                if time.monotonic() > end:
                    raise FelderaTimeoutError(
                        f"timeout error: pipeline '{pipeline_name}' did not"
                        + f" process records represented by token {token} within"
                        + f" {timeout_s}"
                    )

            if self.completion_token_processed(pipeline_name, token):
                break

            elapsed = time.monotonic() - start
            logging.debug(
                f"still waiting for inputs represented by {token} to be processed; elapsed: {elapsed}s"
            )

            retries += 1
            backoff = min(max_backoff, initial_backoff * (exponent**retries))

            time.sleep(backoff)

    def listen_to_pipeline(
        self,
        pipeline_name: str,
        table_name: str,
        format: str,
        backpressure: bool = True,
        array: bool = False,
        timeout: Optional[float] = None,
        case_sensitive: bool = False,
    ):
        """
        Listen for updates to views for pipeline, yields the chunks of data

        :param pipeline_name: The name of the pipeline
        :param table_name: The name of the table to listen to
        :param format: The format of the data, either "json" or "csv"
        :param backpressure: When the flag is True (the default), this method waits for the consumer to receive each
            chunk and blocks the pipeline if the consumer cannot keep up. When this flag is False, the pipeline drops
            data chunks if the consumer is not keeping up with its output. This prevents a slow consumer from slowing
            down the entire pipeline.
        :param array: Set True to group updates in this stream into JSON arrays, used in conjunction with the
            "json" format, the default value is False

        :param timeout: The amount of time in seconds to listen to the stream for
        :param case_sensitive: True if the table name is case sensitive or a reserved keyword, False by default
        """

        params = {
            "format": format,
            "backpressure": _prepare_boolean_input(backpressure),
        }

        if format == "json":
            params["array"] = _prepare_boolean_input(array)

        table_name = f'"{table_name}"' if case_sensitive else table_name

        resp: requests.Response = self.http.post(
            path=f"/pipelines/{quote(pipeline_name, safe='')}/egress/{quote(table_name, safe='')}",
            params=params,
            stream=True,
        )

        end = time.monotonic() + timeout if timeout else None

        def generator():
            # Using the default chunk size below makes `iter_lines` extremely
            # inefficient when dealing with long lines.
            for chunk in resp.iter_lines(chunk_size=50000000):
                if end and time.monotonic() > end:
                    break
                if chunk:
                    yield json.loads(chunk, parse_float=Decimal)

        return generator

    def query_as_text(
        self, pipeline_name: str, query: str
    ) -> Generator[str, None, None]:
        """
        Executes an ad-hoc query on the specified pipeline and returns a generator that yields lines of the table.

        :param pipeline_name: The name of the pipeline to query.
        :param query: The SQL query to be executed.
        :return: A generator yielding the query result in tabular format, one line at a time.
        """
        params = {
            "pipeline_name": pipeline_name,
            "sql": query,
            "format": "text",
        }

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/query",
            params=params,
            stream=True,
        )

        chunk: bytes
        for chunk in resp.iter_lines(chunk_size=50000000):
            if chunk:
                yield chunk.decode("utf-8")

    def query_as_hash(self, pipeline_name: str, query: str) -> str:
        """
        Executes an ad-hoc query on the specified pipeline and returns a hash of the result.

        :param pipeline_name: The name of the pipeline to query.
        :param query: The SQL query to be executed.
        :return: A string containing the hash of the query result.
        """
        params = {
            "pipeline_name": pipeline_name,
            "sql": query,
            "format": "hash",
        }

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/query",
            params=params,
            stream=False,
        )
        return resp

    def query_as_parquet(self, pipeline_name: str, query: str, path: str):
        """
        Executes an ad-hoc query on the specified pipeline and saves the result to a parquet file.
        If the extension isn't `parquet`, it will be automatically appended to `path`.

        :param pipeline_name: The name of the pipeline to query.
        :param query: The SQL query to be executed.
        :param path: The path including the file name to save the resulting parquet file in.
        """

        params = {
            "pipeline_name": pipeline_name,
            "sql": query,
            "format": "parquet",
        }

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/query",
            params=params,
            stream=True,
        )

        path: pathlib.Path = pathlib.Path(path)

        ext = ".parquet"
        if path.suffix != ext:
            path = path.with_suffix(ext)

        file = open(path, "wb")

        chunk: bytes
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)
        file.close()

    def query_as_json(
        self, pipeline_name: str, query: str
    ) -> Generator[Mapping[str, Any], None, None]:
        """
        Executes an ad-hoc query on the specified pipeline and returns the result as a generator that yields
        rows of the query as Python dictionaries.
        All floating-point numbers are deserialized as Decimal objects to avoid precision loss.

        :param pipeline_name: The name of the pipeline to query.
        :param query: The SQL query to be executed.
        :return: A generator that yields each row of the result as a Python dictionary, deserialized from JSON.
        """
        params = {
            "pipeline_name": pipeline_name,
            "sql": query,
            "format": "json",
        }

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/query",
            params=params,
            stream=True,
        )

        for chunk in resp.iter_lines(chunk_size=1024):
            if chunk:
                yield json.loads(chunk, parse_float=Decimal)

    def pause_connector(self, pipeline_name, table_name, connector_name):
        """
        Pause the specified input connector.

        Connectors allow feldera to fetch data from a source or write data to a sink.
        This method allows users to **PAUSE** a specific **INPUT** connector.
        All connectors are RUNNING by default.

        Refer to the connector documentation for more information:
        <https://docs.feldera.com/connectors/#input-connector-orchestration>

        :param pipeline_name: The name of the pipeline.
        :param table_name: The name of the table associated with this connector.
        :param connector_name: The name of the connector.

        :raises FelderaAPIError: If the connector cannot be found, or if the pipeline is not running.
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/pause",
        )

    def resume_connector(
        self, pipeline_name: str, table_name: str, connector_name: str
    ):
        """
        Resume the specified connector.

        Connectors allow feldera to fetch data from a source or write data to a sink.
        This method allows users to **RESUME / START** a specific **INPUT** connector.
        All connectors are RUNNING by default.

        Refer to the connector documentation for more information:
        <https://docs.feldera.com/connectors/#input-connector-orchestration>

        :param pipeline_name: The name of the pipeline.
        :param table_name: The name of the table associated with this connector.
        :param connector_name: The name of the connector.

        :raises FelderaAPIError: If the connector cannot be found, or if the pipeline is not running.
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/start",
        )

    def get_config(self) -> FelderaConfig:
        """
        Retrieves the general Feldera server configuration.
        """

        resp = self.http.get(path="/config")

        return FelderaConfig(resp)

    def get_pipeline_support_bundle(
        self, pipeline_name: str, params: Optional[Dict[str, Any]] = None
    ) -> bytes:
        """
        Generate a support bundle containing diagnostic information from a pipeline.

        This endpoint collects various diagnostic data from the pipeline including
        circuit profile, heap profile, metrics, logs, stats, and connector statistics,
        and packages them into a single ZIP file for support purposes.

        :param pipeline_name: The name of the pipeline
        :param params: Optional query parameters to control data collection
        :return: The support bundle as bytes (ZIP file)
        :raises FelderaAPIError: If the pipeline does not exist or if there's an error
        """

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/support_bundle",
            params=params,
            stream=True,
        )

        buffer = b""
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                buffer += chunk

        return buffer

    def start_samply_profile(self, pipeline_name: str, duration: int):
        """
        Start the profiling this pipeline with samply.

        :param pipeline_name: The name of the pipeline
        :param duration: The duration of the profile in seconds (default: 30)
        :raises FelderaAPIError: If the pipeline does not exist or if there's an error
        """

        if duration <= 0:
            raise ValueError("Duration must be a positive integer")

        params = {"duration_secs": duration}

        self.http.post(
            path=f"/pipelines/{pipeline_name}/samply_profile",
            params=params,
        )

    def get_samply_profile(self, pipeline_name: str) -> bytes:
        """
        Get the most recent available samply profile for the given pipeline.

        :param pipeline_name: The name of the pipeline
        :return: The samply profile as bytes (GZIP file)
        :raises FelderaAPIError: If the pipeline does not exist or if there's an error
        """

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/samply_profile",
            stream=True,
        )

        buffer = b""
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                buffer += chunk

        return buffer

    def generate_completion_token(
        self, pipeline_name: str, table_name: str, connector_name: str
    ) -> str:
        """
        Generate a completion token that can be passed to :meth:`.FelderaClient.completion_token_processed` to
        check whether the pipeline has finished processing all inputs received from the connector before
        the token was generated.

        :param pipeline_name: The name of the pipeline
        :param table_name: The name of the table associated with this connector.
        :param connector_name: The name of the connector.

        :raises FelderaAPIError: If the connector cannot be found, or if the pipeline is not running.
        """

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/completion_token",
        )

        token: str | None = resp.get("token")

        if token is None:
            raise ValueError(
                "got invalid response from feldera when generating completion token"
            )

        return token

    def get_cluster_events(self) -> list[dict]:
        """
        Retrieves all cluster events (status fields only).

        :returns: List of cluster events.
        """
        return self.http.get(path="/cluster/events")

    def get_cluster_event(self, event_id: str, selector: str = "status") -> dict:
        """
        Retrieves a specific cluster event.

        :param event_id: Identifier (UUID) of the event to retrieve, or `latest` for the latest event.
        :param selector: (Optional) Limit the returned fields. Valid values: "all", "status" (default).

        :returns: Event (fields limited based on selector).
        """
        # Selector of fields
        if selector not in ["all", "status"]:
            raise ValueError(f"invalid selector: {selector}")

        # Issue request and return response
        return self.http.get(path=f"/cluster/events/{event_id}?selector={selector}")
