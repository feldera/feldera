import os
import pathlib
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from urllib.parse import urlparse

from deltalake import DeltaTable


MINIO_BUCKET = os.environ.get("CI_MINIO_BUCKET", "ci-tests")
MINIO_ENDPOINT = os.environ.get(
    "CI_MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000"
)
MINIO_REGION = os.environ.get("CI_MINIO_REGION", "us-east-1")
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "ci-kafka-bootstrap.kafka:9092"
)


def env_truthy(name: str) -> bool:
    """Return True when an environment variable is set to a truthy value."""
    value = os.environ.get(name)
    return value is not None and value.lower() not in {"", "0", "false", "no"}


def required_env(name: str) -> str:
    """Return a required environment variable or raise a descriptive error."""
    value = os.environ.get(name)
    if value:
        return value
    raise RuntimeError(f"required environment variable '{name}' is not set")


def runs_in_ci() -> bool:
    """Return True when the test suite is running under CI."""
    return env_truthy("CI")


@dataclass
class DeltaTestLocation:
    """Describe where the Delta sink writes test data and how to read it back."""

    uri: str
    connector_config: dict[str, object]
    root_path: str
    local_dir: pathlib.Path | None = None

    @classmethod
    def create(cls, pipeline_name: str) -> "DeltaTestLocation":
        """Use the local filesystem for local runs and MinIO-backed S3 in CI."""
        if runs_in_ci():
            access_key = required_env("CI_K8S_MINIO_ACCESS_KEY_ID")
            secret_key = required_env("CI_K8S_MINIO_SECRET_ACCESS_KEY")
            prefix = f"{pipeline_name}/{uuid.uuid4().hex}"
            root_path = f"{MINIO_BUCKET}/{prefix}"
            minio_endpoint = MINIO_ENDPOINT.rstrip("/")
            parsed_endpoint = urlparse(minio_endpoint)
            if (
                parsed_endpoint.scheme not in {"http", "https"}
                or not parsed_endpoint.netloc
            ):
                raise ValueError(
                    "CI_MINIO_ENDPOINT must be a full URL, e.g. "
                    "'http://minio.minio.svc.cluster.local:9000'"
                )

            return cls(
                uri=f"s3://{root_path}",
                connector_config={
                    "uri": f"s3://{root_path}",
                    "mode": "truncate",
                    "aws_access_key_id": access_key,
                    "aws_secret_access_key": secret_key,
                    "aws_region": MINIO_REGION,
                    "aws_endpoint": minio_endpoint,
                    "aws_allow_http": str(parsed_endpoint.scheme == "http").lower(),
                },
                root_path=root_path,
            )

        local_dir = pathlib.Path(
            tempfile.mkdtemp(prefix=f"{pipeline_name}_delta_", dir="/tmp")
        )
        return cls(
            uri=f"file://{local_dir}",
            connector_config={
                "uri": f"file://{local_dir}",
                "mode": "truncate",
            },
            root_path=str(local_dir),
            local_dir=local_dir,
        )

    def _delta_storage_options(self) -> dict[str, str]:
        """Return `deltalake` storage_options derived from the connector config."""
        return {
            k: str(v)
            for k, v in self.connector_config.items()
            if k not in ("uri", "mode")
        }

    def row_count(self) -> int:
        """Return the row count of the current Delta snapshot.

        Uses the per-file `numRecords` stats recorded in the delta log, so
        we never need to scan parquet (and never need pyarrow).
        """
        dt = DeltaTable(self.uri, storage_options=self._delta_storage_options())
        return dt.count()

    def cleanup(self) -> None:
        """Remove the local temp directory, if any.

        No-op on the CI/MinIO path: the bucket is ephemeral and the uuid
        prefix prevents collisions, so leaked keys are acceptable.
        """
        if self.local_dir is not None:
            shutil.rmtree(self.local_dir, ignore_errors=True)
            self.local_dir = None
