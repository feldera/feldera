"""Unit tests for FelderaClient.query_as_arrow_ipc and Pipeline.query_arrow."""

import builtins
import io
import sys
from unittest.mock import MagicMock

import pytest

from feldera.rest.feldera_client import FelderaClient


def _import_arrow_modules():
    pa = pytest.importorskip("pyarrow")
    ipc = pytest.importorskip("pyarrow.ipc")
    return pa, ipc


def _make_ipc_bytes(table) -> bytes:
    """Serialise a ``pyarrow.Table`` to Arrow IPC stream bytes."""
    _, ipc = _import_arrow_modules()
    buf = io.BytesIO()
    with ipc.new_stream(buf, table.schema) as writer:
        if table.num_rows > 0:
            writer.write_table(table)
    return buf.getvalue()


def _mock_response(ipc_bytes: bytes) -> MagicMock:
    """Return a mock response whose ``iter_content`` yields the IPC bytes in 1 KB chunks."""
    resp = MagicMock()
    chunk_size = 1024
    chunks = [
        ipc_bytes[i : i + chunk_size]
        for i in range(0, max(len(ipc_bytes), 1), chunk_size)
    ]
    resp.iter_content = MagicMock(return_value=iter(chunks))
    return resp


@pytest.fixture()
def client() -> FelderaClient:
    """A ``FelderaClient`` with a mocked HTTP layer (no real network calls)."""
    c = FelderaClient.__new__(FelderaClient)
    c.http = MagicMock()
    return c


class TestQueryAsArrowIpc:
    def test_non_empty_result_returns_correct_data(self, client: FelderaClient):
        pa, _ = _import_arrow_modules()
        schema = pa.schema([("id", pa.int64()), ("name", pa.utf8())])
        expected = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}, schema=schema)
        ipc_bytes = _make_ipc_bytes(expected)

        client.http.get.return_value = _mock_response(ipc_bytes)
        result = client.query_as_arrow_ipc("my_pipeline", "SELECT id, name FROM t")

        assert isinstance(result, pa.Table)
        assert result.schema == schema
        assert result.num_rows == 3
        assert result.column("id").to_pylist() == [1, 2, 3]
        assert result.column("name").to_pylist() == ["a", "b", "c"]

    def test_http_called_with_correct_params(self, client: FelderaClient):
        pa, _ = _import_arrow_modules()
        schema = pa.schema([("id", pa.int64())])
        table = pa.table({"id": [42]}, schema=schema)
        client.http.get.return_value = _mock_response(_make_ipc_bytes(table))

        client.query_as_arrow_ipc("my_pipeline", "SELECT id FROM t")

        client.http.get.assert_called_once_with(
            path="/pipelines/my_pipeline/query",
            params={
                "pipeline_name": "my_pipeline",
                "sql": "SELECT id FROM t",
                "format": "arrow_ipc",
            },
            stream=True,
        )

    def test_empty_result_preserves_schema(self, client: FelderaClient):
        pa, _ = _import_arrow_modules()
        schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
        empty = pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "value": pa.array([], type=pa.float64()),
            },
            schema=schema,
        )
        client.http.get.return_value = _mock_response(_make_ipc_bytes(empty))

        result = client.query_as_arrow_ipc(
            "my_pipeline", "SELECT id, value FROM t WHERE false"
        )

        assert isinstance(result, pa.Table)
        assert result.schema == schema
        assert result.num_rows == 0

    def test_missing_pyarrow_raises_helpful_import_error(self, client: FelderaClient, monkeypatch):
        real_import = builtins.__import__

        def _import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "pyarrow" or name.startswith("pyarrow."):
                raise ImportError("No module named 'pyarrow'")
            return real_import(name, globals, locals, fromlist, level)

        monkeypatch.delitem(sys.modules, "pyarrow", raising=False)
        monkeypatch.delitem(sys.modules, "pyarrow.ipc", raising=False)
        monkeypatch.setattr(builtins, "__import__", _import)

        with pytest.raises(ImportError, match="pip install feldera\\[arrow\\]"):
            client.query_as_arrow_ipc("my_pipeline", "SELECT 1")

        client.http.get.assert_not_called()


class TestPipelineQueryArrow:
    def test_query_arrow_delegates_to_client(self):
        """Pipeline.query_arrow must forward to client.query_as_arrow_ipc."""
        from feldera.pipeline import Pipeline

        pipeline = Pipeline.__new__(Pipeline)
        pipeline._inner = MagicMock()
        pipeline._inner.name = "pipe1"
        pipeline.client = MagicMock()

        expected = object()
        pipeline.client.query_as_arrow_ipc.return_value = expected

        result = pipeline.query_arrow("SELECT x FROM v")

        pipeline.client.query_as_arrow_ipc.assert_called_once_with("pipe1", "SELECT x FROM v")
        assert result is expected
