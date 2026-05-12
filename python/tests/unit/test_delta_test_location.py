"""Unit tests for DeltaTestLocation helper methods."""

import pytest
from tests import skip_on_arm64
from tests.utils import DeltaTestLocation


@skip_on_arm64
def test_read_table_missing_ok():
    """read_table with missing_ok=True returns None for non-existent table."""
    loc = DeltaTestLocation.create("test_read_table_missing", mode="snapshot")
    try:
        result = loc.read_table(missing_ok=True)
        assert result is None
    finally:
        loc.cleanup()


@skip_on_arm64
def test_read_table_raises_on_missing():
    """read_table raises TableNotFoundError when table doesn't exist."""
    from deltalake.exceptions import TableNotFoundError

    loc = DeltaTestLocation.create("test_read_table_raises", mode="snapshot")
    try:
        with pytest.raises(TableNotFoundError):
            loc.read_table(missing_ok=False)
    finally:
        loc.cleanup()


@skip_on_arm64
def test_log_files_missing_ok():
    """log_files with missing_ok=True returns empty list for non-existent table."""
    loc = DeltaTestLocation.create("test_log_files_missing", mode="snapshot")
    try:
        result = loc.log_files(missing_ok=True)
        assert result == []
    finally:
        loc.cleanup()


@skip_on_arm64
def test_log_files_raises_on_missing():
    """log_files raises TableNotFoundError when table doesn't exist."""
    from deltalake.exceptions import TableNotFoundError

    loc = DeltaTestLocation.create("test_log_files_raises", mode="snapshot")
    try:
        with pytest.raises(TableNotFoundError):
            loc.log_files(missing_ok=False)
    finally:
        loc.cleanup()


@skip_on_arm64
def test_read_table_returns_pyarrow():
    """read_table returns a pyarrow Table when table exists."""
    from deltalake import write_deltalake
    import pyarrow as pa

    loc = DeltaTestLocation.create("test_read_table_pyarrow", mode="snapshot")
    try:
        # Create a simple delta table
        data = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        write_deltalake(
            loc.uri, data, mode="overwrite", storage_options=loc.delta_storage_options()
        )

        # Read it back
        result = loc.read_table()
        assert result is not None
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
        assert "id" in result.column_names
        assert "value" in result.column_names
    finally:
        loc.cleanup()


@skip_on_arm64
def test_log_files_returns_file_list():
    """log_files returns a list of file URIs when table exists."""
    from deltalake import write_deltalake
    import pyarrow as pa

    loc = DeltaTestLocation.create("test_log_files_list", mode="snapshot")
    try:
        # Create a simple delta table
        data = pa.table({"id": [1, 2, 3]})
        write_deltalake(
            loc.uri, data, mode="overwrite", storage_options=loc.delta_storage_options()
        )

        # Get file list
        result = loc.log_files()
        assert isinstance(result, list)
        assert len(result) > 0
        # Each entry should be a file URI
        for file_uri in result:
            assert isinstance(file_uri, str)
            assert ".parquet" in file_uri.lower()
    finally:
        loc.cleanup()
