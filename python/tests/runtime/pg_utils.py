"""Shared utilities for PostgreSQL comparison tests."""

import os

DEFAULT_BATCH_SIZE = 5_000


def pg_audit_mode():
    """Return True when PG_TESTS_AUDIT is set (populate PG only, skip Feldera)."""
    return bool(os.environ.get("PG_TESTS_AUDIT"))


def pg_connect():
    """Connect to PostgreSQL using INTEGRATION_TESTS_POSTGRES_URI.

    Returns a psycopg2 connection (autocommit=True) or None if
    the env var is unset or psycopg2 is not installed.
    """
    pg_uri = os.environ.get("INTEGRATION_TESTS_POSTGRES_URI")
    if not pg_uri:
        return None
    try:
        import psycopg2

        conn = psycopg2.connect(pg_uri)
        conn.autocommit = True
        return conn
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Feldera helpers
# ---------------------------------------------------------------------------


def send_rows(pipeline, table, rows, batch_size=DEFAULT_BATCH_SIZE):
    """Send insert/upsert rows to Feldera in batches."""
    for i in range(0, len(rows), batch_size):
        pipeline.input_json(table, rows[i : i + batch_size])


def send_deletes(pipeline, table, ids, batch_size=DEFAULT_BATCH_SIZE):
    """Send delete operations to Feldera in batches."""
    delete_rows = [{"delete": {"id": id_}} for id_ in ids]
    for i in range(0, len(delete_rows), batch_size):
        pipeline.input_json(
            table,
            delete_rows[i : i + batch_size],
            update_format="insert_delete",
        )


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------


def pg_insert(conn, table, rows, batch_size=DEFAULT_BATCH_SIZE):
    """Insert rows (list of dicts) into a PostgreSQL table."""
    if not rows:
        return
    cols = list(rows[0].keys())
    col_names = ", ".join(cols)
    placeholders = "(" + ",".join(["%s"] * len(cols)) + ")"
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            args = ",".join(
                cur.mogrify(placeholders, tuple(row[c] for c in cols)).decode()
                for row in batch
            )
            cur.execute(f"INSERT INTO {table} ({col_names}) VALUES {args}")


def pg_update(conn, table, key_col, val_col, pairs, batch_size=DEFAULT_BATCH_SIZE):
    """Update *val_col* by *key_col* for a list of ``(key, value)`` pairs."""
    if not pairs:
        return
    with conn.cursor() as cur:
        for i in range(0, len(pairs), batch_size):
            batch = pairs[i : i + batch_size]
            args = ",".join(
                cur.mogrify("(%s,%s)", (val, key)).decode()
                for key, val in batch
            )
            cur.execute(
                f"UPDATE {table} AS t SET {val_col} = v.val "
                f"FROM (VALUES {args}) AS v(val, key) "
                f"WHERE t.{key_col} = v.key"
            )


def pg_delete(conn, table, key_col, ids):
    """Delete rows whose *key_col* is in *ids*."""
    if not ids:
        return
    with conn.cursor() as cur:
        id_list = ",".join(str(i) for i in ids)
        cur.execute(f"DELETE FROM {table} WHERE {key_col} IN ({id_list})")
