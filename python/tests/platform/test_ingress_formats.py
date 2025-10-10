# TODO: these tests should be part of runtime tests

import json
import time
from http import HTTPStatus
from urllib.parse import quote

from .helper import (
    http_request,
    api_url,
    start_pipeline,
    gen_pipeline_name,
    adhoc_query_json,
    create_pipeline,
)


def _ingress(
    pipeline: str,
    table: str,
    body: str,
    *,
    format: str = "json",
    update_format: str = "raw",
    array: bool = False,
    content_type: str | None = None,
):
    params = [f"format={format}", f"update_format={update_format}"]
    if array:
        params.append("array=true")
    path = api_url(
        f"/pipelines/{pipeline}/ingress/{table}"
        + ("?" + "&".join(params) if params else "")
    )
    headers = {}
    if content_type:
        headers["Content-Type"] = content_type
    else:
        headers["Content-Type"] = (
            "application/json" if format == "json" else "text/plain"
        )
    return http_request("POST", path, data=body.encode("utf-8"), headers=headers)


def _change_stream_start(pipeline: str, object_name: str):
    # object_name may already has quotes around it; if so, percent-encode them.
    if object_name.startswith('"') and object_name.endswith('"'):
        encoded = quote(object_name, safe="")
    else:
        encoded = object_name
    path = api_url(
        f"/pipelines/{pipeline}/egress/{encoded}?format=json&backpressure=true"
    )
    r = http_request("POST", path, stream=True)
    return r


def _read_json_events(resp, expected_count: int, timeout_s: float = 10.0):
    """
    Read expected_count JSON events from a streaming response.
    """
    events = []
    start = time.time()
    for line in resp.iter_lines():
        if not line:
            continue
        try:
            for data in json.loads(line.decode("utf-8")).get("json_data"):
                events.append(data)
        except Exception as e:  # noqa: BLE001
            raise AssertionError(f"Invalid JSON line: {line!r} ({e})")
        if len(events) >= expected_count:
            break
        if time.time() - start > timeout_s:
            raise TimeoutError(
                f"Timeout reading events (wanted {expected_count}, got {len(events)})"
            )
    return events


class JsonLineReader:
    def __init__(self, resp):
        self.resp = resp
        self._iter = resp.iter_lines()

    def read_events(self, n, timeout_s=10.0):
        events, start = [], time.time()
        while len(events) < n:
            try:
                line = next(self._iter)
            except StopIteration:
                # server closed the stream
                break
            if not line:
                if time.time() - start > timeout_s:
                    raise TimeoutError(
                        f"Timeout waiting for {n} events, got {len(events)}"
                    )
                continue
            try:
                payload = json.loads(line.decode("utf-8"))
                for data in payload.get("json_data") or []:
                    events.append(data)
                    if len(events) >= n:
                        break
            except Exception as e:
                raise AssertionError(f"Invalid JSON line: {line!r} ({e})")
        return events


@gen_pipeline_name
def test_json_ingress(pipeline_name):
    """
    Exercise raw inserts, insert_delete format, array format, parse errors,
    debezium update, and CSV ingestion with parse error.
    """
    sql = (
        "CREATE TABLE t1(c1 integer, c2 bool, c3 varchar) "
        "WITH ('materialized' = 'true'); "
        "CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;"
    )
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    # Raw format (missing some fields)
    r = _ingress(
        pipeline_name,
        "T1",
        '{"c1":10,"c2":true}\n{"c1":20,"c3":"foo"}',
        format="json",
        update_format="raw",
    )
    assert r.status_code == HTTPStatus.OK, r.text
    got = adhoc_query_json(pipeline_name, "select * from t1 order by c1, c2, c3")
    assert got == [
        {"c1": 10, "c2": True, "c3": None},
        {"c1": 20, "c2": None, "c3": "foo"},
    ]

    # insert_delete format (delete and new insert)
    r = _ingress(
        pipeline_name,
        "t1",
        '{"delete":{"c1":10,"c2":true}}\n{"insert":{"c1":30,"c3":"bar"}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK
    got = adhoc_query_json(pipeline_name, "select * from t1 order by c1, c2, c3")
    assert got == [
        {"c1": 20, "c2": None, "c3": "foo"},
        {"c1": 30, "c2": None, "c3": "bar"},
    ]

    # Insert via JSON array style
    r = _ingress(
        pipeline_name,
        "T1",
        '{"insert":[40,true,"buzz"]}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK

    # Use array of updates instead of newline-delimited JSON
    r = _ingress(
        pipeline_name,
        "T1",
        '[{"delete":[40,true,"buzz"]},{"insert":[50,true,""]}]',
        format="json",
        update_format="insert_delete",
        array=True,
    )
    assert r.status_code == HTTPStatus.OK

    got = adhoc_query_json(pipeline_name, "select * from T1 order by c1, c2, c3")
    # Expect 20,30,50
    assert got == [
        {"c1": 20, "c2": None, "c3": "foo"},
        {"c1": 30, "c2": None, "c3": "bar"},
        {"c1": 50, "c2": True, "c3": ""},
    ]

    # Trigger parse errors with array=true (some invalid types)
    bad_payload = (
        '[{"insert":[35,true,""]},'
        '{"delete":[40,"foo","buzz"]},'
        '{"insert":[true,true,""]}]'
    )
    r = _ingress(
        pipeline_name,
        "T1",
        bad_payload,
        format="json",
        update_format="insert_delete",
        array=True,
    )
    assert r.status_code == HTTPStatus.BAD_REQUEST, r.text
    assert "Errors parsing input data (2 errors)" in r.text

    # Even records that are parsed successfully don't get ingested when
    # using array format
    got = adhoc_query_json(pipeline_name, "select * from T1 order by c1, c2, c3")
    # Expect 20,30,50
    assert got == [
        {"c1": 20, "c2": None, "c3": "foo"},
        {"c1": 30, "c2": None, "c3": "bar"},
        {"c1": 50, "c2": True, "c3": ""},
    ]

    # Debezium CDC style ('u' update)
    r = _ingress(
        pipeline_name,
        "T1",
        '{"payload":{"op":"u","before":[50,true,""],"after":[60,true,"hello"]}}',
        format="json",
        update_format="debezium",
    )
    assert r.status_code == HTTPStatus.OK
    got = adhoc_query_json(pipeline_name, "select * from t1 order by c1, c2, c3")
    assert got[-1] == {"c1": 60, "c2": True, "c3": "hello"}

    # CSV with a parse error in the second row,
    # (the second record is invalid, but the other two should
    # get ingested).
    csv_body = "15,true,foo\nnot_a_number,true,ΑαΒβΓγΔδ\n16,false,unicode🚲"
    r = _ingress(
        pipeline_name,
        "t1",
        csv_body,
        format="csv",
        update_format="raw",
        content_type="text/csv",
    )
    # Expect BAD_REQUEST due to parse error, but first and last ingested
    assert r.status_code == HTTPStatus.BAD_REQUEST, r.text
    assert "Errors parsing input data (1 errors)" in r.text
    got = adhoc_query_json(pipeline_name, "select * from t1 order by c1, c2, c3")
    # Verify 15 & 16 present along with earlier rows (20,30,60, etc.)
    assert any(row["c1"] == 15 for row in got)
    assert any(row["c1"] == 16 for row in got)


@gen_pipeline_name
def test_map_column(pipeline_name):
    """
    Table with column of type MAP
    """
    sql = (
        "CREATE TABLE t1(c1 integer, c2 bool, c3 MAP<varchar,varchar>) "
        "WITH ('materialized'='true'); CREATE VIEW v1 AS SELECT * FROM t1;"
    )
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    r = _ingress(
        pipeline_name,
        "T1",
        '{"c1":10,"c2":true,"c3":{"foo":"1","bar":"2"}}\n{"c1":20}',
        format="json",
        update_format="raw",
    )
    assert r.status_code == HTTPStatus.OK
    got = adhoc_query_json(pipeline_name, "select * from t1 order by c1")
    assert got == [
        {"c1": 10, "c2": True, "c3": {"bar": "2", "foo": "1"}},
        {"c1": 20, "c2": None, "c3": None},
    ]


@gen_pipeline_name
def test_parse_datetime(pipeline_name):
    sql = "CREATE TABLE t1(t TIME, ts TIMESTAMP, d DATE) WITH ('materialized'='true');"
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    r = _ingress(
        pipeline_name,
        "t1",
        '{"t":"13:22:00","ts":"2021-05-20 12:12:33","d":"2021-05-20"}\n'
        '{"t":" 11:12:33.483221092 ","ts":" 2024-02-25 12:12:33 ","d":" 2024-02-25 "}',
        format="json",
        update_format="raw",
    )
    assert r.status_code == HTTPStatus.OK
    # Order by normalized
    got = adhoc_query_json(pipeline_name, "select * from t1 order by t, ts, d")
    # Compare normalized strings
    assert any(row["t"] == "11:12:33.483221092" for row in got)
    assert any(row["t"] == "13:22:00" for row in got)


@gen_pipeline_name
def test_quoted_columns(pipeline_name):
    sql = (
        'CREATE TABLE t1("c1" integer not null,"C2" bool not null,"😁❤" varchar not null,'
        "\"αβγ\" boolean not null, ΔΘ boolean not null) WITH ('materialized'='true');"
    )
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)
    r = _ingress(
        pipeline_name,
        "T1",
        '{"c1":10,"C2":true,"😁❤":"foo","αβγ":true,"δθ":false}',
        format="json",
        update_format="raw",
    )
    assert r.status_code == HTTPStatus.OK
    got = adhoc_query_json(pipeline_name, 'select * from t1 order by "c1"')
    assert got == [{"c1": 10, "C2": True, "😁❤": "foo", "αβγ": True, "δθ": False}]


@gen_pipeline_name
def test_primary_keys(pipeline_name):
    """
    Port of primary_keys: test insert/update/delete semantics with primary key.
    """
    sql = (
        "CREATE TABLE t1(id bigint not null, s varchar not null, primary key(id)) "
        "WITH ('materialized'='true');"
    )
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    # Insert two rows
    r = _ingress(
        pipeline_name,
        "T1",
        '{"insert":{"id":1,"s":"1"}}\n{"insert":{"id":2,"s":"2"}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK
    got = adhoc_query_json(pipeline_name, "select * from t1 order by id")
    assert got == [{"id": 1, "s": "1"}, {"id": 2, "s": "2"}]

    # Modify: insert (overwrite id=1) and update id=2
    r = _ingress(
        pipeline_name,
        "T1",
        '{"insert":{"id":1,"s":"1-modified"}}\n{"update":{"id":2,"s":"2-modified"}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK
    got = adhoc_query_json(pipeline_name, "select * from t1 order by id")
    assert got == [
        {"id": 1, "s": "1-modified"},
        {"id": 2, "s": "2-modified"},
    ]

    # Delete id=2
    r = _ingress(
        pipeline_name,
        "T1",
        '{"delete":{"id":2}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK
    got = adhoc_query_json(pipeline_name, "select * from t1 order by id")
    assert got == [{"id": 1, "s": "1-modified"}]


@gen_pipeline_name
def test_case_sensitive_tables(pipeline_name):
    """
    - Distinguish between quoted and unquoted identifiers.
    - Validate streaming outputs for two views.
    """
    sql = (
        'CREATE TABLE "TaBle1"(id bigint not null);'
        "CREATE TABLE table1(id bigint);"
        'CREATE MATERIALIZED VIEW "V1" AS SELECT * FROM "TaBle1";'
        'CREATE MATERIALIZED VIEW "v1" AS SELECT * FROM table1;'
    )
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    stream_v1 = _change_stream_start(pipeline_name, '"V1"')
    stream_v1_lower = _change_stream_start(pipeline_name, '"v1"')

    # Ingest into quoted "TaBle1"
    r = _ingress(
        pipeline_name,
        quote('"TaBle1"', safe=""),
        '{"insert":{"id":1}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK
    # Ingest into unquoted table1
    r = _ingress(
        pipeline_name,
        "table1",
        '{"insert":{"id":2}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK

    ev_v1 = _read_json_events(stream_v1, 1)
    ev_v1_lower = _read_json_events(stream_v1_lower, 1)
    assert ev_v1 == [{"insert": {"id": 1}}]
    assert ev_v1_lower == [{"insert": {"id": 2}}]

    # Validate adhoc queries respect case
    q1 = adhoc_query_json(pipeline_name, 'select * from "V1"')
    q2 = adhoc_query_json(pipeline_name, "select * from v1")
    assert q1 == [{"id": 1}]
    assert q2 == [{"id": 2}]


@gen_pipeline_name
def test_duplicate_outputs(pipeline_name):
    """
    multiple inserts producing duplicate output values.
    """
    sql = (
        "CREATE TABLE t1(id bigint not null, s varchar not null); "
        "CREATE VIEW v1 AS SELECT s FROM t1;"
    )
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    stream = _change_stream_start(pipeline_name, "V1")
    reader = JsonLineReader(stream)

    # First batch
    r = _ingress(
        pipeline_name,
        "T1",
        '{"insert":{"id":1,"s":"1"}}\n{"insert":{"id":2,"s":"2"}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK
    evs = reader.read_events(2)
    assert evs == [{"insert": {"s": "1"}}, {"insert": {"s": "2"}}]

    # Second batch
    r = _ingress(
        pipeline_name,
        "T1",
        '{"insert":{"id":3,"s":"3"}}\n{"insert":{"id":4,"s":"4"}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK
    evs = reader.read_events(2)
    assert evs == [{"insert": {"s": "3"}}, {"insert": {"s": "4"}}]

    # Duplicates
    r = _ingress(
        pipeline_name,
        "T1",
        '{"insert":{"id":5,"s":"1"}}\n{"insert":{"id":6,"s":"2"}}',
        format="json",
        update_format="insert_delete",
    )
    assert r.status_code == HTTPStatus.OK
    evs = reader.read_events(2)
    assert evs == [{"insert": {"s": "1"}}, {"insert": {"s": "2"}}]


@gen_pipeline_name
def test_upsert(pipeline_name):
    """
    - Insert several rows with composite PK.
    - Perform updates/inserts overwriting existing rows.
    - Perform no-op updates and deletes of non-existing keys.
    """
    sql = (
        "CREATE TABLE t1("
        "id1 bigint not null,"
        "id2 bigint not null,"
        "str1 varchar not null,"
        "str2 varchar,"
        "int1 bigint not null,"
        "int2 bigint,"
        "primary key(id1,id2)) "
        "WITH ('materialized'='true');"
    )
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)

    stream = _change_stream_start(pipeline_name, "T1")
    reader = JsonLineReader(stream)

    # Initial inserts (array=true)
    r = _ingress(
        pipeline_name,
        "T1",
        '[{"insert":{"id1":1,"id2":1,"str1":"1","int1":1}},'
        '{"insert":{"id1":2,"id2":1,"str1":"1","int1":1}},'
        '{"insert":{"id1":3,"id2":1,"str1":"1","int1":1}}]',
        format="json",
        update_format="insert_delete",
        array=True,
    )
    assert r.status_code == HTTPStatus.OK
    evs = reader.read_events(3)
    assert evs == [
        {
            "insert": {
                "id1": 1,
                "id2": 1,
                "str1": "1",
                "str2": None,
                "int1": 1,
                "int2": None,
            }
        },
        {
            "insert": {
                "id1": 2,
                "id2": 1,
                "str1": "1",
                "str2": None,
                "int1": 1,
                "int2": None,
            }
        },
        {
            "insert": {
                "id1": 3,
                "id2": 1,
                "str1": "1",
                "str2": None,
                "int1": 1,
                "int2": None,
            }
        },
    ]

    # Mixed updates
    r = _ingress(
        pipeline_name,
        "T1",
        '[{"update":{"id1":1,"id2":1,"str1":"2"}},'
        '{"update":{"id1":2,"id2":1,"str2":"foo"}},'
        '{"insert":{"id1":3,"id2":1,"str1":"1","str2":"2","int1":3,"int2":33}}]',
        format="json",
        update_format="insert_delete",
        array=True,
    )
    assert r.status_code == HTTPStatus.OK
    evs = reader.read_events(6)
    assert evs == [
        {
            "delete": {
                "id1": 1,
                "id2": 1,
                "str1": "1",
                "str2": None,
                "int1": 1,
                "int2": None,
            }
        },
        {
            "delete": {
                "id1": 2,
                "id2": 1,
                "str1": "1",
                "str2": None,
                "int1": 1,
                "int2": None,
            }
        },
        {
            "delete": {
                "id1": 3,
                "id2": 1,
                "str1": "1",
                "str2": None,
                "int1": 1,
                "int2": None,
            }
        },
        {
            "insert": {
                "id1": 1,
                "id2": 1,
                "str1": "2",
                "str2": None,
                "int1": 1,
                "int2": None,
            }
        },
        {
            "insert": {
                "id1": 2,
                "id2": 1,
                "str1": "1",
                "str2": "foo",
                "int1": 1,
                "int2": None,
            }
        },
        {
            "insert": {
                "id1": 3,
                "id2": 1,
                "str1": "1",
                "str2": "2",
                "int1": 3,
                "int2": 33,
            }
        },
    ]

    # No-op / mixed operations (some won't generate output)
    r = _ingress(
        pipeline_name,
        "T1",
        '[{"update":{"id1":1,"id2":1}},'
        '{"update":{"id1":2,"id2":1,"str2":null}},'
        '{"delete":{"id1":3,"id2":1}},'
        '{"delete":{"id1":4,"id2":1}},'
        '{"update":{"id1":4,"id2":1,"int1":0,"str1":""}}]',
        format="json",
        update_format="insert_delete",
        array=True,
    )
    assert r.status_code == HTTPStatus.OK
    # Expect 3 events: delete (id2=1 id1=2 old str2=foo), delete (id1=3...), insert (id1=2 updated str2 null)
    evs = reader.read_events(3)
    assert evs == [
        {
            "delete": {
                "id1": 2,
                "id2": 1,
                "str1": "1",
                "str2": "foo",
                "int1": 1,
                "int2": None,
            }
        },
        {
            "delete": {
                "id1": 3,
                "id2": 1,
                "str1": "1",
                "str2": "2",
                "int1": 3,
                "int2": 33,
            }
        },
        {
            "insert": {
                "id1": 2,
                "id2": 1,
                "str1": "1",
                "str2": None,
                "int1": 1,
                "int2": None,
            }
        },
    ]
