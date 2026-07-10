from .helper import (
    API_PREFIX,
    HTTPStatus,
    get,
    post_json,
    put_json,
    patch_json,
    delete,
    wait_for_program_success,
    gen_pipeline_name,
    cleanup_pipeline,
    create_pipeline,
    start_pipeline,
    pause_pipeline,
    resume_pipeline,
    stop_pipeline,
    clear_pipeline,
)

# Field selector constants

PIPELINE_FIELD_SELECTOR_ALL_FIELDS = [
    "id",
    "name",
    "description",
    "tags",
    "created_at",
    "version",
    "platform_version",
    "runtime_config",
    "program_code",
    "udf_rust",
    "udf_toml",
    "program_config",
    "program_version",
    "program_status",
    "program_status_since",
    "program_error",
    "program_info",
    "deployment_status",
    "deployment_status_since",
    "deployment_desired_status",
    "deployment_desired_status_since",
    "deployment_error",
    "refresh_version",
    "storage_status",
    "storage_status_details",
    "deployment_resources_status",
    "deployment_resources_status_details",
    "deployment_resources_status_since",
    "deployment_resources_desired_status",
    "deployment_resources_desired_status_since",
    "deployment_runtime_status",
    "deployment_runtime_status_details",
    "deployment_runtime_status_since",
    "deployment_runtime_desired_status",
    "deployment_runtime_desired_status_since",
    "deployment_initial",
    "deployment_id",
    "bootstrap_policy",
    "silent_bootstrap",
    "concurrent_bootstrap",
]

PIPELINE_FIELD_SELECTOR_STATUS_FIELDS = [
    "id",
    "name",
    "description",
    "tags",
    "created_at",
    "version",
    "platform_version",
    "program_config",
    "program_version",
    "program_status",
    "program_status_since",
    "deployment_status",
    "deployment_status_since",
    "deployment_desired_status",
    "deployment_desired_status_since",
    "deployment_resources_status",
    "deployment_resources_status_since",
    "deployment_resources_desired_status",
    "deployment_resources_desired_status_since",
    "deployment_runtime_status",
    "deployment_runtime_status_details",
    "deployment_runtime_status_since",
    "deployment_runtime_desired_status",
    "deployment_runtime_desired_status_since",
    "deployment_id",
    "deployment_initial",
    "deployment_error",
    "refresh_version",
    "storage_status",
    "storage_status_details",
    "bootstrap_policy",
    "silent_bootstrap",
    "concurrent_bootstrap",
]


@gen_pipeline_name
def test_pipeline_post(pipeline_name):
    name_min = pipeline_name + "-min"
    name_sql2 = pipeline_name + "-sql2"
    name_all = pipeline_name + "-all"
    name_missing = pipeline_name + "-missing"
    cleanup_pipeline(name_min)
    cleanup_pipeline(name_sql2)
    cleanup_pipeline(name_all)
    cleanup_pipeline(name_missing)

    # Empty body
    r = post_json(f"{API_PREFIX}/pipelines", {})
    assert r.status_code == HTTPStatus.BAD_REQUEST

    # Name missing
    r = post_json(f"{API_PREFIX}/pipelines", {"program-code": ""})
    assert r.status_code == HTTPStatus.BAD_REQUEST

    # Program SQL code missing
    r = post_json(f"{API_PREFIX}/pipelines", {"name": name_missing})
    assert r.status_code == HTTPStatus.BAD_REQUEST

    # Minimum body
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": name_min,
            "program_code": "",
        },
    )
    assert r.status_code == HTTPStatus.CREATED
    pipeline = r.json()
    assert pipeline["name"] == name_min
    assert pipeline["description"] == ""
    assert pipeline["tags"] == []
    assert isinstance(pipeline["runtime_config"], dict)
    assert pipeline["program_code"] == ""
    assert pipeline.get("udf_rust", "") == ""
    assert pipeline.get("udf_toml", "") == ""
    assert isinstance(pipeline["program_config"], dict)

    # Body with (invalid) SQL
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": name_sql2,
            "program_code": "sql-2",
        },
    )
    assert r.status_code == HTTPStatus.CREATED
    pipeline = r.json()
    assert pipeline["name"] == name_sql2
    assert pipeline["program_code"] == "sql-2"
    assert pipeline["udf_rust"] == ""
    assert pipeline["udf_toml"] == ""

    # All fields
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": name_all,
            "description": "description-3",
            "tags": ["alpha", "beta-1"],
            "runtime_config": {"workers": 123},
            "program_code": "sql-3",
            "udf_rust": "rust-3",
            "udf_toml": "toml-3",
            "program_config": {"profile": "dev"},
        },
    )
    assert r.status_code == HTTPStatus.CREATED
    pipeline = r.json()
    assert pipeline["name"] == name_all
    assert pipeline["description"] == "description-3"
    assert pipeline["tags"] == ["alpha", "beta-1"]
    assert pipeline["runtime_config"]["workers"] == 123
    assert pipeline["program_code"] == "sql-3"
    assert pipeline["udf_rust"] == "rust-3"
    assert pipeline["udf_toml"] == "toml-3"
    assert pipeline["program_config"]["profile"] == "dev"


@gen_pipeline_name
def test_pipeline_get(pipeline_name):
    name1 = pipeline_name + "-1"
    name2 = pipeline_name + "-2"
    cleanup_pipeline(name1)
    cleanup_pipeline(name2)
    # Not found
    r = get(f"{API_PREFIX}/pipelines/nonexistent-pg")
    assert r.status_code == HTTPStatus.NOT_FOUND

    # List is initially empty
    r = get(f"{API_PREFIX}/pipelines")
    assert r.status_code == HTTPStatus.OK

    # Create first pipeline
    sql1 = "CREATE TABLE t1(c1 INT);"
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {"name": name1, "program_code": sql1},
    )
    assert r.status_code == HTTPStatus.CREATED

    # Retrieve list
    r = get(f"{API_PREFIX}/pipelines")
    assert r.status_code == HTTPStatus.OK
    list_all = r.json()
    assert any(obj["name"] == name1 for obj in list_all)

    # Retrieve first pipeline
    r = get(f"{API_PREFIX}/pipelines/{name1}")
    assert r.status_code == HTTPStatus.OK
    object1_1 = r.json()

    # Create second pipeline
    sql2 = "CREATE TABLE t2(c2 INT);"
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {"name": name2, "program_code": sql2},
    )
    assert r.status_code == HTTPStatus.CREATED

    # Retrieve list again
    r = get(f"{API_PREFIX}/pipelines")
    assert r.status_code == HTTPStatus.OK
    list2 = r.json()
    assert any(obj["name"] == name1 for obj in list2)
    assert any(obj["name"] == name2 for obj in list2)

    # Retrieve first pipeline again
    r = get(f"{API_PREFIX}/pipelines/{name1}")
    assert r.status_code == HTTPStatus.OK
    assert object1_1["id"] == r.json()["id"]

    # Retrieve second pipeline
    r = get(f"{API_PREFIX}/pipelines/{name2}")
    assert r.status_code == HTTPStatus.OK
    object2_2 = r.json()
    assert object2_2["name"] == name2
    assert object2_2["program_code"] == sql2


@gen_pipeline_name
def test_pipeline_get_selector(pipeline_name):
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {"name": pipeline_name, "program_code": "CREATE TABLE t1(c1 INT);"},
    )
    assert r.status_code == HTTPStatus.CREATED

    for base in [f"{API_PREFIX}/pipelines", f"{API_PREFIX}/pipelines/{pipeline_name}"]:
        for selector_value, expected_fields in [
            ("", PIPELINE_FIELD_SELECTOR_ALL_FIELDS),
            ("all", PIPELINE_FIELD_SELECTOR_ALL_FIELDS),
            ("status", PIPELINE_FIELD_SELECTOR_STATUS_FIELDS),
        ]:
            if selector_value:
                endpoint = f"{base}?selector={selector_value}"
            else:
                endpoint = base
            r = get(endpoint)
            assert r.status_code == HTTPStatus.OK
            val = r.json()
            if isinstance(val, list):
                assert len(val) >= 1
                obj = next(o for o in val if o["name"] == pipeline_name)
            else:
                obj = val
            keys = sorted(obj.keys())
            print(f"keys: {keys}")
            assert sorted(expected_fields) == keys


@gen_pipeline_name
def test_pipeline_create_compile_delete(pipeline_name):
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": pipeline_name,
            "description": "desc",
            "runtime_config": {},
            "program_code": "CREATE TABLE t1(c1 INTEGER);",
            "program_config": {},
        },
    )
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(pipeline_name, expected_program_version=1)

    # Delete
    dr = delete(f"{API_PREFIX}/pipelines/{pipeline_name}")
    assert dr.status_code == HTTPStatus.OK

    # Confirm gone
    gr = get(f"{API_PREFIX}/pipelines/{pipeline_name}")
    assert gr.status_code == HTTPStatus.NOT_FOUND


@gen_pipeline_name
def test_pipeline_name_conflict(pipeline_name):
    body = {
        "name": pipeline_name,
        "description": "desc",
        "runtime_config": {},
        "program_code": "CREATE TABLE t1(c1 INTEGER);",
        "program_config": {},
    }
    r1 = post_json(f"{API_PREFIX}/pipelines", body)
    assert r1.status_code == HTTPStatus.CREATED
    # Conflict
    body2 = dict(body)
    body2["description"] = "different"
    body2["program_code"] = "CREATE TABLE t2(c2 VARCHAR);"
    r2 = post_json(f"{API_PREFIX}/pipelines", body2)
    assert r2.status_code == HTTPStatus.CONFLICT


def test_pipeline_name_invalid():
    # Empty
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": "",
            "description": "",
            "runtime_config": {},
            "program_code": "",
            "program_config": {},
        },
    )
    # TODO check that there's enough information to figure out why the request is bad
    assert r.status_code == HTTPStatus.BAD_REQUEST

    # Too long
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": "a" * 101,
            "description": "",
            "runtime_config": {},
            "program_code": "",
            "program_config": {},
        },
    )
    # TODO check that there's enough information to figure out why the request is bad
    assert r.status_code == HTTPStatus.BAD_REQUEST

    # Invalid characters
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": "%abc",
            "description": "",
            "runtime_config": {},
            "program_code": "",
            "program_config": {},
        },
    )
    # TODO check that there's enough information to figure out why the request is bad
    assert r.status_code == HTTPStatus.BAD_REQUEST


@gen_pipeline_name
def test_refresh_version(pipeline_name):
    # Create
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": pipeline_name,
            "program_code": "",
        },
    )
    assert r.status_code == HTTPStatus.CREATED
    obj = r.json()
    assert obj["refresh_version"] == 1

    wait_for_program_success(pipeline_name, expected_program_version=1)
    obj = get(f"{API_PREFIX}/pipelines/{pipeline_name}").json()
    assert obj["refresh_version"] == 3

    # Patch program_code
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"program_code": "CREATE TABLE t1 ( v1 INT );"},
    )
    assert r.status_code == HTTPStatus.OK
    obj = r.json()
    assert obj["refresh_version"] == 4

    wait_for_program_success(pipeline_name, expected_program_version=2)
    obj = get(f"{API_PREFIX}/pipelines/{pipeline_name}").json()
    assert obj["refresh_version"] == 6


@gen_pipeline_name
def test_pipeline_connector_endpoint_naming(pipeline_name):
    sql = """
        CREATE TABLE t1 (i1 BIGINT) WITH (
            'connectors' = '[
                { "name": "abc", "transport": { "name": "datagen", "config": {} } },
                { "transport": { "name": "datagen", "config": {} } },
                { "name": "def", "transport": { "name": "datagen", "config": {} } },
                { "transport": { "name": "datagen", "config": {} } }
            ]'
        );

        CREATE TABLE t2 (i1 BIGINT) WITH (
            'connectors' = '[
                { "name": "c1", "transport": { "name": "datagen", "config": {} } }
            ]'
        );

        CREATE TABLE t3 (i1 BIGINT) WITH (
            'connectors' = '[
                { "transport": { "name": "datagen", "config": {} } }
            ]'
        );

        CREATE MATERIALIZED VIEW v1 WITH (
            'connectors' = '[
                { "transport": { "name": "kafka_output", "config": { "topic": "p1" } } },
                { "name": "c1", "transport": { "name": "kafka_output", "config": { "topic": "p1" } } },
                { "transport": { "name": "kafka_output", "config": { "topic": "p1" } } },
                { "name": "c3", "transport": { "name": "kafka_output", "config": { "topic": "p1" } } }
            ]'
        ) AS ( SELECT  * FROM t1 );

        CREATE MATERIALIZED VIEW v2 WITH (
            'connectors' = '[
                { "name": "c1", "transport": { "name": "kafka_output", "config": { "topic": "p1" } } }
            ]'
        ) AS ( SELECT  * FROM t2 );

        CREATE MATERIALIZED VIEW v3 WITH (
            'connectors' = '[
                { "transport": { "name": "kafka_output", "config": { "topic": "p1" } } }
            ]'
        ) AS ( SELECT  * FROM t3 );
    """
    r = put_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"name": pipeline_name, "program_code": sql},
    )
    assert r.status_code in (HTTPStatus.OK, HTTPStatus.CREATED)
    wait_for_program_success(pipeline_name, expected_program_version=1)
    obj = get(f"{API_PREFIX}/pipelines/{pipeline_name}").json()

    input_connectors = sorted(obj["program_info"]["input_connectors"].keys())
    output_connectors = sorted(obj["program_info"]["output_connectors"].keys())

    assert input_connectors == [
        "t1.abc",
        "t1.def",
        "t1.unnamed-1",
        "t1.unnamed-3",
        "t2.c1",
        "t3.unnamed-0",
    ]
    assert output_connectors == [
        "v1.c1",
        "v1.c3",
        "v1.unnamed-0",
        "v1.unnamed-2",
        "v2.c1",
        "v3.unnamed-0",
    ]


@gen_pipeline_name
def test_pipeline_tags(pipeline_name):
    cleanup_pipeline(pipeline_name)

    # Create with tags.
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": pipeline_name,
            "description": "initial",
            "tags": ["prod", "team-billing"],
            "program_code": "",
        },
    )
    assert r.status_code == HTTPStatus.CREATED
    assert r.json()["tags"] == ["prod", "team-billing"]

    # Tags round-trip on read.
    r = get(f"{API_PREFIX}/pipelines/{pipeline_name}")
    assert r.status_code == HTTPStatus.OK
    assert r.json()["tags"] == ["prod", "team-billing"]

    # PATCH replaces the whole tags list; description is left untouched.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"tags": ["staging"]},
    )
    assert r.status_code == HTTPStatus.OK
    obj = r.json()
    assert obj["tags"] == ["staging"]
    assert obj["description"] == "initial"

    # PATCH of description leaves tags untouched (fields patch independently).
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"description": "changed"},
    )
    assert r.status_code == HTTPStatus.OK
    obj = r.json()
    assert obj["description"] == "changed"
    assert obj["tags"] == ["staging"]

    # PATCH with an empty list clears the tags (an empty list is a real value).
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"tags": []},
    )
    assert r.status_code == HTTPStatus.OK
    assert r.json()["tags"] == []

    # PUT replaces the pipeline wholesale: omitting tags resets them to empty.
    r = put_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {
            "name": pipeline_name,
            "tags": ["only-tag"],
            "program_code": "",
        },
    )
    assert r.status_code == HTTPStatus.OK
    assert r.json()["tags"] == ["only-tag"]

    r = put_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"name": pipeline_name, "program_code": ""},
    )
    assert r.status_code == HTTPStatus.OK
    assert r.json()["tags"] == []


@gen_pipeline_name
def test_pipeline_metadata_validation(pipeline_name):
    cleanup_pipeline(pipeline_name)

    # Each of these tags violates the constraints (disallowed character, too
    # long, empty) and must be rejected at creation time.
    invalid_tag_sets = [
        ["bad,tag"],
        ["a" * 51],
        [""],
        ["ok", "also,bad"],
    ]
    for tags in invalid_tag_sets:
        r = post_json(
            f"{API_PREFIX}/pipelines",
            {"name": pipeline_name, "tags": tags, "program_code": ""},
        )
        assert r.status_code == HTTPStatus.BAD_REQUEST, (
            f"tags {tags} should be rejected"
        )

    # A description one character over the 300-char limit is rejected too.
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {"name": pipeline_name, "description": "a" * 301, "program_code": ""},
    )
    assert r.status_code == HTTPStatus.BAD_REQUEST

    # Valid metadata is accepted: a tag set covering every allowed character
    # class, plus a description of exactly the maximum length.
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": pipeline_name,
            "tags": ["a-z_0/9", "with space", "pipe|sep", "back\\slash", "dot.dot"],
            "description": "a" * 300,
            "program_code": "",
        },
    )
    assert r.status_code == HTTPStatus.CREATED

    # Patching to an invalid tag is rejected.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"tags": ["bad?tag"]},
    )
    assert r.status_code == HTTPStatus.BAD_REQUEST

    # Patching to an over-long description is rejected.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"description": "a" * 301},
    )
    assert r.status_code == HTTPStatus.BAD_REQUEST


def _version_counters(pipeline_name):
    """Return the `(version, program_version, refresh_version)` triple."""
    obj = get(f"{API_PREFIX}/pipelines/{pipeline_name}?selector=status").json()
    return obj["version"], obj["program_version"], obj["refresh_version"]


@gen_pipeline_name
def test_pipeline_metadata_edits_do_not_bump_version(pipeline_name):
    """
    The `tags` and `description` fields are client-side metadata with no
    deployment semantics, so editing them must not move any of the pipeline's
    version counters (`version`, `program_version`, `refresh_version`).
    Editing a core field (here `program_code`) must, by contrast, bump them.
    """
    cleanup_pipeline(pipeline_name)

    # Create and let the program compile. Compilation bumps `refresh_version`
    # on its own; once it has settled, only explicit edits move the counters,
    # which makes the assertions below deterministic.
    r = post_json(
        f"{API_PREFIX}/pipelines",
        {
            "name": pipeline_name,
            "description": "initial",
            "tags": ["prod"],
            "program_code": "CREATE TABLE t1 ( c1 INT );",
        },
    )
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(pipeline_name, expected_program_version=1)

    baseline = _version_counters(pipeline_name)
    assert baseline[0] == 1, "freshly created pipeline starts at version 1"
    assert baseline[1] == 1, "freshly created pipeline starts at program_version 1"

    # Patching only the description leaves every counter where it was.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"description": "changed"},
    )
    assert r.status_code == HTTPStatus.OK
    assert r.json()["description"] == "changed"
    assert _version_counters(pipeline_name) == baseline

    # Patching only the tags is equally free.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"tags": ["staging", "team-x"]},
    )
    assert r.status_code == HTTPStatus.OK
    assert r.json()["tags"] == ["staging", "team-x"]
    assert _version_counters(pipeline_name) == baseline

    # Patching both fields in one request is still free, including clearing
    # the tags with an empty list.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"description": "again", "tags": []},
    )
    assert r.status_code == HTTPStatus.OK
    assert _version_counters(pipeline_name) == baseline

    # A no-op patch (re-sending the current values) does not move them either.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"description": "again", "tags": []},
    )
    assert r.status_code == HTTPStatus.OK
    assert _version_counters(pipeline_name) == baseline

    # Editing a core field bumps all three counters by one. Read them from the
    # PATCH response, which reflects the row immediately after the update,
    # before background recompilation moves `refresh_version` again.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"program_code": "CREATE TABLE t1 ( c1 INT, c2 INT );"},
    )
    assert r.status_code == HTTPStatus.OK
    obj = r.json()
    assert obj["version"] == baseline[0] + 1
    assert obj["program_version"] == baseline[1] + 1
    assert obj["refresh_version"] == baseline[2] + 1
    # The metadata is carried across the core edit untouched.
    assert obj["description"] == "again"
    assert obj["tags"] == []


@gen_pipeline_name
def test_pipeline_metadata_edits_at_deployment_stages(pipeline_name):
    """
    Editing `tags`/`description` is allowed at every stage of deployment
    (stopped, running, paused, stopped-with-storage), unlike core fields which
    are restricted to the stopped state. None of these metadata edits bump the
    pipeline `version`.
    """
    create_pipeline(pipeline_name, "CREATE TABLE t1 ( c1 INT );")

    def patch_metadata_ok(stage, description, tags):
        r = patch_json(
            f"{API_PREFIX}/pipelines/{pipeline_name}",
            {"description": description, "tags": tags},
        )
        assert r.status_code == HTTPStatus.OK, f"metadata patch while {stage}: {r.text}"
        obj = r.json()
        assert obj["description"] == description, f"description while {stage}"
        assert obj["tags"] == tags, f"tags while {stage}"
        # `version` only ever moves on a core edit, so it is a stable witness
        # that the metadata patch did not bump the pipeline version, even while
        # the pipeline is live and background status updates move other counters.
        assert obj["version"] == base_version, f"version bumped while {stage}"

    base_version = _version_counters(pipeline_name)[0]

    # Stopped, storage cleared.
    patch_metadata_ok("stopped", "while-stopped", ["s1"])

    # Running.
    start_pipeline(pipeline_name)
    patch_metadata_ok("running", "while-running", ["r1", "r2"])

    # A core edit while running is rejected; it requires the stopped state.
    r = patch_json(
        f"{API_PREFIX}/pipelines/{pipeline_name}",
        {"program_code": "CREATE TABLE t1 ( c1 INT, c2 INT );"},
    )
    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert r.json()["error_code"] == "UpdateRestrictedToStopped"
    # The rejected core edit did not change the version either.
    assert _version_counters(pipeline_name)[0] == base_version

    # Paused.
    pause_pipeline(pipeline_name)
    patch_metadata_ok("paused", "while-paused", [])

    # Stopped again, but with storage still in use (not cleared).
    resume_pipeline(pipeline_name)
    stop_pipeline(pipeline_name, force=True)
    patch_metadata_ok("stopped-with-storage", "while-stopped-inuse", ["final"])

    clear_pipeline(pipeline_name)
