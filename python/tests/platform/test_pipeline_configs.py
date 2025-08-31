from http import HTTPStatus

from .helper import (
    API_PREFIX,
    post_json,
    put_json,
    patch_json,
    gen_pipeline_name,
    cleanup_pipeline,
)


def _pipeline_url(name: str) -> str:
    return f"{API_PREFIX}/pipelines/{name}"


@gen_pipeline_name
def test_pipeline_runtime_config(pipeline_name):
    """
    Tests that the pipeline runtime configuration is validated and stored correctly,
    and that patching works on the field as whole.
    """

    # Valid runtime_config variants
    # Each item: (runtime_config_field_value, predicate_to_validate)
    valid_cases = [
        (
            None,  # explicit JSON null
            lambda rc: isinstance(rc, dict) and "workers" in rc,
        ),
        (
            {},
            lambda rc: isinstance(rc, dict) and rc.get("workers") is not None,
        ),
        (
            {"workers": 12},
            lambda rc: rc.get("workers") == 12,
        ),
        (
            {
                "workers": 100,
                "resources": {
                    "cpu_cores_min": 5,
                    "storage_mb_max": 2000,
                    "storage_class": "normal",
                },
            },
            lambda rc: rc.get("workers") == 100
            and rc.get("resources", {}).get("cpu_cores_min") == 5
            and rc.get("resources", {}).get("storage_mb_max") == 2000
            and rc.get("resources", {}).get("storage_class") == "normal",
        ),
    ]

    for idx, (runtime_config, predicate) in enumerate(valid_cases):
        body = {
            "name": pipeline_name,
            "program_code": f"-- runtime config variant {idx}",
        }
        if runtime_config is not None:
            body["runtime_config"] = runtime_config
        resp = put_json(_pipeline_url(pipeline_name), body)
        assert resp.status_code in (HTTPStatus.OK, HTTPStatus.CREATED), resp.text
        obj = resp.json()
        assert predicate(obj.get("runtime_config")), (
            f"Runtime config predicate failed for variant {idx}: {obj.get('runtime_config')}"
        )

    # Invalid runtime_config cases
    invalid_cases = [
        {"workers": "not-a-number"},
        {"resources": {"storage_mb_max": "not-a-number"}},
    ]
    for invalid in invalid_cases:
        body = {
            "name": pipeline_name,
            "program_code": "-- invalid variant",
            "runtime_config": invalid,
        }
        resp = put_json(_pipeline_url(pipeline_name), body)
        assert resp.status_code == HTTPStatus.BAD_REQUEST, resp.text
        err = resp.json()
        # Rust test checks error_code == InvalidRuntimeConfig
        assert err.get("error_code") == "InvalidRuntimeConfig", err

    # Patching original (create with explicit rich config)
    pipeline_name_2 = pipeline_name + "-2"
    cleanup_pipeline(pipeline_name_2)
    original_body = {
        "name": pipeline_name_2,
        "program_code": "-- patch original",
        "program_config": {
            "profile": "unoptimized",
            "cache": False,
        },
    }
    resp_orig = post_json(f"{API_PREFIX}/pipelines", original_body)
    assert resp_orig.status_code in (HTTPStatus.CREATED, HTTPStatus.OK), resp_orig.text

    # Patch modifying some nested fields
    patch_body = {
        "runtime_config": {
            "workers": 1,
            "resources": {"storage_mb_max": 123},
        }
    }
    resp = patch_json(_pipeline_url(pipeline_name), patch_body)
    assert resp.status_code == HTTPStatus.OK, resp.text
    rc_modified = resp.json()["runtime_config"]
    assert rc_modified.get("workers") == 1
    assert rc_modified.get("resources", {}).get("storage_mb_max") == 123


@gen_pipeline_name
def test_pipeline_program_config(pipeline_name):
    """
    Tests that the pipeline program configuration is validated and stored correctly,
    and that patching works on the field as whole.
    """

    valid_cases = [
        (None, lambda pc: isinstance(pc, dict) and "cache" in pc),
        (None, lambda pc: isinstance(pc, dict)),  # explicit null path
        ({}, lambda pc: isinstance(pc, dict)),
        ({"profile": "dev"}, lambda pc: pc.get("profile") == "dev"),
        ({"cache": True}, lambda pc: pc.get("cache") is True),
        (
            {"profile": "dev", "cache": False},
            lambda pc: pc.get("profile") == "dev" and pc.get("cache") is False,
        ),
    ]

    for idx, (program_config, predicate) in enumerate(valid_cases):
        body = {
            "name": pipeline_name,
            "program_code": f"sql-1 {idx}",  # SQL doesn't matter for this test
        }
        if program_config is not None:
            body["program_config"] = program_config
        resp = put_json(_pipeline_url(pipeline_name), body)
        assert resp.status_code in (HTTPStatus.OK, HTTPStatus.CREATED), resp.text
        obj = resp.json()
        assert predicate(obj.get("program_config")), (
            f"Program config predicate failed for variant {idx}: {obj.get('program_config')}"
        )

    # Invalid program_config variants
    invalid_program_configs = [
        {"profile": "does-not-exist"},
        {"cache": 123},
        {"profile": 123},
        {"profile": "unknown", "cache": "a"},
    ]
    for invalid in invalid_program_configs:
        body = {
            "name": pipeline_name,
            "program_code": "-- invalid program config",
            "program_config": invalid,
        }
        resp = put_json(_pipeline_url(pipeline_name), body)
        assert resp.status_code == HTTPStatus.BAD_REQUEST, resp.text
        err = resp.json()
        assert err.get("error_code") == "InvalidProgramConfig", err

    # Original full config
    original_body = {
        "name": pipeline_name,
        "program_code": "sql-2",
        "program_config": {
            "profile": "unoptimized",
            "cache": False,
        },
    }
    resp = put_json(_pipeline_url(pipeline_name), original_body)
    assert resp.status_code in (HTTPStatus.CREATED, HTTPStatus.OK), resp.text
    pc_original = resp.json()["program_config"]
    assert pc_original.get("profile") == "unoptimized"
    assert not pc_original.get("cache")

    # Patch no changes
    resp = patch_json(_pipeline_url(pipeline_name), {})
    assert resp.status_code == HTTPStatus.OK
    pc_after_noop = resp.json()["program_config"]
    assert pc_after_noop == pc_original

    # Patch modify cache only
    resp = patch_json(_pipeline_url(pipeline_name), {"program_config": {"cache": True}})
    assert resp.status_code == HTTPStatus.OK
    pc_modified = resp.json()["program_config"]
    assert pc_modified.get("cache") is True
