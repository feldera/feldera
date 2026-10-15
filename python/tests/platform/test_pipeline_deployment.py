from http import HTTPStatus

from feldera.testutils import FELDERA_TEST_NUM_HOSTS

from .helper import (
    api_url,
    create_pipeline,
    gen_pipeline_name,
    get,
    start_pipeline_as_paused,
    stop_pipeline,
)

RESOURCE_FIELDS = [
    "cpu_cores_min",
    "cpu_cores_max",
    "memory_min",
    "memory_max",
    "storage_min",
    "storage_max",
]

STATES = {"Current", "InProgress", "Failed", "Terminating", "NotFound", "Unknown"}


def _deployment(name: str) -> dict:
    r = get(api_url(f"/pipelines/{name}/deployment"))
    assert r.status_code == HTTPStatus.OK, r.text
    return r.json()


@gen_pipeline_name
def test_pipeline_deployment(pipeline_name):
    """
    GET /deployment returns the spec of the last deployment. Runners that observe
    their resources, such as the Kubernetes runner, also return a status per host.
    """
    create_pipeline(pipeline_name, "")

    # Never started: there is no deployment config yet.
    spec = _deployment(pipeline_name)["spec"]["resources"]
    assert all(spec[field] is None for field in RESOURCE_FIELDS), spec

    start_pipeline_as_paused(pipeline_name)
    deployment = _deployment(pipeline_name)
    pipeline = get(api_url(f"/pipelines/{pipeline_name}")).json()
    resources = pipeline["runtime_config"]["resources"]
    for field in RESOURCE_FIELDS:
        assert deployment["spec"]["resources"][field] == resources.get(field), field

    status = deployment.get("status")
    if status is not None:
        assert status["state"] in STATES, status
        assert len(status["hosts"]) == max(FELDERA_TEST_NUM_HOSTS, 1), status
        for host in status["hosts"]:
            assert host["compute"]["state"] in STATES, host

    # The spec stays after stop: it is what the next start reuses or replaces.
    stop_pipeline(pipeline_name)
    assert _deployment(pipeline_name)["spec"] == deployment["spec"]


def test_pipeline_deployment_unknown_pipeline():
    r = get(api_url("/pipelines/test-pipeline-deployment-does-not-exist/deployment"))
    assert r.status_code == HTTPStatus.NOT_FOUND, r.text
    assert r.json()["error_code"] == "UnknownPipelineName", r.text
