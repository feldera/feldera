from unittest import mock

from feldera.rest.feldera_client import FelderaClient


def test_validation_and_diff_forward_program_configuration():
    with mock.patch.object(
        FelderaClient, "get_config", return_value=mock.Mock(version="x")
    ):
        client = FelderaClient(url="http://example.test")
    client.http = mock.Mock()
    config = {
        "compiler": "java",
        "runtime_version": "v0.382.0",
        "use_platform_compiler": True,
    }
    client.pipeline_diff("p", program_config=config)
    assert client.http.post.call_args.kwargs["body"] == {"program_config": config}
    client.pipeline_diff("p", program_config={})
    assert client.http.post.call_args.kwargs["body"] == {"program_config": {}}
    client.pipeline_diff("p")
    assert "program_config" not in client.http.post.call_args.kwargs["body"]
    client.validate_program("SELECT 1", ir=True, program_config=config)
    assert client.http.post.call_args.kwargs["body"] == {
        "program_code": "SELECT 1",
        "ir": True,
        "program_config": config,
    }
