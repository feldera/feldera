import pytest
from feldera.testutils import unique_pipeline_name
from tests.platform.helper import cleanup_pipeline, reset_pipeline


@pytest.fixture(name="pipeline_name")
def fixture_pipeline_name(request):
    # Use the test node name (or request.function.__name__) to seed uniqueness
    name = unique_pipeline_name(request.function.__name__)
    cleanup_pipeline(name)
    try:
        yield name
    finally:
        reset_pipeline(name)
