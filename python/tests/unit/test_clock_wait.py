from types import SimpleNamespace

import pytest

from tests import utils


@pytest.mark.parametrize("settled", ["2030-01-01 05:30:00", "2030-01-01 05:30:01"])
def test_advance_clock_wait_normalizes_rfc3339_timezone(monkeypatch, settled):
    class Pipeline:
        def __init__(self):
            self.values = iter(["2030-01-01 05:29:59", settled])

        def advance_clock(self, delta_ms):
            assert delta_ms == 1_000
            return {"now": "2030-01-01T05:30:00+05:30"}

        def query(self, statement):
            assert statement == "SELECT t FROM v;"
            return [{"t": next(self.values)}]

    def wait_for_view(_, condition, **_kwargs):
        assert not condition()
        assert condition()

    monkeypatch.setattr(utils, "wait_for_condition", wait_for_view)

    response = utils.advance_clock_and_wait_for_view(Pipeline(), 1_000, "v", "t")

    assert response["now"] == "2030-01-01T05:30:00+05:30"


def test_datagen_wait_polls_connector_status(monkeypatch):
    from tests.workloads import test_now

    statuses = iter([False, True])

    class Pipeline:
        def input_connector_stats(self, table_name, connector_name):
            assert (table_name, connector_name) == ("t", "datagen")
            return SimpleNamespace(metrics=SimpleNamespace(end_of_input=next(statuses)))

    monkeypatch.setattr(test_now.time, "sleep", lambda _: None)
    test_now._wait_datagen_end_of_input(Pipeline(), timeout_s=1)
