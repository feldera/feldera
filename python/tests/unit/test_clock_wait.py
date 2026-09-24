from tests import utils


def test_advance_clock_wait_normalizes_rfc3339_timezone(monkeypatch):
    class Pipeline:
        def __init__(self):
            self.values = iter(["2030-01-01 05:29:59", "2030-01-01 05:30:00"])

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

    response = utils.advance_clock_and_wait_for_view(Pipeline(), 1_000)

    assert response["now"] == "2030-01-01T05:30:00+05:30"
