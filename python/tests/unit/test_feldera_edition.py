from feldera.rest.feldera_config import FelderaEdition


def test_editions_parse_and_classify():
    """Every edition the server reports parses, and unknown ones do not raise."""
    assert FelderaEdition.from_value("Enterprise").is_enterprise()
    assert FelderaEdition.from_value("EnterpriseDev") == FelderaEdition.ENTERPRISE_DEV
    assert FelderaEdition.from_value("EnterpriseDev").is_enterprise()
    assert not FelderaEdition.from_value("Open source").is_enterprise()
    assert FelderaEdition.from_value("SomethingNew") == FelderaEdition.UNKNOWN
    assert not FelderaEdition.from_value("SomethingNew").is_enterprise()
