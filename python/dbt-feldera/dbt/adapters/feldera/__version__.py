from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version


def determine_client_version() -> str:
    """Return the installed dbt-feldera package version.

    Uses ``importlib.metadata`` so the value always reflects
    what is declared in ``pyproject.toml`` at install time,
    exactly like the feldera Python client does.
    """
    try:
        return _pkg_version("dbt-feldera")
    except PackageNotFoundError:
        return "unknown"


version = determine_client_version()
