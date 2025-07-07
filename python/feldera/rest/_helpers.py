def client_version() -> str:
    from importlib.metadata import version, PackageNotFoundError

    try:
        version = version("feldera")
    except PackageNotFoundError:
        version = "unknown"

    return version
