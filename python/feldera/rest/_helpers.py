import logging
import os


def determine_client_version() -> str:
    from importlib.metadata import version, PackageNotFoundError

    try:
        version = version("feldera")
    except PackageNotFoundError:
        version = "unknown"

    return version


def requests_verify_from_env() -> str | bool:
    env_feldera_tls_insecure = os.environ.get("FELDERA_TLS_INSECURE")
    FELDERA_HTTPS_TLS_CERT = os.environ.get("FELDERA_HTTPS_TLS_CERT")

    if env_feldera_tls_insecure is not None and FELDERA_HTTPS_TLS_CERT is not None:
        logging.warning(
            "environment variables FELDERA_HTTPS_TLS_CERT and "
            + "FELDERA_TLS_INSECURE both are set."
            + "\nFELDERA_HTTPS_TLS_CERT takes priority."
        )

    if env_feldera_tls_insecure is None:
        feldera_tls_insecure = False
    else:
        feldera_tls_insecure = env_feldera_tls_insecure.strip().lower() in (
            "1",
            "true",
            "yes",
        )

    requests_verify = not feldera_tls_insecure
    if FELDERA_HTTPS_TLS_CERT is not None:
        requests_verify = FELDERA_HTTPS_TLS_CERT

    return requests_verify
