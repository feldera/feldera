import logging
from enum import Enum

logger = logging.getLogger(__name__)


class FelderaEdition(Enum):
    """
    The compilation profile to use when compiling the program.
    Represents the Feldera edition between Enterprise and Open source.
    """

    ENTERPRISE = "Enterprise"
    """
    The Enterprise version of Feldera.
    """

    ENTERPRISE_DEV = "EnterpriseDev"
    """
    The Enterprise edition for development use.
    """

    OPEN_SOURCE = "Open source"
    """
    The open source version of Feldera.
    """

    UNKNOWN = "Unknown"
    """
    An edition this client does not recognize, reported by a newer server.
    """

    @staticmethod
    def from_value(value):
        error = None
        if isinstance(value, dict):
            error = value
            value = list(value.keys())[0]

        for member in FelderaEdition:
            if member.value.lower() == value.lower():
                member.error = error
                return member
        # A newer server may report an edition this client predates; connecting must still work.
        logger.warning(f"Unknown Feldera edition '{value}'")
        return FelderaEdition.UNKNOWN

    def is_enterprise(self):
        return self in (FelderaEdition.ENTERPRISE, FelderaEdition.ENTERPRISE_DEV)


class FelderaConfig:
    """
    General configuration of the current Feldera instance.
    """

    def __init__(self, cfg: dict):
        self.changelog_url = cfg.get("changelog_url")
        self.edition = FelderaEdition.from_value(cfg.get("edition"))
        self.license_validity = cfg.get("license_validity")
        self.revision = cfg.get("revision")
        self.telemetry = cfg.get("telemetry")
        self.update_info = cfg.get("update_info")
        self.version = cfg.get("version")
