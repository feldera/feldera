from enum import Enum


class CompilationProfile(Enum):
    """
    The compilation profile to use when compiling the program.
    """

    SERVER_DEFAULT = None
    """
    The compiler server default compilation profile.
    """

    DEV = "dev"
    """
    The development compilation profile.
    """

    UNOPTIMIZED = "unoptimized"
    """
    The unoptimized compilation profile.
    """

    OPTIMIZED = "optimized"
    """
    The optimized compilation profile, the default for this API.
    """


class BuildMode(Enum):
    CREATE = 1
    GET = 2
    GET_OR_CREATE = 3
