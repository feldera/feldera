"""
Shared timeout and polling constants for SDK wait loops.
"""

# Wait timeout defaults (seconds)
WAIT_TIMEOUT_LIGHT_OPERATION_S = 60.0
WAIT_TIMEOUT_STANDARD_OPERATION_S = 300.0
WAIT_TIMEOUT_HEAVY_OPERATION_S = 600.0
WAIT_TIMEOUT_LONGEST_OPERATION_S = 3600.0
WAIT_TIMEOUT_PROGRAM_COMPILATION_S = 1800.0

# Poll interval defaults (seconds)
WAIT_POLL_INTERVAL_DEFAULT_S = 2.0

# Exponential backoff defaults for completion-token polling
BACKOFF_INITIAL_WAIT = 0.1
BACKOFF_MAX_WAIT = 5
BACKOFF_EXPONENT = 1.2
BACKOFF_RETRIES = 0
