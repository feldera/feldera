"""CLI entry point: ``python -m pyfeldera``."""

from __future__ import annotations

import argparse
import logging
import sys

from pyfeldera.server import FelderaServer


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pyfeldera",
        description="Start a self-contained Feldera pipeline-manager.",
    )
    parser.add_argument(
        "--bind-address",
        default="127.0.0.1",
        help="IP address to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="HTTP port (default: 8080)",
    )
    parser.add_argument(
        "--working-dir",
        default=None,
        help="Base working directory for Feldera state (default: ~/.feldera)",
    )
    parser.add_argument(
        "--feldera-home",
        default=None,
        help="Override path for the Feldera source tree (crates, Cargo.lock, etc.)",
    )
    parser.add_argument(
        "--precompile",
        action="store_true",
        help="Run the precompile step and exit",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Python log level (default: INFO)",
    )
    parser.add_argument(
        "extra",
        nargs="*",
        help="Extra arguments forwarded to pipeline-manager",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    server = FelderaServer(
        bind_address=args.bind_address,
        port=args.port,
        working_dir=args.working_dir,
        feldera_home=args.feldera_home,
        extra_args=args.extra,
    )

    if args.precompile:
        server.precompile()
        sys.exit(0)

    server.start_blocking()


if __name__ == "__main__":
    main()
