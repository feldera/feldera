"""CLI entry point: ``python3 -m pyfeldera``."""

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
    parser.add_argument("--bind-address", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--deploy-dir", default=None,
                        help="Where to deploy bundled files (default: ~/.pyfeldera)")
    parser.add_argument("--precompile", action="store_true",
                        help="Run precompile step and exit")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("extra", nargs="*",
                        help="Extra args forwarded to pipeline-manager")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    server = FelderaServer(
        bind_address=args.bind_address,
        port=args.port,
        deploy_dir=args.deploy_dir,
        extra_args=args.extra,
    )

    if args.precompile:
        server.precompile()
        sys.exit(0)

    server.start_blocking()


if __name__ == "__main__":
    main()
