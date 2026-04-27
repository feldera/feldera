#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "feldera",
# ]
# ///
"""
Back up and restore Feldera pipeline definitions via the pipeline manager.

Examples:

  uv run scripts/backup_pipelines.py ./backup
  uv run scripts/backup_pipelines.py --restore ./backup
  uv run scripts/backup_pipelines.py --restore ./backup --url http://localhost:8081 --replace
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from textwrap import shorten
from typing import Any

from feldera import FelderaClient
from feldera.enums import PipelineFieldSelector
from feldera.rest.errors import FelderaAPIError
from feldera.rest.pipeline import Pipeline as RestPipeline

MANIFEST_FILE = "manifest.json"
UNSAFE_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
ERROR_WIDTH = 180


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Back up every Feldera pipeline's SQL/runtime config to a folder, "
            "or restore those pipeline definitions to a Feldera instance."
        )
    )
    parser.add_argument(
        "folder",
        type=Path,
        help="Backup folder to write to, or restore folder to read from.",
    )
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Restore pipelines from the folder.",
    )
    parser.add_argument(
        "--url",
        default=None,
        help=(
            "Feldera API URL. If omitted, the SDK uses FELDERA_HOST or "
            "http://localhost:8080."
        ),
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help=(
            "Optional Feldera API key. If omitted, the SDK will also check "
            "FELDERA_API_KEY."
        ),
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="During restore, update pipelines that already exist.",
    )
    return parser.parse_args()


def is_safe_filename_stem(pipeline_name: str) -> bool:
    return (
        bool(pipeline_name)
        and pipeline_name == pipeline_name.strip()
        and pipeline_name not in {".", ".."}
        and UNSAFE_FILENAME_CHARS.search(pipeline_name) is None
    )


def write_json(path: Path, data: Any) -> None:
    path.write_text(
        json.dumps(data, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def truncate(text: str, max_chars: int = ERROR_WIDTH) -> str:
    return shorten(" ".join(text.split()), width=max_chars, placeholder="...")


def describe_error(error: BaseException) -> str:
    if isinstance(error, FelderaAPIError):
        message = error.message or error.error or str(error)
        prefix = f"{error.error_code}: " if error.error_code else ""
        suffix = f" (HTTP {error.status_code})"
        return truncate(f"{prefix}{message}{suffix}")

    return truncate(str(error))


def is_storage_not_cleared_error(error: BaseException) -> bool:
    return (
        isinstance(error, FelderaAPIError)
        and error.error_code == "EditRestrictedToClearedStorage"
    )


def print_section(title: str, fields: dict[str, Any]) -> None:
    print(title)
    for label, value in fields.items():
        print(f"  {label}: {value}")
    print()


def print_status(label: str, message: str, detail: str | None = None) -> None:
    print(f"  {label.upper():<5} {message}")
    if detail:
        print(f"        {detail}")


def pipeline_attr(pipeline: Any, name: str, default: Any) -> Any:
    value = getattr(pipeline, name, default)
    return default if value is None else value


def validate_pipeline_filenames(pipelines: list[RestPipeline]) -> None:
    invalid_names = [
        pipeline.name
        for pipeline in pipelines
        if not is_safe_filename_stem(pipeline.name)
    ]
    if invalid_names:
        raise ValueError(
            "Cannot back up pipeline names that are unsafe as filenames: "
            + ", ".join(repr(name) for name in invalid_names)
        )

    seen: dict[str, str] = {}
    for pipeline in pipelines:
        stem = pipeline.name
        key = stem.casefold()
        if key in seen:
            raise RuntimeError(
                "Pipeline names produce the same filename on case-insensitive "
                f"filesystems: {seen[key]!r} and {pipeline.name!r}"
            )
        seen[key] = pipeline.name


def backup(client: FelderaClient, folder: Path, source_url: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    manifest_path = folder / MANIFEST_FILE
    tmp_manifest_path = manifest_path.with_suffix(manifest_path.suffix + ".tmp")

    pipelines = client.pipelines(selector=PipelineFieldSelector.ALL)
    validate_pipeline_filenames(pipelines)

    print_section(
        "Feldera backup",
        {
            "source": source_url,
            "folder": folder,
            "pipelines": len(pipelines),
        },
    )

    manifest: dict[str, Any] = {
        "format_version": 1,
        "source_url": source_url,
        "pipelines": [],
    }

    for pipeline in pipelines:
        sql_file = f"{pipeline.name}.sql"
        runtime_config_file = f"{pipeline.name}_cfg.json"

        (folder / sql_file).write_text(
            pipeline_attr(pipeline, "program_code", ""),
            encoding="utf-8",
        )
        write_json(
            folder / runtime_config_file,
            pipeline_attr(pipeline, "runtime_config", {}),
        )
        print_status(
            "ok",
            pipeline.name,
            f"wrote {sql_file} and {runtime_config_file}",
        )

        manifest["pipelines"].append(
            {
                "name": pipeline.name,
                "sql_file": sql_file,
                "runtime_config_file": runtime_config_file,
                "description": pipeline_attr(pipeline, "description", ""),
                "program_config": pipeline_attr(pipeline, "program_config", {}),
                "udf_rust": pipeline_attr(pipeline, "udf_rust", ""),
                "udf_toml": pipeline_attr(pipeline, "udf_toml", ""),
            }
        )

    write_json(tmp_manifest_path, manifest)
    os.replace(tmp_manifest_path, manifest_path)
    print(f"Backup complete: {len(pipelines)} pipeline(s) written to {folder}")


def load_manifest(folder: Path) -> list[dict[str, Any]]:
    manifest_path = folder / MANIFEST_FILE
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Restore folder has no {MANIFEST_FILE}: {folder}. "
            "The backup may be incomplete."
        )

    manifest = read_json(manifest_path)
    pipelines = manifest.get("pipelines")
    if not isinstance(pipelines, list):
        raise ValueError(f"{manifest_path} does not contain a pipeline list")

    return pipelines


def warn_missing_restore_files(folder: Path, records: list[dict[str, Any]]) -> None:
    for record in records:
        name = record.get("name", "<unknown>")
        sql_file = record.get("sql_file")
        runtime_config_file = record.get("runtime_config_file")

        if not sql_file:
            print_status("warn", name, "manifest entry has no sql_file")
        elif not (folder / sql_file).exists():
            print_status(
                "warn",
                name,
                f"SQL file listed in manifest is missing: {sql_file}",
            )

        if not runtime_config_file:
            print_status("warn", name, "manifest entry has no runtime_config_file")
        elif not (folder / runtime_config_file).exists():
            print_status(
                "warn",
                name,
                "runtime config file listed in manifest is missing: "
                f"{runtime_config_file}; using defaults",
            )


def restore(
    client: FelderaClient,
    folder: Path,
    target_url: str,
    replace: bool,
) -> None:
    if not folder.is_dir():
        raise FileNotFoundError(f"Restore folder does not exist: {folder}")

    records = load_manifest(folder)
    warn_missing_restore_files(folder, records)
    restored = 0
    restored_with_default_config: list[str] = []
    cleared_storage: list[str] = []
    failures: list[tuple[str, str]] = []
    print_section(
        "Feldera restore",
        {
            "target": target_url,
            "folder": folder,
            "pipelines": len(records),
            "mode": "replace existing pipelines" if replace else "create only",
        },
    )

    for record in records:
        name = record.get("name", "<unknown>")
        try:
            sql_path = folder / record["sql_file"]
            cfg_path = folder / record["runtime_config_file"]

            sql = sql_path.read_text(encoding="utf-8")
            try:
                runtime_config = read_json(cfg_path) if cfg_path.exists() else {}
            except Exception as error:
                print_status(
                    "warn",
                    name,
                    "could not read runtime config; using defaults. "
                    f"{describe_error(error)}",
                )
                runtime_config = {}

            def make_pipeline(config: dict[str, Any]) -> RestPipeline:
                return RestPipeline(
                    name=name,
                    sql=sql,
                    udf_rust=record.get("udf_rust", ""),
                    udf_toml=record.get("udf_toml", ""),
                    program_config=record.get("program_config", {}),
                    runtime_config=config,
                    description=record.get("description", ""),
                )

            def create_or_update(config: dict[str, Any]) -> None:
                pipeline = make_pipeline(config)
                if replace:
                    client.create_or_update_pipeline(pipeline, wait=False)
                else:
                    client.create_pipeline(pipeline, wait=False)

            def create_or_update_after_clear_if_needed(
                config: dict[str, Any],
            ) -> None:
                try:
                    create_or_update(config)
                except FelderaAPIError as error:
                    if not replace or not is_storage_not_cleared_error(error):
                        raise

                    print_status(
                        "warn",
                        name,
                        "restore needs cleared storage; stopping and clearing",
                    )
                    client.stop_pipeline(name, force=True, wait=True)
                    client.clear_storage(name, wait=True)
                    cleared_storage.append(name)
                    print_status("info", name, "storage cleared; retrying restore")
                    create_or_update(config)

            try:
                create_or_update_after_clear_if_needed(runtime_config)
            except FelderaAPIError as error:
                if runtime_config == {}:
                    raise
                print_status(
                    "warn",
                    name,
                    "saved runtime config rejected; retrying with defaults. "
                    f"{describe_error(error)}",
                )
                try:
                    create_or_update_after_clear_if_needed({})
                    restored_with_default_config.append(name)
                    print_status("ok", name, "restored with default runtime config")
                except Exception as retry_error:
                    failures.append(
                        (
                            name,
                            "saved config failed: "
                            f"{describe_error(error)}; default config failed: "
                            f"{describe_error(retry_error)}",
                        )
                    )
                    print_status("fail", name, "restore failed")
                    continue
            else:
                print_status("ok", name, "restored")

            restored += 1
        except Exception as error:
            failures.append((name, describe_error(error)))
            print_status("fail", name, describe_error(error))

    print()
    print("Restore summary")
    print(f"  restored: {restored}")
    print(
        f"  restored with default runtime config: {len(restored_with_default_config)}"
    )
    print(f"  storage cleared before retry: {len(cleared_storage)}")
    print(f"  failed: {len(failures)}")
    if cleared_storage:
        print()
        print("Pipelines that needed storage cleared: " + ", ".join(cleared_storage))
    if restored_with_default_config:
        print()
        print(
            "Pipelines restored with default runtime config: "
            + ", ".join(restored_with_default_config)
        )
    if failures:
        print()
        print("Failed pipelines:")
        for name, error in failures:
            print(f"  - {name}: {error}")


def main() -> None:
    args = parse_args()
    client = FelderaClient(url=args.url, api_key=args.api_key)

    if args.restore:
        restore(
            client=client,
            folder=args.folder,
            target_url=client.config.url,
            replace=args.replace,
        )
    else:
        backup(client=client, folder=args.folder, source_url=client.config.url)


if __name__ == "__main__":
    main()
