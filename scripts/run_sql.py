"""Run one Snowflake SQL file and save command output to the project log."""

import argparse
import shutil
import subprocess
from pathlib import Path

if __package__:
    from . import configure_logging
else:
    from runtime_logging import configure_logging


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a SQL file with Snowflake CLI and record its output."
    )
    parser.add_argument("--connection", default="etl_conn")
    parser.add_argument("--file", required=True, help="SQL file, relative to the repo root.")
    args = parser.parse_args()

    logger = configure_logging(__file__)
    project_root = Path(__file__).resolve().parent.parent
    sql_path = project_root / args.file
    if not sql_path.is_file():
        logger.error("SQL file does not exist: %s.", sql_path)
        return 2

    snow_cli = shutil.which("snow")
    if snow_cli is None:
        logger.error("Snowflake CLI 'snow' was not found on PATH; SQL was not executed.")
        return 2

    command = [
        snow_cli,
        "sql",
        "--connection",
        args.connection,
        "--filename",
        str(sql_path),
    ]
    logger.info("Running SQL file %s using connection %s.", sql_path, args.connection)
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.SubprocessError):
        logger.exception("Failed to execute SQL file %s.", sql_path)
        return 1

    if result.stdout.strip():
        logger.info("Snowflake CLI output for %s:\n%s", sql_path.name, result.stdout.strip())
    if result.returncode != 0:
        logger.error(
            "SQL file %s failed with exit code %s.", sql_path, result.returncode
        )
        if result.stderr.strip():
            logger.error("Snowflake CLI error for %s:\n%s", sql_path.name, result.stderr.strip())
        return result.returncode or 1

    if result.stderr.strip():
        logger.warning("Snowflake CLI stderr for %s:\n%s", sql_path.name, result.stderr.strip())
    logger.info("SQL file %s completed successfully.", sql_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
