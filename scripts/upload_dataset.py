"""Upload the Meetup CSV files to the Snowflake internal stage."""

import argparse
import shutil
import subprocess
import time
from pathlib import Path

if __package__:
    from . import configure_logging
else:
    from runtime_logging import configure_logging


DATASET_FILES = (
    "categories.csv",
    "cities.csv",
    "events.csv",
    "groups.csv",
    "groups_topics.csv",
    "members.csv",
    "members_topics.csv",
    "topics.csv",
    "venues.csv",
)
STAGE = "@RAW_DATA.STG_MEETUP"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload the Meetup CSV files from the repository's data folder."
    )
    parser.add_argument(
        "--connection",
        default="etl_conn",
        help="Snowflake CLI connection name (default: etl_conn).",
    )
    args = parser.parse_args()
    logger = configure_logging(__file__)
    logger.info("Starting Meetup dataset upload to %s.", STAGE)

    snow_cli = shutil.which("snow")
    if snow_cli is None:
        logger.error("Snowflake CLI 'snow' was not found on PATH; no files were uploaded.")
        return 2

    logger.info("Checking Snowflake connection '%s'.", args.connection)
    try:
        connection_result = subprocess.run(
            [snow_cli, "connection", "test", "--connection", args.connection],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.SubprocessError):
        logger.exception(
            "Could not test Snowflake connection '%s'; no files were uploaded.",
            args.connection,
        )
        return 2

    if connection_result.returncode != 0:
        logger.error(
            "Snowflake connection '%s' failed its test; no files were uploaded.",
            args.connection,
        )
        if connection_result.stdout.strip():
            logger.error("Connection test output:\n%s", connection_result.stdout.strip())
        if connection_result.stderr.strip():
            logger.error("Connection test error:\n%s", connection_result.stderr.strip())
        return connection_result.returncode or 2

    logger.info("Snowflake connection '%s' is available.", args.connection)
    data_dir = Path(__file__).resolve().parent.parent / "data"
    failures: list[str] = []
    uploaded = 0
    for name in DATASET_FILES:
        path = data_dir / name
        if not path.is_file():
            failures.append(name)
            logger.error("Skipping %s: dataset file does not exist at %s.", name, path)
            continue

        command = [
            snow_cli,
            "stage",
            "copy",
            str(path),
            STAGE,
            "--connection",
            args.connection,
            "--overwrite",
            "--auto-compress",
            "--parallel",
            "8",
        ]
        logger.info("Uploading %s.", name)
        started_at = time.perf_counter()
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
            failures.append(name)
            logger.exception("Upload command failed to run for %s.", name)
            continue

        elapsed_seconds = time.perf_counter() - started_at
        if result.returncode != 0:
            failures.append(name)
            logger.error(
                "Upload failed for %s with exit code %s after %.2f seconds.",
                name,
                result.returncode,
                elapsed_seconds,
            )
            if result.stdout.strip():
                logger.error("Snowflake CLI stdout for %s:\n%s", name, result.stdout.strip())
            if result.stderr.strip():
                logger.error("Snowflake CLI stderr for %s:\n%s", name, result.stderr.strip())
            continue

        uploaded += 1
        logger.info("Uploaded %s successfully in %.2f seconds.", name, elapsed_seconds)

    if failures:
        logger.error(
            "Upload finished with errors: %d/%d files uploaded; failures: %s.",
            uploaded,
            len(DATASET_FILES),
            ", ".join(failures),
        )
        return 1

    logger.info("Uploaded all %d files successfully to %s.", uploaded, STAGE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
