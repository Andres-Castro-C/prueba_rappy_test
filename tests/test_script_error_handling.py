import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import run_sql, upload_dataset
from scripts.runtime_logging import configure_logging


class UploadDatasetTests(unittest.TestCase):
    @patch("scripts.upload_dataset.configure_logging")
    @patch("scripts.upload_dataset.shutil.which", return_value="snow")
    @patch("scripts.upload_dataset.subprocess.run")
    def test_upload_continues_after_failures_and_reports_nonzero(
        self, run, which, configure_logging
    ):
        logger = Mock()
        configure_logging.return_value = logger
        run.side_effect = [
            SimpleNamespace(returncode=0, stdout="Connection test passed", stderr=""),
            SimpleNamespace(returncode=7, stdout="", stderr="Snowflake rejected file"),
            OSError("CLI could not start"),
            SimpleNamespace(returncode=0, stdout="uploaded", stderr=""),
        ]

        with (
            patch.object(
                upload_dataset,
                "DATASET_FILES",
                ("categories.csv", "cities.csv", "missing.csv", "events.csv"),
            ),
            patch.object(sys, "argv", ["upload_dataset.py"]),
        ):
            result = upload_dataset.main()

        self.assertEqual(result, 1)
        self.assertEqual(run.call_count, 4)
        self.assertEqual(run.call_args_list[-1].args[0][3], str(
            Path(upload_dataset.__file__).resolve().parent.parent
            / "data"
            / "events.csv"
        ))
        logger.exception.assert_called_once()
        logger.error.assert_any_call(
            "Upload finished with errors: %d/%d files uploaded; failures: %s.",
            1,
            4,
            "categories.csv, cities.csv, missing.csv",
        )

    @patch("scripts.upload_dataset.configure_logging")
    @patch("scripts.upload_dataset.shutil.which", return_value="snow")
    @patch(
        "scripts.upload_dataset.subprocess.run",
        return_value=SimpleNamespace(
            returncode=1, stdout="", stderr="connection unavailable"
        ),
    )
    def test_connection_failure_stops_before_attempting_uploads(
        self, run, which, configure_logging
    ):
        logger = Mock()
        configure_logging.return_value = logger

        with patch.object(sys, "argv", ["upload_dataset.py"]):
            result = upload_dataset.main()

        self.assertEqual(result, 1)
        run.assert_called_once()
        logger.error.assert_any_call(
            "Snowflake connection '%s' failed its test; no files were uploaded.",
            "etl_conn",
        )


class RunSqlTests(unittest.TestCase):
    @patch("scripts.run_sql.configure_logging")
    @patch("scripts.run_sql.shutil.which", return_value="snow")
    @patch(
        "scripts.run_sql.subprocess.run",
        return_value=SimpleNamespace(
            returncode=5, stdout="SQL output", stderr="SQL compilation error"
        ),
    )
    def test_sql_failure_is_logged_and_returned(self, run, which, configure_logging):
        logger = Mock()
        configure_logging.return_value = logger

        with patch.object(
            sys,
            "argv",
            ["run_sql.py", "--file", "sql/01_setup_stage.sql"],
        ):
            result = run_sql.main()

        self.assertEqual(result, 5)
        logger.error.assert_any_call(
            "SQL file %s failed with exit code %s.",
            Path(run_sql.__file__).resolve().parent.parent
            / "sql"
            / "01_setup_stage.sql",
            5,
        )
        logger.error.assert_any_call(
            "Snowflake CLI error for %s:\n%s",
            "01_setup_stage.sql",
            "SQL compilation error",
        )


class RuntimeLoggingTests(unittest.TestCase):
    def test_writes_diagnostics_to_a_rotating_log_file(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = Path(temp_dir) / "project" / "scripts" / "script.py"
            logger = configure_logging(str(script_path))
            try:
                logger.error("diagnostic test message")
                for handler in logger.handlers:
                    handler.flush()
                log_file = Path(temp_dir) / "project" / "logs" / "pipeline.log"
                self.assertIn("diagnostic test message", log_file.read_text(encoding="utf-8"))
            finally:
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
                    handler.close()


if __name__ == "__main__":
    unittest.main()
