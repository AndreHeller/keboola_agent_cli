"""Tests for OutputFormatter with JSON and Rich dual mode."""

import json
import sys
from io import StringIO

from rich.console import Console

from keboola_agent_cli.output import OutputFormatter, format_job_detail, format_jobs_table


class TestOutputFormatterJsonMode:
    """Tests for OutputFormatter in JSON mode."""

    def test_output_list_data(self) -> None:
        """JSON mode outputs a valid JSON envelope with list data."""
        formatter = OutputFormatter(json_mode=True, no_color=True)
        captured = StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            formatter.output([{"id": 1, "name": "test"}])
        finally:
            sys.stdout = old_stdout

        result = json.loads(captured.getvalue())
        assert result["status"] == "ok"
        assert result["data"] == [{"id": 1, "name": "test"}]

    def test_output_dict_data(self) -> None:
        """JSON mode outputs valid JSON with dict data."""
        formatter = OutputFormatter(json_mode=True, no_color=True)
        captured = StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            formatter.output({"key": "value"})
        finally:
            sys.stdout = old_stdout

        result = json.loads(captured.getvalue())
        assert result["status"] == "ok"
        assert result["data"]["key"] == "value"

    def test_output_empty_list(self) -> None:
        """JSON mode handles empty list data."""
        formatter = OutputFormatter(json_mode=True, no_color=True)
        captured = StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            formatter.output([])
        finally:
            sys.stdout = old_stdout

        result = json.loads(captured.getvalue())
        assert result["status"] == "ok"
        assert result["data"] == []

    def test_error_json_output(self) -> None:
        """JSON mode error outputs structured error envelope."""
        formatter = OutputFormatter(json_mode=True, no_color=True)
        captured = StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            formatter.error(
                message="Token expired",
                error_code="INVALID_TOKEN",
                project="prod-aws",
                retryable=False,
            )
        finally:
            sys.stdout = old_stdout

        result = json.loads(captured.getvalue())
        assert result["status"] == "error"
        assert result["error"]["code"] == "INVALID_TOKEN"
        assert result["error"]["message"] == "Token expired"
        assert result["error"]["project"] == "prod-aws"
        assert result["error"]["retryable"] is False

    def test_success_json_output(self) -> None:
        """JSON mode success outputs structured response."""
        formatter = OutputFormatter(json_mode=True, no_color=True)
        captured = StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            formatter.success("Project added successfully")
        finally:
            sys.stdout = old_stdout

        result = json.loads(captured.getvalue())
        assert result["status"] == "ok"
        assert result["data"]["message"] == "Project added successfully"


class TestOutputFormatterHumanMode:
    """Tests for OutputFormatter in human (Rich) mode."""

    def test_output_calls_human_formatter(self) -> None:
        """Human mode calls the provided human_formatter callable."""
        formatter = OutputFormatter(json_mode=False, no_color=True)
        called_with: list = []

        def mock_formatter(console: Console, data: object) -> None:
            called_with.append(data)

        formatter.output({"test": "data"}, mock_formatter)
        assert len(called_with) == 1
        assert called_with[0] == {"test": "data"}

    def test_output_without_formatter_does_not_crash(self) -> None:
        """Human mode without a formatter falls back to console.print and does not crash."""
        formatter = OutputFormatter(json_mode=False, no_color=True)
        formatter.output("simple string")

    def test_error_does_not_crash(self) -> None:
        """Human mode error output does not crash."""
        formatter = OutputFormatter(json_mode=False, no_color=True)
        formatter.error("Something went wrong")

    def test_success_does_not_crash(self) -> None:
        """Human mode success output does not crash."""
        formatter = OutputFormatter(json_mode=False, no_color=True)
        formatter.success("All good")


class TestOutputFormatterInit:
    """Tests for OutputFormatter initialization options."""

    def test_json_mode_flag(self) -> None:
        """json_mode flag is stored correctly."""
        formatter = OutputFormatter(json_mode=True)
        assert formatter.json_mode is True

    def test_verbose_flag(self) -> None:
        """verbose flag is stored correctly."""
        formatter = OutputFormatter(verbose=True)
        assert formatter.verbose is True

    def test_no_color_creates_console(self) -> None:
        """no_color creates a Console instance with color disabled."""
        formatter = OutputFormatter(no_color=True)
        assert formatter.console is not None
        assert formatter.err_console is not None


class TestFormatJobsTable:
    """Tests for format_jobs_table Rich output."""

    def test_jobs_table_with_jobs(self) -> None:
        """format_jobs_table renders table with job rows and project column."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {
            "jobs": [
                {
                    "id": 1001,
                    "status": "success",
                    "component": "keboola.ex-db-snowflake",
                    "configId": "101",
                    "createdTime": "2026-02-26T10:00:00Z",
                    "durationSeconds": 45,
                    "project_alias": "prod",
                },
                {
                    "id": 1002,
                    "status": "error",
                    "component": "keboola.wr-db-snowflake",
                    "configId": "201",
                    "createdTime": "2026-02-26T11:00:00Z",
                    "durationSeconds": 120,
                    "project_alias": "prod",
                },
            ],
            "errors": [],
        }

        format_jobs_table(console, data)
        output = console.file.getvalue()

        assert "Jobs" in output
        assert "Project" in output
        assert "prod" in output
        assert "1001" in output
        assert "1002" in output
        assert "success" in output
        assert "error" in output
        assert "45s" in output
        assert "2m 0s" in output

    def test_jobs_table_empty_jobs(self) -> None:
        """format_jobs_table shows helpful message when no jobs found."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {"jobs": [], "errors": []}

        format_jobs_table(console, data)
        output = console.file.getvalue()

        assert "No jobs found" in output

    def test_jobs_table_with_errors_only(self) -> None:
        """format_jobs_table shows errors when all projects fail."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {
            "jobs": [],
            "errors": [
                {
                    "project_alias": "bad",
                    "error_code": "INVALID_TOKEN",
                    "message": "Token expired",
                },
            ],
        }

        format_jobs_table(console, data)
        output = console.file.getvalue()

        assert "Warning" in output
        assert "bad" in output
        assert "Token expired" in output
        assert "all projects failed" in output

    def test_jobs_table_multi_project(self) -> None:
        """format_jobs_table shows both project aliases in one table."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {
            "jobs": [
                {
                    "id": 1001,
                    "status": "success",
                    "component": "comp-a",
                    "configId": "1",
                    "createdTime": "2026-02-26T10:00:00Z",
                    "durationSeconds": 10,
                    "project_alias": "prod",
                },
                {
                    "id": 2001,
                    "status": "processing",
                    "component": "comp-b",
                    "configId": "2",
                    "createdTime": "2026-02-26T12:00:00Z",
                    "project_alias": "dev",
                },
            ],
            "errors": [],
        }

        format_jobs_table(console, data)
        output = console.file.getvalue()

        assert "prod" in output
        assert "dev" in output
        assert "Project" in output

    def test_jobs_table_duration_formatting(self) -> None:
        """format_jobs_table formats duration in human-readable format."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {
            "jobs": [
                {
                    "id": 1,
                    "status": "success",
                    "component": "c",
                    "configId": "1",
                    "createdTime": "2026-01-01T00:00:00Z",
                    "durationSeconds": 3661,  # 1h 1m
                    "project_alias": "x",
                },
            ],
            "errors": [],
        }

        format_jobs_table(console, data)
        output = console.file.getvalue()

        assert "1h 1m" in output

    def test_jobs_table_no_duration(self) -> None:
        """format_jobs_table shows '-' for jobs without duration info."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {
            "jobs": [
                {
                    "id": 1,
                    "status": "processing",
                    "component": "c",
                    "configId": "1",
                    "createdTime": "2026-01-01T00:00:00Z",
                    "project_alias": "x",
                },
            ],
            "errors": [],
        }

        format_jobs_table(console, data)
        output = console.file.getvalue()

        # Should contain a dash for missing duration
        assert "-" in output


class TestFormatJobDetail:
    """Tests for format_job_detail Rich output."""

    def test_job_detail_renders_key_fields(self) -> None:
        """format_job_detail renders all key fields in a panel."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {
            "id": "1001",
            "status": "error",
            "component": "keboola.ex-db-snowflake",
            "config": "123",
            "mode": "run",
            "type": "standard",
            "createdTime": "2026-02-26T10:00:00Z",
            "startTime": "2026-02-26T10:00:05Z",
            "endTime": "2026-02-26T10:00:50Z",
            "durationSeconds": 45,
            "branchId": "538",
            "url": "https://queue.keboola.com/jobs/1001",
            "result": {
                "message": "Validation Error: missing field",
                "error": {"type": "user"},
            },
            "project_alias": "prod",
        }

        format_job_detail(console, data)
        output = console.file.getvalue()

        assert "1001" in output
        assert "prod" in output
        assert "error" in output
        assert "keboola.ex-db-snowflake" in output
        assert "123" in output
        assert "run" in output
        assert "standard" in output
        assert "45s" in output
        assert "538" in output
        assert "queue.keboola.com" in output
        assert "Validation Error" in output
        assert "user" in output

    def test_job_detail_success_job(self) -> None:
        """format_job_detail renders a successful job without error section."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {
            "id": "2001",
            "status": "success",
            "component": "keboola.wr-db-snowflake",
            "config": "456",
            "mode": "run",
            "type": "standard",
            "createdTime": "2026-02-26T12:00:00Z",
            "durationSeconds": 120,
            "result": {"message": ""},
            "project_alias": "dev",
        }

        format_job_detail(console, data)
        output = console.file.getvalue()

        assert "2001" in output
        assert "dev" in output
        assert "success" in output
        assert "2m 0s" in output

    def test_job_detail_minimal_data(self) -> None:
        """format_job_detail handles minimal job data without crashing."""
        console = Console(file=StringIO(), no_color=True, force_terminal=False)
        data = {
            "id": "3001",
            "status": "processing",
            "component": "comp",
            "project_alias": "test",
        }

        format_job_detail(console, data)
        output = console.file.getvalue()

        assert "3001" in output
        assert "processing" in output
