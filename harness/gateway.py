"""Client for the Flink SQL Gateway REST API (v4).

    with Session({"pipeline.name": "q1"}) as session:
        session.execute_script(ddl)
        job_id = session.execute("INSERT INTO ...").job_id

Submitted jobs keep running after their session closes, so sessions are meant
to be short-lived. Every submission then starts from a clean catalog and its
own settings, and nothing leaks from one run into the next.
"""

import os
import time
from dataclasses import dataclass

import httpx

GATEWAY = os.environ.get("GATEWAY", "http://localhost:8083")


class GatewayError(Exception):
    """A request or statement failed; the message is Flink's root cause."""


@dataclass
class Result:
    columns: list[str]
    rows: list[list]
    job_id: str | None  # set when the statement started a Flink job


class Session:
    def __init__(self, properties=None, url=GATEWAY):
        self.http = httpx.Client(base_url=url, timeout=30)
        self.handle = self._call("POST", "/v4/sessions", json={"properties": properties or {}})["sessionHandle"]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        self._call("DELETE", f"/v4/sessions/{self.handle}")
        self.http.close()

    def execute(self, statement):
        """Run one statement and wait for its complete result.

        INSERTs return as soon as the job is submitted with the job id as the result.
        A SELECT on an unbounded stream never completes so dont run those using this method.
        """
        operation_handle = self._call("POST", f"/v4/sessions/{self.handle}/statements", json={"statement": statement})["operationHandle"]
        columns, rows, job_id = [], [], None
        result_uri = f"/v4/sessions/{self.handle}/operations/{operation_handle}/result/0"
        while result_uri:
            page = self._call("GET", result_uri)
            if page["resultType"] == "NOT_READY":
                time.sleep(0.2)
                continue
            results = page.get("results", {})
            columns = columns or [c["name"] for c in results.get("columns", [])]
            rows += [row["fields"] for row in results.get("data", [])]
            job_id = job_id or page.get("jobID")
            result_uri = page.get("nextResultUri")
        return Result(columns, rows, job_id)

    def execute_script(self, script):
        """Run every statement of a SQL script (e.g. a Nexmark query file) in order."""
        return [self.execute(statement) for statement in split_statements(script)]

    def _call(self, method, path, **kwargs):
        response = self.http.request(method, path, **kwargs)
        if response.is_error:
            raise GatewayError(root_cause(response))
        return response.json()


def split_statements(script):
    """Split a script on ';', after dropping full-line '--' comments."""
    code = "\n".join(line for line in script.splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in code.split(";") if statement.strip()]


def root_cause(response):
    """The gateway returns Java stack traces; the last 'Caused by:' line is the useful one."""
    try:
        errors = response.json().get("errors", [])
    except ValueError:
        return f"HTTP {response.status_code}: {response.text[:200]}"
    causes = [line for error in errors for line in error.splitlines() if line.startswith("Caused by:")]
    return causes[-1] if causes else "; ".join(errors) or f"HTTP {response.status_code}"
