"""End-to-end check through the Flink REST APIs.

Opens a SQL Gateway session, has Flink write 1, 2, 3 to a fresh Kafka topic,
waits for that job on the cluster, reads the topic back and checks the sum.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

GATEWAY = os.environ.get("GATEWAY", "http://sql-gateway:8083")
FLINK = os.environ.get("FLINK", "http://jobmanager:8081")


def call(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data, {"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        errors = json.load(e).get("errors", [])
        causes = [l for err in errors for l in err.splitlines() if l.startswith("Caused by:")]
        sys.exit(f"{method} {url} failed: {causes[-1] if causes else errors}")


def open_session(properties):
    return call("POST", f"{GATEWAY}/v4/sessions", {"properties": properties})["sessionHandle"]


def close_session(session):
    call("DELETE", f"{GATEWAY}/v4/sessions/{session}")


def execute(session, statement):
    """Submit one statement and return all its result rows."""
    print(">", " ".join(statement.split()))
    op = call("POST", f"{GATEWAY}/v4/sessions/{session}/statements", {"statement": statement})["operationHandle"]
    rows, uri = [], f"/v4/sessions/{session}/operations/{op}/result/0"
    while uri:
        page = call("GET", GATEWAY + uri)
        if page["resultType"] == "NOT_READY":
            time.sleep(0.5)
            continue
        rows += [row["fields"] for row in page["results"]["data"]]
        uri = page.get("nextResultUri")
    print(f"  {rows}")
    return rows


def wait_for_job(job_id, timeout=60):
    """Poll the Flink cluster until the job reaches a terminal state."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = call("GET", f"{FLINK}/jobs/{job_id}")["state"]
        if state in ("FINISHED", "FAILED", "CANCELED"):
            print(f"  job {job_id}: {state}")
            return state
        time.sleep(0.5)
    sys.exit(f"job {job_id} did not finish within {timeout}s")


def main():
    topic = f"smoke-{int(time.time())}"
    session = open_session({"execution.runtime-mode": "batch"})
    try:
        execute(session, f"""
            CREATE TABLE smoke (id INT) WITH (
              'connector' = 'kafka',
              'topic' = '{topic}',
              'properties.bootstrap.servers' = 'kafka:9092',
              'scan.startup.mode' = 'earliest-offset',
              'scan.bounded.mode' = 'latest-offset',
              'format' = 'json'
            )""")
        [[job_id]] = execute(session, "INSERT INTO smoke VALUES (1), (2), (3)")
        if wait_for_job(job_id) != "FINISHED":
            sys.exit("smoke: FAILED (insert job did not finish)")
        [[total]] = execute(session, "SELECT SUM(id) FROM smoke")
    finally:
        close_session(session)

    if total != 6:
        sys.exit(f"smoke: FAILED (expected sum 6, got {total})")
    print("smoke: OK")


if __name__ == "__main__":
    main()
