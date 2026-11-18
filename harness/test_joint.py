"""Submission checks without a running Flink cluster: uv run python -m unittest."""

import json
import unittest
from unittest.mock import Mock

import httpx

from gateway import Session
from joint import execute_joint


class JointSubmissionTest(unittest.TestCase):
    def test_submits_one_intact_block_and_returns_job(self):
        requests = []

        def handle(request):
            requests.append(request)
            if request.url.path.endswith("/statements"):
                return httpx.Response(200, json={"operationHandle": "op"})
            return httpx.Response(200, json={
                "resultType": "PAYLOAD", "jobID": "joint-job",
                "results": {"columns": [], "data": []},
            })

        session = Session.__new__(Session)
        session.handle = "session"
        with httpx.Client(base_url="http://gateway", transport=httpx.MockTransport(handle)) as client:
            session.http = client
            result = execute_joint(session, [
                "INSERT INTO a SELECT 'a;b' FROM source;  ",
                "INSERT INTO b SELECT 'a;b' FROM source",
            ])
        submissions = [request for request in requests if request.method == "POST"]
        self.assertEqual(len(submissions), 1)
        self.assertEqual(json.loads(submissions[0].content)["statement"],
                         "EXECUTE STATEMENT SET\nBEGIN\n"
                         "INSERT INTO a SELECT 'a;b' FROM source;\n"
                         "INSERT INTO b SELECT 'a;b' FROM source;\nEND;")
        self.assertEqual(result.job_id, "joint-job")

    def test_rejects_missing_or_empty_queries_before_submission(self):
        session = Session.__new__(Session)
        session.execute = Mock()
        for inserts in ([], ["INSERT INTO a SELECT 1"], ["INSERT INTO a SELECT 1", ";"]):
            with self.subTest(inserts=inserts), self.assertRaises(ValueError):
                execute_joint(session, inserts)
        session.execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
