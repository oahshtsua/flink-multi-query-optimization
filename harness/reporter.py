"""A report directory per harness run: results/<timestamp>_<query>/."""

import json
from datetime import datetime
from pathlib import Path

RESULTS = Path(__file__).parent.parent / "results"


class Report:
    def __init__(self, query, mode, root=RESULTS):
        started = datetime.now()
        self.dir = Path(root) / f"{started:%Y%m%d-%H%M%S}_{query}"
        self.dir.mkdir(parents=True)
        self.run = {
            "query": query,
            "mode": mode,
            "started": started.isoformat(timespec="seconds"),
            "queries": [],
        }
        self.write_run()

    def add_explain(self, name, explain):
        """Save an EXPLAIN result as Flink returned it."""
        (self.dir / f"explain_{name}.txt").write_text(explain.strip() + "\n")

    def add_query(self, query, job_id=None):
        """Save a submitted query's SQL and record it, with its job id, in run.json."""
        (self.dir / f"query_{query.name}.sql").write_text(query.sql + ";\n")
        self.run["queries"].append(
            {
                "name": query.name,
                "param": query.param,
                "value": query.value,
                "job_id": job_id,
            }
        )
        self.write_run()

    def write_run(self):
        (self.dir / "run.json").write_text(json.dumps(self.run, indent=2) + "\n")
