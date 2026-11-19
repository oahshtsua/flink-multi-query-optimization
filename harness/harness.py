"""Run a base query and its variants on the Flink cluster in one of three modes."""

from pathlib import Path

import typer

from gateway import Session, split_statements
from reporter import Report
from variants import VariantGenerator

NEXMARK = (
    Path(__file__).parent.parent / "nexmark/nexmark-flink/src/main/resources/queries"
)

app = typer.Typer()


class NexmarkSession(Session):
    """A session that starts with Nexmark's `kafka` table and its person/auction/bid views."""

    def __init__(self, properties=None):
        super().__init__(properties)
        try:
            self.execute_script(
                (NEXMARK / "ddl_kafka.sql")
                .read_text()
                .replace("${BOOTSTRAP_SERVERS}", "kafka:9092")
            )
            self.execute_script(
                (NEXMARK / "ddl_views.sql")
                .read_text()
                .replace("${NEXMARK_TABLE}", "kafka")
            )
        except Exception:
            self.close()
            raise


def explain(session, statement):
    """The plan of a statement, as text sections followed by the operator graph in JSON."""
    result = session.execute("EXPLAIN JSON_EXECUTION_PLAN " + statement)
    return result.rows[0][0]


@app.command()
def single(query: str):
    """Run the base query alone, as one job."""
    base, _ = VariantGenerator().generate(query)
    report = Report(query, mode="single")
    with NexmarkSession({"pipeline.name": base.name}) as session:
        *setup, insert = split_statements(base.sql)
        for statement in setup:
            session.execute(statement)
        report.add_explain(base.name, explain(session, insert))
        job_id = session.execute(insert).job_id
    report.add_query(base, job_id)
    print(f"{base.name}: job {job_id}, report in {report.dir}")


@app.command()
def independent(query: str, param: str | None = None):
    """Run the base query and its variants, each as its own job."""
    pass


@app.command()
def joint(query: str, param: str | None = None):
    """Run the base query and its variants together in one statement set, as one job."""
    pass


if __name__ == "__main__":
    app()
