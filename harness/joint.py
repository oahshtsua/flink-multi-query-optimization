"""Submit multiple inserts as one Flink job.

For a Q2 example, start Kafka, Flink and the generator with
`make generator-up`, then run `uv run python joint.py` from `harness/`.
The example discards both outputs and prints one job ID. Use the Flink
UI at localhost:8081 to inspect and cancel the job.
"""

from gateway import Session
from variants import VariantGenerator


def execute_joint(session, inserts):
    """Submit two or more INSERTs after preparing their tables in the session.

    Pass each INSERT as a separate string, with an optional trailing semicolon.
    """
    statements = [statement.strip().removesuffix(";").rstrip() for statement in inserts]
    if len(statements) < 2 or any(not statement for statement in statements):
        raise ValueError("joint execution requires at least two non-empty INSERT statements")

    block = ";\n".join(statements)
    # Keep the block intact because execute_script splits on semicolons.
    return session.execute(f"EXECUTE STATEMENT SET\nBEGIN\n{block};\nEND;")


def main():
    generator = VariantGenerator()
    queries = [generator.render("q2"), generator.render("q2", {"divisor": 124})]
    with Session({"pipeline.name": "joint-q2"}) as session:
        # Read only the fields needed by Q2 from the existing Nexmark messages.
        session.execute_script("""
            CREATE TABLE events (
                event_type INT,
                bid ROW<auction BIGINT, price BIGINT>
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'nexmark',
                'properties.bootstrap.servers' = 'kafka:9092',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            );
            CREATE VIEW bid AS
                SELECT e.bid.auction AS auction, e.bid.price AS price
                FROM events AS e WHERE e.event_type = 2;
            CREATE TABLE output_a (auction BIGINT, price BIGINT)
                WITH ('connector' = 'blackhole');
            CREATE TABLE output_b (auction BIGINT, price BIGINT)
                WITH ('connector' = 'blackhole');
        """)
        result = execute_joint(session, [
            f"INSERT INTO output_a {queries[0]}",
            f"INSERT INTO output_b {queries[1]}",
        ])
        print(f"Joint job: {result.job_id}")


if __name__ == "__main__":
    main()
