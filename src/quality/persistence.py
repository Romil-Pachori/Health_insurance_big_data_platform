from collections.abc import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def postgres_url(cfg: dict) -> str:
    return (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


def build_engine(cfg: dict) -> Engine:
    return create_engine(postgres_url(cfg), pool_pre_ping=True)


def ensure_schemas_and_quality_table(engine: Engine, schemas: dict[str, str]) -> None:
    with engine.begin() as conn:
        for schema in sorted(set(schemas.values())):
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS quality.check_results (
                    run_id TEXT NOT NULL,
                    run_date DATE NOT NULL,
                    check_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    details TEXT,
                    checked_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
        )


def persist_check_results(engine: Engine, rows: Iterable[dict]) -> None:
    insert_stmt = text(
        """
        INSERT INTO quality.check_results
            (run_id, run_date, check_name, status, severity, details, checked_at)
        VALUES
            (:run_id, :run_date, :check_name, :status, :severity, :details, :checked_at)
        """
    )
    with engine.begin() as conn:
        for row in rows:
            conn.execute(insert_stmt, row)

