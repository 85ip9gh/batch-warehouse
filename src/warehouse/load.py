"""Load the Parquet warehouse into PostgreSQL, idempotently.

The transform is a pure function of the landing directory and rewrites the whole
Parquet warehouse on every run. The load mirrors that property into Postgres: a
load of the same warehouse produces the same tables, with no duplicates and no
drift, because a load a batch pipeline cannot safely retry is not finished.

Two modes, and the difference is the promise the observation grain makes:

- A **full** load replaces every table from the current warehouse in one
  transaction. Dimensions are truncated and reloaded alongside the facts, so a
  posting or a company that has left the corpus leaves the warehouse too.
- A **partition** load replaces exactly one ingest date in the observation fact
  and touches nothing else. Reloading ``2026-08-21`` rewrites ``2026-08-21`` and
  no other day, which is what makes "reloading a partition is idempotent" a
  claim a test can check rather than one a comment asserts. It assumes the
  dimensions already hold that day's keys, which a prior full load guarantees.

Neither ``psycopg`` nor ``pyarrow`` is imported at module load. Everything above
the runtime section is pure string-shaping, so it is unit-tested with no
database and no Arrow, the same split the transform keeps between ``conform`` and
the Spark layer.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

SCHEMA = "wh"
SCHEMA_FILE = Path(__file__).resolve().parent / "schema.sql"


@dataclass(frozen=True)
class Table:
    """A target table and its column order.

    The column tuple is the contract between the Parquet the transform writes and
    the table the load fills. It is written out here rather than read from either
    side so a column added on one side and forgotten on the other fails a test
    instead of silently loading NULLs or shifting every value one place left.
    """

    name: str
    columns: tuple[str, ...]


# Dimensions are listed first and loaded first, so an immediate foreign key from
# a fact is satisfied by the time the fact row arrives.
DIMENSIONS: tuple[Table, ...] = (
    Table("dim_company", ("company_key", "company_name", "spelling_count")),
    Table(
        "dim_location",
        ("location_key", "city", "province_code", "province", "country_code", "country", "resolved", "raw_location"),
    ),
    Table("dim_source", ("source_key", "source_type", "platform")),
    Table("dim_date", ("date_key", "date", "year", "month", "day", "day_name", "iso_week", "is_weekend")),
)

FACTS: tuple[Table, ...] = (
    Table(
        "fact_posting_observation",
        (
            "ingest_date", "source_key", "job_url", "title", "company_key", "location_key",
            "date_posted", "job_type", "is_remote", "salary_min", "salary_max", "currency",
            "salary_interval", "source_file",
        ),
    ),
    Table(
        "fact_posting",
        (
            "job_url", "title", "company_key", "location_key", "date_posted", "job_type",
            "is_remote", "salary_min", "salary_max", "currency", "salary_interval",
            "first_seen_date", "last_seen_date", "observation_count", "source_count", "days_visible",
        ),
    ),
)

ALL_TABLES: tuple[Table, ...] = DIMENSIONS + FACTS
OBSERVATION: Table = FACTS[0]


# --- pure SQL shaping, no database ------------------------------------------

def qualified(name: str) -> str:
    return f"{SCHEMA}.{name}"


def copy_sql(table: Table) -> str:
    """``COPY`` into a named column list.

    Naming every column rather than relying on positional order means the load
    does not silently depend on the physical column order in the table matching
    the tuple above. The two are kept equal on purpose, and this makes a drift a
    loud error rather than a quiet transposition.
    """
    cols = ", ".join(table.columns)
    return f"COPY {qualified(table.name)} ({cols}) FROM STDIN"


def truncate_all_sql() -> str:
    """Empty every table in one statement.

    Listing all six in a single ``TRUNCATE`` lets Postgres clear them together
    despite the foreign keys between them, which a table-at-a-time truncate would
    reject unless it were ordered facts-first by hand.
    """
    names = ", ".join(qualified(t.name) for t in ALL_TABLES)
    return f"TRUNCATE {names}"


def delete_partition_sql() -> str:
    return f"DELETE FROM {qualified(OBSERVATION.name)} WHERE ingest_date = %s"


# --- runtime, needs psycopg and pyarrow -------------------------------------

def _read_rows(warehouse_dir: Path, table: Table) -> list[dict]:
    """Every row of one Parquet table, as dicts, in the table's declared columns.

    ``pyarrow`` is imported here, not at module top, so the shaping functions
    above stay importable with neither Arrow nor a JVM present.
    """
    import pyarrow.parquet as pq

    arrow = pq.read_table(str(Path(warehouse_dir) / table.name), columns=list(table.columns))
    return arrow.select(list(table.columns)).to_pylist()


def _tuples(rows: Iterable[dict], columns: Sequence[str]) -> Iterator[tuple]:
    for row in rows:
        yield tuple(row[col] for col in columns)


def _copy_table(cur, table: Table, rows: Iterable[dict]) -> int:
    """Stream one table in over ``COPY``.

    psycopg adapts each Python value to text: a ``date`` and a ``'YYYY-MM-DD'``
    string both arrive as a date, a ``bool`` as ``t``/``f``, ``None`` as NULL. The
    string dates the transform emits for ingest and span columns are parsed by
    the target column's own type, so no conversion is needed here.
    """
    written = 0
    with cur.copy(copy_sql(table)) as copy:
        for values in _tuples(rows, table.columns):
            copy.write_row(values)
            written += 1
    return written


def apply_schema(conn) -> None:
    """Create the schema if it is not already there. Safe to rerun."""
    conn.execute(SCHEMA_FILE.read_text(encoding="utf-8"))


def run_full(warehouse_dir: Path, conn) -> dict[str, int]:
    """Replace every table from the warehouse, in one transaction.

    Truncate first so the load is a replacement and not an accumulation, then
    fill dimensions before facts. The whole thing is one transaction: a load that
    fails halfway leaves the previous warehouse intact rather than a mix of the
    old dimensions and half the new facts.
    """
    counts: dict[str, int] = {}
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(truncate_all_sql())
            for table in ALL_TABLES:
                counts[table.name] = _copy_table(cur, table, _read_rows(warehouse_dir, table))
    # Outside the load transaction: give the planner fresh statistics so the
    # query-tuning step measures a real plan rather than one built on defaults.
    conn.execute(f"ANALYZE {qualified(OBSERVATION.name)}")
    conn.execute(f"ANALYZE {qualified('fact_posting')}")
    return counts


def run_partition(warehouse_dir: Path, conn, ingest_date: str) -> dict[str, int]:
    """Replace exactly one ingest date in the observation fact.

    Delete the partition and reload only its rows, in one transaction. Running it
    twice for the same date leaves the same rows and the same count, which is the
    idempotency the observation grain promises. It does not rebuild the derived
    dimensions or ``fact_posting``: those are functions of the whole corpus, and
    a full load owns them.
    """
    rows = [r for r in _read_rows(warehouse_dir, OBSERVATION) if str(r["ingest_date"]) == ingest_date]
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(delete_partition_sql(), (ingest_date,))
            written = _copy_table(cur, OBSERVATION, rows)
    return {OBSERVATION.name: written}


def connect(dsn: str | None = None):
    """Open a connection from an explicit DSN or ``BW_PG_DSN``.

    The DSN carries a password and never appears in this repository or the
    vault: it is read from the environment at run time, set by the operator or
    the orchestrator, and nowhere else.
    """
    import psycopg

    resolved = dsn or os.environ.get("BW_PG_DSN")
    if not resolved:
        raise SystemExit("no database: pass --dsn or set BW_PG_DSN")
    # autocommit, so the `with conn.transaction()` blocks are the only
    # transactions and apply_schema commits as it runs. Without it every
    # statement joins one implicit transaction that conn.close() rolls back at
    # the end: a load that prints its row counts and then quietly discards them.
    return psycopg.connect(resolved, autocommit=True)


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Load the Parquet warehouse into PostgreSQL.")
    parser.add_argument("--warehouse", required=True, type=Path, help="the Parquet warehouse directory")
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN; defaults to $BW_PG_DSN")
    parser.add_argument("--init-schema", action="store_true", help="create the schema first (idempotent)")
    parser.add_argument(
        "--ingest-date",
        default=None,
        help="load only this ingest date into the observation fact, replacing that partition",
    )
    args = parser.parse_args(argv)

    conn = connect(args.dsn)
    try:
        if args.init_schema:
            apply_schema(conn)
        if args.ingest_date:
            counts = run_partition(args.warehouse, conn, args.ingest_date)
        else:
            counts = run_full(args.warehouse, conn)
    finally:
        conn.close()

    width = max(len(name) for name in counts)
    for name, rows in counts.items():
        print(f"{name:<{width}}  {rows:>7} rows")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
