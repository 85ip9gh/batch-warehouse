"""Load-layer tests: the SQL shape without a database, idempotency with one.

The split mirrors the rest of the suite. The pure string-shaping in ``load`` is
tested with no Postgres and no Arrow, so it runs in the fast job alongside the
conforming rules. The behaviour that only a real database can show, that a full
reload does not duplicate and that reloading one partition rewrites only that
partition, runs against a Postgres service container.

Set ``BW_REQUIRE_PG=1`` and a missing ``BW_PG_DSN`` becomes a failure instead of
a skip. CI sets it on the container job, because a suite that skips itself when
its database is absent reports green for a gate that never ran, which is the
exact trap ``BW_REQUIRE_SPARK`` closes for the Spark layer.
"""

from __future__ import annotations

import datetime as dt
import os

import pytest

import warehouse.load as load

REQUIRED = os.environ.get("BW_REQUIRE_PG") == "1"
DSN = os.environ.get("BW_PG_DSN")

if REQUIRED:
    import psycopg  # noqa: F401  - refuse to skip where a database is promised
    import pyarrow  # noqa: F401


# --- pure SQL shape, no database --------------------------------------------

def test_copy_sql_names_every_column() -> None:
    sql = load.copy_sql(load.OBSERVATION)
    assert sql.startswith("COPY wh.fact_posting_observation (")
    assert sql.rstrip().endswith("FROM STDIN")
    for column in load.OBSERVATION.columns:
        assert column in sql


def test_truncate_lists_all_six_tables() -> None:
    sql = load.truncate_all_sql()
    for table in load.ALL_TABLES:
        assert f"wh.{table.name}" in sql
    assert len(load.ALL_TABLES) == 6


def test_delete_partition_targets_the_observation_fact() -> None:
    assert load.delete_partition_sql() == "DELETE FROM wh.fact_posting_observation WHERE ingest_date = %s"


def test_observation_column_contract_is_pinned() -> None:
    # This must change only when the transform's output columns change, and then
    # on purpose. It is the tripwire for a column added on one side alone, which
    # would otherwise load NULLs or shift every value one place over in silence.
    assert load.OBSERVATION.columns == (
        "ingest_date", "source_key", "job_url", "title", "company_key", "location_key",
        "date_posted", "job_type", "is_remote", "salary_min", "salary_max", "currency",
        "salary_interval", "source_file",
    )


def test_dimensions_load_before_facts() -> None:
    # Foreign keys are checked as each fact row arrives, so the dimensions have
    # to be filled first. ALL_TABLES encodes that order and the load relies on it.
    names = [t.name for t in load.ALL_TABLES]
    assert names.index("dim_company") < names.index("fact_posting_observation")
    assert names.index("dim_source") < names.index("fact_posting_observation")


# --- idempotency against a real Postgres ------------------------------------

def _need_pg() -> None:
    if not DSN:
        if REQUIRED:
            pytest.fail("BW_REQUIRE_PG=1 but BW_PG_DSN is unset: the gate would pass having tested nothing")
        pytest.skip("set BW_PG_DSN to run the load integration tests")


def _write_warehouse(root):
    """A small, referentially complete warehouse on disk, one Parquet dir per table.

    ingest and span dates are written as strings, exactly as the transform emits
    them, so the load's reliance on the target column parsing them into dates is
    exercised rather than assumed. date_posted is a real date with a null, so
    both paths are covered.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    def write(name, table):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(directory / "data.parquet"))

    write("dim_company", pa.table({
        "company_key": ["acme", "globex"],
        "company_name": ["Acme Inc.", "Globex"],
        "spelling_count": pa.array([1, 2], pa.int64()),
    }))
    write("dim_location", pa.table({
        "location_key": ["halifax|ns|ca", "raw:somewhere"],
        "city": ["Halifax", None],
        "province_code": ["NS", None],
        "province": ["Nova Scotia", None],
        "country_code": ["CA", None],
        "country": ["Canada", None],
        "resolved": pa.array([True, False], pa.bool_()),
        "raw_location": ["Halifax, NS, CA", "Somewhere"],
    }))
    write("dim_source", pa.table({
        "source_key": ["indeed", "company:workday"],
        "source_type": ["job_board", "employer_ats"],
        "platform": ["indeed", "workday"],
    }))
    write("dim_date", pa.table({
        "date_key": ["2026-08-20", "2026-08-21", "2026-08-22"],
        "date": pa.array([dt.date(2026, 8, 20), dt.date(2026, 8, 21), dt.date(2026, 8, 22)], pa.date32()),
        "year": pa.array([2026, 2026, 2026], pa.int32()),
        "month": pa.array([8, 8, 8], pa.int32()),
        "day": pa.array([20, 21, 22], pa.int32()),
        "day_name": ["Thursday", "Friday", "Saturday"],
        "iso_week": pa.array([34, 34, 34], pa.int32()),
        "is_weekend": pa.array([False, False, True], pa.bool_()),
    }))
    write("fact_posting_observation", pa.table({
        "ingest_date": ["2026-08-21", "2026-08-21", "2026-08-22"],
        "source_key": ["indeed", "company:workday", "indeed"],
        "job_url": ["https://x/1", "https://x/2", "https://x/1"],
        "title": ["Engineer", "Analyst", "Engineer"],
        "company_key": ["acme", "globex", "acme"],
        "location_key": ["halifax|ns|ca", "raw:somewhere", "halifax|ns|ca"],
        "date_posted": pa.array([dt.date(2026, 8, 20), None, dt.date(2026, 8, 20)], pa.date32()),
        "job_type": ["fulltime", "fulltime", "fulltime"],
        "is_remote": pa.array([False, None, False], pa.bool_()),
        "salary_min": pa.array([90000.0, None, 90000.0], pa.float64()),
        "salary_max": pa.array([110000.0, None, 110000.0], pa.float64()),
        "currency": ["CAD", None, "CAD"],
        "salary_interval": ["yearly", None, "yearly"],
        "source_file": ["f1.csv", "f2.csv", "f1.csv"],
    }))
    write("fact_posting", pa.table({
        "job_url": ["https://x/1", "https://x/2"],
        "title": ["Engineer", "Analyst"],
        "company_key": ["acme", "globex"],
        "location_key": ["halifax|ns|ca", "raw:somewhere"],
        "date_posted": pa.array([dt.date(2026, 8, 20), None], pa.date32()),
        "job_type": ["fulltime", "fulltime"],
        "is_remote": pa.array([False, None], pa.bool_()),
        "salary_min": pa.array([90000.0, None], pa.float64()),
        "salary_max": pa.array([110000.0, None], pa.float64()),
        "currency": ["CAD", None],
        "salary_interval": ["yearly", None],
        "first_seen_date": ["2026-08-21", "2026-08-21"],
        "last_seen_date": ["2026-08-22", "2026-08-21"],
        "observation_count": pa.array([2, 1], pa.int64()),
        "source_count": pa.array([1, 1], pa.int64()),
        "days_visible": pa.array([2, 1], pa.int32()),
    }))
    return root


@pytest.fixture
def conn():
    _need_pg()
    psycopg = pytest.importorskip("psycopg")
    pytest.importorskip("pyarrow")
    connection = psycopg.connect(DSN, autocommit=True)
    connection.execute("DROP SCHEMA IF EXISTS wh CASCADE")
    load.apply_schema(connection)
    try:
        yield connection
    finally:
        connection.execute("DROP SCHEMA IF EXISTS wh CASCADE")
        connection.close()


def _counts(connection) -> dict[str, int]:
    return {
        table.name: connection.execute(f"SELECT count(*) FROM wh.{table.name}").fetchone()[0]
        for table in load.ALL_TABLES
    }


def test_full_load_returns_the_row_counts_it_wrote(conn, tmp_path) -> None:
    warehouse = _write_warehouse(tmp_path / "warehouse")
    counts = load.run_full(warehouse, conn)
    assert counts["fact_posting_observation"] == 3
    assert counts["fact_posting"] == 2
    assert counts["dim_company"] == 2
    assert _counts(conn) == {
        "dim_company": 2, "dim_location": 2, "dim_source": 2, "dim_date": 3,
        "fact_posting_observation": 3, "fact_posting": 2,
    }


def test_a_second_full_load_neither_duplicates_nor_drifts(conn, tmp_path) -> None:
    warehouse = _write_warehouse(tmp_path / "warehouse")
    load.run_full(warehouse, conn)
    before = _counts(conn)
    salary_before = conn.execute(
        "SELECT salary_min FROM wh.fact_posting WHERE job_url = 'https://x/1'"
    ).fetchone()[0]

    load.run_full(warehouse, conn)

    assert _counts(conn) == before
    salary_after = conn.execute(
        "SELECT salary_min FROM wh.fact_posting WHERE job_url = 'https://x/1'"
    ).fetchone()[0]
    assert salary_after == salary_before


def test_reloading_one_partition_is_idempotent_and_local(conn, tmp_path) -> None:
    warehouse = _write_warehouse(tmp_path / "warehouse")
    load.run_full(warehouse, conn)

    def on(day: str) -> int:
        return conn.execute(
            "SELECT count(*) FROM wh.fact_posting_observation WHERE ingest_date = %s", (day,)
        ).fetchone()[0]

    assert on("2026-08-21") == 2 and on("2026-08-22") == 1

    load.run_partition(warehouse, conn, "2026-08-21")
    load.run_partition(warehouse, conn, "2026-08-21")

    assert on("2026-08-21") == 2, "reloading a partition twice must not duplicate it"
    assert on("2026-08-22") == 1, "reloading one partition must not touch another"


def test_foreign_keys_are_enforced_not_decorative(conn, tmp_path) -> None:
    psycopg = pytest.importorskip("psycopg")
    warehouse = _write_warehouse(tmp_path / "warehouse")
    load.run_full(warehouse, conn)
    # A fact row pointing at a source that is not in the dimension must be
    # rejected. If it were accepted the "dimensional model" would be six tables
    # that happen to sit near each other, not a model.
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute(
            "INSERT INTO wh.fact_posting_observation (ingest_date, source_key, job_url) "
            "VALUES ('2026-08-21', 'no-such-source', 'https://x/9')"
        )
