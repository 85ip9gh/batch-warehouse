"""Spark-layer tests: the shape of the facts, not the conforming rules.

These need a local Spark session, which is not a cluster. They skip when pyspark
or a JVM is absent so the conforming suite still runs anywhere, and they never
write Parquet, because writing needs Hadoop native binaries that are not present
on every developer machine. Compute is what these assert on.

Set BW_REQUIRE_SPARK=1 and the skip becomes a failure. CI sets it, because a
suite that skips itself when a dependency goes missing reports green for a gate
that did not run, and this module skipped silently for two days that way.

`conform.py` owns the judgement and is tested exhaustively without any of this.
What is left for here is the part unit tests cannot reach: whether the grain
holds, whether dedup keeps what it should, and whether a rerun is deterministic.
"""

from __future__ import annotations

import os

import pytest

REQUIRED = os.environ.get("BW_REQUIRE_SPARK") == "1"

if REQUIRED:
    import pyspark  # noqa: F401  - refuse to skip where Spark is promised
else:
    pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

from warehouse.transform import build_observations, build_postings, build_session  # noqa: E402

LANDED_COLUMNS = [
    "id", "site", "job_url", "title", "company", "location", "date_posted",
    "job_type", "is_remote", "min_amount", "max_amount", "currency", "interval",
    "_ingest_date", "_source_file",
]


@pytest.fixture(scope="session")
def spark():
    # Built through build_session rather than by hand, so these tests inherit
    # the worker-path guarantee production gets. Building a session here
    # directly is what let the UDF path fail everywhere except one image: the
    # tests passed under a PYTHONPATH typed at the command line and nothing
    # recorded that they depended on it.
    try:
        session = build_session(app_name="tests", master="local[1]", shuffle_partitions=1)
    except Exception as exc:  # pragma: no cover - environment dependent
        if REQUIRED:
            raise
        pytest.skip(f"no usable Spark session: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def landed(spark, rows: list[dict]):
    filled = [{col: row.get(col) for col in LANDED_COLUMNS} for row in rows]
    return spark.createDataFrame(filled, schema=", ".join(f"{c} string" for c in LANDED_COLUMNS))


def observation(url: str, date: str, site: str = "indeed", **extra) -> dict:
    row = {
        "job_url": url, "site": site, "_ingest_date": date,
        "_source_file": f"jobspy-results-{date}-local-{site}.csv",
        "title": "Software Engineer", "company": "Acme Inc.",
        "location": "Halifax, NS, CA", "date_posted": date, "is_remote": "false",
    }
    row.update(extra)
    return row


class TestObservationGrain:
    def test_the_same_posting_on_two_days_stays_two_observations(self, spark) -> None:
        # This is the whole reason the observation grain exists. Collapsing
        # here would destroy the answer to "was this open on that date".
        df = landed(spark, [observation("https://x/1", "2026-08-01"), observation("https://x/1", "2026-08-02")])
        assert build_observations(df).count() == 2

    def test_the_same_posting_from_two_sources_on_one_day_stays_two(self, spark) -> None:
        # Different sources are different sightings, and source_count on the
        # posting fact depends on both surviving.
        df = landed(spark, [
            observation("https://x/1", "2026-08-01", site="indeed"),
            observation("https://x/1", "2026-08-01", site="linkedin"),
        ])
        assert build_observations(df).count() == 2

    def test_an_exact_duplicate_triple_collapses_to_one(self, spark) -> None:
        # Same posting, same source, same day, recorded twice because a day ran
        # two overlapping scrapes. That is one sighting.
        df = landed(spark, [observation("https://x/1", "2026-08-01"), observation("https://x/1", "2026-08-01")])
        assert build_observations(df).count() == 1

    def test_dedup_picks_the_same_survivor_every_run(self, spark) -> None:
        # Without a deterministic order the row kept depends on scheduling, and
        # two runs over identical input would differ.
        df = landed(spark, [
            observation("https://x/1", "2026-08-01", title="First",
                        _source_file="jobspy-results-2026-08-01-local-indeed.csv"),
            observation("https://x/1", "2026-08-01", title="Second",
                        _source_file="jobspy-results-2026-08-01-a-indeed.csv"),
        ])
        titles = {build_observations(df).collect()[0]["title"] for _ in range(3)}
        assert titles == {"Second"}


class TestPostingSnapshot:
    def test_collapses_every_sighting_of_one_posting_to_one_row(self, spark) -> None:
        df = landed(spark, [observation("https://x/1", f"2026-08-0{d}") for d in (1, 2, 3)])
        postings = build_postings(build_observations(df))
        assert postings.count() == 1
        assert postings.collect()[0]["observation_count"] == 3

    def test_takes_attributes_from_the_last_sighting_not_the_first(self, spark) -> None:
        # A re-listed posting should read as its current self.
        df = landed(spark, [
            observation("https://x/1", "2026-08-01", title="Old title"),
            observation("https://x/1", "2026-08-05", title="New title"),
        ])
        row = build_postings(build_observations(df)).collect()[0]
        assert row["title"] == "New title"

    def test_records_the_span_it_was_visible_for(self, spark) -> None:
        df = landed(spark, [observation("https://x/1", "2026-08-01"), observation("https://x/1", "2026-08-05")])
        row = build_postings(build_observations(df)).collect()[0]
        assert row["first_seen_date"] == "2026-08-01"
        assert row["last_seen_date"] == "2026-08-05"
        assert row["days_visible"] == 5

    def test_a_posting_seen_on_one_day_is_visible_for_one_day_not_zero(self, spark) -> None:
        df = landed(spark, [observation("https://x/1", "2026-08-01")])
        assert build_postings(build_observations(df)).collect()[0]["days_visible"] == 1

    def test_counts_distinct_sources_not_sightings(self, spark) -> None:
        df = landed(spark, [
            observation("https://x/1", "2026-08-01", site="indeed"),
            observation("https://x/1", "2026-08-02", site="indeed"),
            observation("https://x/1", "2026-08-02", site="linkedin"),
        ])
        row = build_postings(build_observations(df)).collect()[0]
        assert row["source_count"] == 2
        assert row["observation_count"] == 2

    def test_a_posting_with_no_stated_date_still_gets_a_row(self, spark) -> None:
        # 8.7% of observations state no posting date. Dropping them would lose
        # one row in twelve from the warehouse entirely.
        df = landed(spark, [observation("https://x/1", "2026-08-01", date_posted="")])
        postings = build_postings(build_observations(df))
        assert postings.count() == 1
        assert postings.collect()[0]["date_posted"] is None
