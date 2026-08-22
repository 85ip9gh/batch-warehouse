"""Ingest tests.

Every one runs with no Spark, no PostgreSQL, no network and no container
runtime, on fixtures written by the test itself. Sentinel proved the value of
that discipline: tests that need infrastructure get run once and then skipped,
and a suite nobody runs is documentation with a green badge.
"""

from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

import pytest

from warehouse.ingest import (
    DROPPED_COLUMNS,
    IngestError,
    discover,
    group_by_date,
    ingest_date_of,
    is_raw_scrape,
    land_all,
    land_partition,
)

HEADER = ["id", "site", "job_url", "title", "company", "location", "emails", "description"]


def write_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in HEADER})
    return path


def posting(url: str, **overrides: str) -> dict[str, str]:
    row = {
        "id": f"id-{url[-1]}",
        "site": "indeed",
        "job_url": url,
        "title": "Software Engineer",
        "company": "Acme",
        "location": "Halifax, NS",
        "emails": "recruiter@example.com",
        "description": "Line one\nLine two, with a comma",
    }
    row.update(overrides)
    return row


def read_landed(partition_path: Path) -> list[dict[str, str]]:
    with gzip.open(partition_path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


class TestFileSelection:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("jobspy-results-2026-07-20.csv", "2026-07-20"),
            ("jobspy-results-2026-08-03-local.csv", "2026-08-03"),
            ("jobspy-results-2026-08-10-local-careerbeacon.csv", "2026-08-10"),
            ("jobspy-results-2026-08-04-remote-indeed.csv", "2026-08-04"),
            ("jobspy-results-2026-08-06-national-linkedin.csv", "2026-08-06"),
        ],
    )
    def test_reads_the_date_out_of_every_real_filename_shape(self, name: str, expected: str) -> None:
        assert ingest_date_of(Path(name)) == expected

    def test_refuses_a_filename_with_no_date_rather_than_guessing_one(self) -> None:
        # Defaulting to today would file rows under a partition that a later
        # re-land of the true date could never correct.
        with pytest.raises(IngestError, match="no ingest date"):
            ingest_date_of(Path("jobspy-results-latest.csv"))

    @pytest.mark.parametrize(
        "name",
        [
            "jobspy-results-2026-08-03-local-combined.csv",
            "jobspy-results-2026-08-03-local-combined-shortlist.csv",
            "jobspy-results-2026-08-03-local-combined-extracted.csv",
            "jobspy-results-2026-07-24-screened.csv",
            "jobspy-results-2026-08-11-local-combined-shortlist-yearsbar.csv",
        ],
    )
    def test_rejects_every_derived_variant(self, name: str) -> None:
        # These are re-processings of the same day's scrapes. Landing them
        # counts the same posting more than once.
        assert is_raw_scrape(Path(name)) is False

    def test_accepts_the_per_source_scrapes(self) -> None:
        for name in (
            "jobspy-results-2026-07-20.csv",
            "jobspy-results-2026-08-03-local-indeed.csv",
            "jobspy-results-2026-08-06-national-companies.csv",
        ):
            assert is_raw_scrape(Path(name)) is True

    def test_discover_ignores_unrelated_files_in_the_same_directory(self, tmp_path: Path) -> None:
        # The real corpus sits beside contact exports and outreach drafts. The
        # glob, not the directory listing, is what keeps them out.
        write_csv(tmp_path / "jobspy-results-2026-07-20-local-indeed.csv", [posting("https://x/1")])
        write_csv(tmp_path / "jobspy-results-2026-07-20-local-combined.csv", [posting("https://x/1")])
        (tmp_path / "linkedin-connections.csv").write_text("name,company\nA,B\n", encoding="utf-8")
        (tmp_path / "referral-drafts-2026-07-22.md").write_text("draft", encoding="utf-8")

        found = [p.name for p in discover(tmp_path)]

        assert found == ["jobspy-results-2026-07-20-local-indeed.csv"]

    def test_groups_a_days_several_source_files_into_one_date(self, tmp_path: Path) -> None:
        for source in ("indeed", "linkedin", "careerbeacon"):
            write_csv(tmp_path / f"jobspy-results-2026-08-10-local-{source}.csv", [posting("https://x/1")])
        write_csv(tmp_path / "jobspy-results-2026-08-11-local-indeed.csv", [posting("https://x/2")])

        grouped = group_by_date(discover(tmp_path))

        assert sorted(grouped) == ["2026-08-10", "2026-08-11"]
        assert len(grouped["2026-08-10"]) == 3


class TestLanding:
    def test_drops_the_email_column_so_it_never_reaches_the_warehouse(self, tmp_path: Path) -> None:
        source = write_csv(
            tmp_path / "jobspy-results-2026-07-20-local-indeed.csv",
            [posting("https://x/1", emails="someone@example.com")],
        )

        partition = land_partition("2026-07-20", [source], tmp_path / "landing")

        landed = read_landed(partition.path)
        assert landed, "expected a landed row"
        for column in DROPPED_COLUMNS:
            assert column not in landed[0]
        body = partition.path.read_bytes()
        assert b"someone@example.com" not in gzip.decompress(body)

    def test_keeps_everything_else_including_newlines_inside_a_description(self, tmp_path: Path) -> None:
        source = write_csv(tmp_path / "jobspy-results-2026-07-20-local-indeed.csv", [posting("https://x/1")])

        partition = land_partition("2026-07-20", [source], tmp_path / "landing")

        landed = read_landed(partition.path)[0]
        assert landed["description"] == "Line one\nLine two, with a comma"
        assert landed["job_url"] == "https://x/1"

    def test_stamps_lineage_onto_every_row(self, tmp_path: Path) -> None:
        source = write_csv(tmp_path / "jobspy-results-2026-07-20-local-indeed.csv", [posting("https://x/1")])

        partition = land_partition("2026-07-20", [source], tmp_path / "landing")

        landed = read_landed(partition.path)[0]
        assert landed["_ingest_date"] == "2026-07-20"
        assert landed["_source_file"] == "jobspy-results-2026-07-20-local-indeed.csv"

    def test_merges_a_days_sources_into_one_partition(self, tmp_path: Path) -> None:
        first = write_csv(tmp_path / "jobspy-results-2026-07-20-local-indeed.csv", [posting("https://x/1")])
        second = write_csv(
            tmp_path / "jobspy-results-2026-07-20-local-linkedin.csv",
            [posting("https://x/2", site="linkedin")],
        )

        partition = land_partition("2026-07-20", [first, second], tmp_path / "landing")

        assert partition.rows == 2
        assert partition.distinct_urls == 2
        assert partition.source_files == (first.name, second.name)

    def test_counts_distinct_urls_not_rows(self, tmp_path: Path) -> None:
        # The same posting can appear twice within one day when two sources
        # carry it. The partition records both facts separately.
        source = write_csv(
            tmp_path / "jobspy-results-2026-07-20-local-indeed.csv",
            [posting("https://x/1"), posting("https://x/1", site="linkedin")],
        )

        partition = land_partition("2026-07-20", [source], tmp_path / "landing")

        assert partition.rows == 2
        assert partition.distinct_urls == 1

    def test_writes_a_manifest_beside_the_partition(self, tmp_path: Path) -> None:
        source = write_csv(tmp_path / "jobspy-results-2026-07-20-local-indeed.csv", [posting("https://x/1")])

        partition = land_partition("2026-07-20", [source], tmp_path / "landing")

        manifest = json.loads((partition.path.parent / "_manifest.json").read_text(encoding="utf-8"))
        assert manifest["ingest_date"] == "2026-07-20"
        assert manifest["rows"] == 1
        assert manifest["dropped_columns"] == list(DROPPED_COLUMNS)
        assert manifest["sha256"] == partition.sha256
        assert manifest["sha256_covers"] == "uncompressed NDJSON body"

    def test_refuses_a_file_whose_rows_are_wider_than_its_header(self, tmp_path: Path) -> None:
        # A silent drop here would lose real columns. Failing names the file.
        path = tmp_path / "jobspy-results-2026-07-20-local-indeed.csv"
        path.write_text("id,title\n1,Engineer,surprise\n", encoding="utf-8")

        with pytest.raises(IngestError, match="more fields than the header"):
            land_partition("2026-07-20", [path], tmp_path / "landing")


class TestIdempotency:
    def test_re_landing_a_date_reproduces_it_byte_for_byte(self, tmp_path: Path) -> None:
        source = write_csv(
            tmp_path / "jobspy-results-2026-07-20-local-indeed.csv",
            [posting("https://x/1"), posting("https://x/2")],
        )
        landing = tmp_path / "landing"

        first = land_partition("2026-07-20", [source], landing)
        first_bytes = first.path.read_bytes()
        second = land_partition("2026-07-20", [source], landing)

        assert second.sha256 == first.sha256
        assert second.rows == first.rows
        # Byte equality, not just row equality. A partition that reproduces its
        # contents but not its bytes cannot be compared by checksum downstream.
        assert second.path.read_bytes() == first_bytes

    def test_re_landing_one_date_leaves_every_other_partition_untouched(self, tmp_path: Path) -> None:
        july = write_csv(tmp_path / "jobspy-results-2026-07-20-local-indeed.csv", [posting("https://x/1")])
        august = write_csv(tmp_path / "jobspy-results-2026-08-01-local-indeed.csv", [posting("https://x/2")])
        landing = tmp_path / "landing"
        land_all(tmp_path, landing)
        untouched = (landing / "ingest_date=2026-08-01" / "observations.ndjson.gz").read_bytes()

        write_csv(july, [posting("https://x/1"), posting("https://x/3")])
        land_all(tmp_path, landing, only_date="2026-07-20")

        assert (landing / "ingest_date=2026-08-01" / "observations.ndjson.gz").read_bytes() == untouched
        assert len(read_landed(landing / "ingest_date=2026-07-20" / "observations.ndjson.gz")) == 2
        assert august.exists()

    def test_a_failed_land_leaves_the_previous_partition_intact(self, tmp_path: Path) -> None:
        good = write_csv(tmp_path / "jobspy-results-2026-07-20-local-indeed.csv", [posting("https://x/1")])
        landing = tmp_path / "landing"
        first = land_partition("2026-07-20", [good], landing)
        before = first.path.read_bytes()

        broken = tmp_path / "jobspy-results-2026-07-20-local-linkedin.csv"
        broken.write_text("id,title\n1,Engineer,surprise\n", encoding="utf-8")
        with pytest.raises(IngestError):
            land_partition("2026-07-20", [good, broken], landing)

        # The half-written body was discarded rather than renamed over a good
        # partition, so an interrupted run reads as the previous day's data
        # rather than as a short day.
        assert first.path.read_bytes() == before
        assert not (first.path.parent / "observations.ndjson.gz.tmp").exists()

    def test_land_all_refuses_a_date_it_has_no_files_for(self, tmp_path: Path) -> None:
        write_csv(tmp_path / "jobspy-results-2026-07-20-local-indeed.csv", [posting("https://x/1")])

        with pytest.raises(IngestError, match="no scrape files"):
            land_all(tmp_path, tmp_path / "landing", only_date="2026-07-21")
