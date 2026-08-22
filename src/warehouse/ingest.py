"""Land raw scrapes into immutable, date-partitioned files.

Landing does three things and refuses to do a fourth. It selects which files are
real scrapes, drops the columns that must never reach the warehouse, and writes
one partition per ingest date. It does not clean, conform, deduplicate or
interpret anything: those are the transform's job, and a landing step that
quietly repairs its input destroys the evidence of what the source actually
sent.

Immutability here means a landed partition is never appended to and never
edited. Re-landing a date rewrites that partition whole, atomically, and leaves
every other partition untouched.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

# A posting description runs to tens of kilobytes and the default field limit is
# 128 KB, which is close enough that a single verbose employer would abort a run.
csv.field_size_limit(1 << 30)

SCRAPE_GLOB = "jobspy-results-*.csv"

# Filenames carry meaning and the meaning decides what is real input.
#
# `-screened` files are this project's own earlier filtering. The `-combined`,
# `-shortlist`, `-extracted` and `-yearsbar` variants were written by
# re-processing the same day's per-source scrapes, so ingesting them counts the
# same posting several times. Measured across the corpus they add 20,858 rows
# and zero new URLs, which is what makes excluding them safe rather than merely
# convenient: they hold nothing the scrapes do not.
DERIVED_MARKERS = ("screened", "combined", "shortlist", "extracted", "yearsbar")

# Dropped at the boundary, not filtered downstream. `emails` is contact detail
# harvested out of posting bodies and belongs to people who never published it
# here, and the only way to be sure it never reaches the warehouse is for it
# never to be written down in the first place.
DROPPED_COLUMNS = ("emails",)

INGEST_DATE_RE = re.compile(r"jobspy-results-(\d{4}-\d{2}-\d{2})")


class IngestError(RuntimeError):
    """Raised when input cannot be landed without guessing."""


@dataclass(frozen=True)
class Partition:
    """What one landed ingest date contains."""

    ingest_date: str
    rows: int
    distinct_urls: int
    source_files: tuple[str, ...]
    sha256: str
    path: Path


def ingest_date_of(path: Path) -> str:
    """The scrape date encoded in a filename.

    Raises rather than defaulting to today. A file whose date cannot be read is
    a file whose partition cannot be known, and landing it under a guessed date
    would put rows in a partition that a later re-land of the real date would
    never correct.
    """
    match = INGEST_DATE_RE.search(path.name)
    if not match:
        raise IngestError(f"no ingest date in filename: {path.name}")
    return match.group(1)


def is_raw_scrape(path: Path) -> bool:
    """True for a per-source scrape, False for anything derived from one."""
    return not any(marker in path.name for marker in DERIVED_MARKERS)


def discover(source_dir: Path) -> list[Path]:
    """Raw scrape files under `source_dir`, sorted.

    Globbed by name rather than by listing the directory. The corpus lives
    beside unrelated personal material (contact exports, outreach drafts, a mail
    audit, screenshots naming real people), and an explicit pattern is what
    stops any of it being picked up by a future reorganisation of that folder.
    """
    return sorted(p for p in source_dir.glob(SCRAPE_GLOB) if is_raw_scrape(p))


def group_by_date(paths: Iterable[Path]) -> dict[str, list[Path]]:
    """Scrape files keyed by ingest date.

    One date routinely has several files, because a day's run scrapes each
    source separately. They land together in one partition.
    """
    grouped: dict[str, list[Path]] = {}
    for path in sorted(paths):
        grouped.setdefault(ingest_date_of(path), []).append(path)
    return grouped


def _read_rows(paths: Sequence[Path]) -> Iterator[dict[str, str]]:
    for path in paths:
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise IngestError(f"no header row: {path.name}")
            for row in reader:
                # csv.DictReader puts unmatched trailing fields under a None key
                # when a row has more columns than the header. Dropping it
                # silently would lose data; failing names the file.
                if None in row:
                    raise IngestError(f"row has more fields than the header: {path.name}")
                yield row


def _clean(row: dict[str, str], source_file: str, ingest_date: str) -> dict[str, str]:
    cleaned = {k: v for k, v in row.items() if k not in DROPPED_COLUMNS}
    # Lineage travels with the row. Without it a landed partition cannot answer
    # which scrape produced a given observation, and the transform has to infer
    # from the partition path, which stops being true the moment anything is
    # ever backfilled.
    cleaned["_ingest_date"] = ingest_date
    cleaned["_source_file"] = source_file
    return cleaned


def land_partition(ingest_date: str, paths: Sequence[Path], landing_dir: Path) -> Partition:
    """Write one ingest date's scrapes to a single immutable partition.

    The write is atomic: the body goes to a temporary file in the destination
    directory and is renamed into place only once it is complete. A run
    interrupted halfway leaves the previous partition intact rather than a
    truncated file that reads as a short day.
    """
    if not paths:
        raise IngestError(f"no source files for {ingest_date}")

    partition_dir = landing_dir / f"ingest_date={ingest_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    final = partition_dir / "observations.ndjson.gz"
    tmp = partition_dir / "observations.ndjson.gz.tmp"

    digest = hashlib.sha256()
    rows = 0
    urls: set[str] = set()

    try:
        # mtime=0 so the gzip header does not embed a timestamp. Two runs over
        # identical input then produce byte-identical files, which is what lets
        # a re-land be compared rather than merely trusted.
        with gzip.GzipFile(filename="", mode="wb", fileobj=tmp.open("wb"), mtime=0) as gz:
            for path in paths:
                for row in _read_rows([path]):
                    record = _clean(row, path.name, ingest_date)
                    line = json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                    body = line.encode("utf-8")
                    # The checksum covers the uncompressed body, so it
                    # identifies the data rather than the compression settings.
                    digest.update(body)
                    gz.write(body)
                    rows += 1
                    url = (row.get("job_url") or "").strip()
                    if url:
                        urls.add(url)
        os.replace(tmp, final)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    partition = Partition(
        ingest_date=ingest_date,
        rows=rows,
        distinct_urls=len(urls),
        source_files=tuple(p.name for p in paths),
        sha256=digest.hexdigest(),
        path=final,
    )
    _write_manifest(partition_dir, partition)
    return partition


def _write_manifest(partition_dir: Path, partition: Partition) -> None:
    manifest = {
        "ingest_date": partition.ingest_date,
        "rows": partition.rows,
        "distinct_job_urls": partition.distinct_urls,
        "source_files": list(partition.source_files),
        "dropped_columns": list(DROPPED_COLUMNS),
        "sha256": partition.sha256,
        "sha256_covers": "uncompressed NDJSON body",
    }
    tmp = partition_dir / "_manifest.json.tmp"
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, partition_dir / "_manifest.json")


def land_all(source_dir: Path, landing_dir: Path, only_date: str | None = None) -> list[Partition]:
    """Land every scrape date found under `source_dir`."""
    grouped = group_by_date(discover(source_dir))
    if only_date is not None:
        if only_date not in grouped:
            raise IngestError(f"no scrape files for {only_date}")
        grouped = {only_date: grouped[only_date]}
    return [land_partition(date, paths, landing_dir) for date, paths in sorted(grouped.items())]


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="directory holding the scrape CSVs")
    parser.add_argument("--landing", required=True, type=Path, help="landing root to write partitions under")
    parser.add_argument("--date", default=None, help="land only this ingest date (YYYY-MM-DD)")
    args = parser.parse_args(argv)

    partitions = land_all(args.source, args.landing, args.date)
    for partition in partitions:
        print(
            f"{partition.ingest_date}  {partition.rows:>6} rows  "
            f"{partition.distinct_urls:>6} urls  {len(partition.source_files)} files"
        )
    total = sum(p.rows for p in partitions)
    print(f"{len(partitions)} partitions, {total} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
