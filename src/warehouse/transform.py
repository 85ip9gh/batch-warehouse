"""The Spark transform: landed observations to a dimensional model.

Thin on purpose. Every conforming decision lives in `conform.py` as a pure
function and this module applies them, so the rules can be tested without a
session and this file only has to be right about shape.

Reads the landing partitions, writes Parquet for two facts and four dimensions.
It never reads the source CSVs and never writes back to landing: landing is
immutable and the transform is a consumer of it.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from warehouse.conform import conform_company, parse_amount, parse_location, parse_posted_date

LOCATION_SCHEMA = StructType(
    [
        StructField("location_key", StringType()),
        StructField("city", StringType()),
        StructField("province_code", StringType()),
        StructField("province", StringType()),
        StructField("country_code", StringType()),
        StructField("country", StringType()),
        StructField("resolved", BooleanType()),
    ]
)

COMPANY_SCHEMA = StructType(
    [StructField("company_key", StringType()), StructField("company_name", StringType())]
)


def ensure_worker_importable() -> str:
    """Put this package's parent directory on PYTHONPATH, and return it.

    A Spark UDF is pickled by reference, so the Python worker unpickles it by
    importing `warehouse.conform` for itself. The worker is a SUBPROCESS, and it
    inherits the environment and nothing else: `pytest.ini`'s `pythonpath`, an
    editable install's `.pth`, and any `sys.path` edit are all in-process and
    none of them reach it.

    Without this the worker dies with ModuleNotFoundError, and the failure does
    not say so. It surfaces as a task failure, and on Windows as the opaque
    "Python worker exited unexpectedly (crashed)". The cost of finding that out
    was two sessions, so it is fixed here rather than in a runbook: setting an
    environment variable at the call site is a step someone eventually forgets.

    Called before the session starts because workers inherit the environment as
    it was when the JVM launched them.
    """
    root = str(Path(__file__).resolve().parent.parent)
    existing = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    if root not in existing:
        os.environ["PYTHONPATH"] = os.pathsep.join([root, *existing])
    return root


def build_session(
    app_name: str = "batch-warehouse",
    master: str = "local[*]",
    shuffle_partitions: int = 8,
) -> SparkSession:
    """A local-mode session sized for a workstation.

    Local mode, not a cluster. The corpus is hundreds of megabytes and inflating
    that into a distributed deployment would be theatre. Shuffle partitions are
    cut from the default 200 because at this size the default spends more time
    scheduling empty tasks than doing work.

    The tests build their session through here too, on a narrower master, so the
    worker-path guarantee above is the same one production gets rather than a
    second arrangement that can drift.
    """
    ensure_worker_importable()
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@F.udf(returnType=COMPANY_SCHEMA)
def _company_udf(raw: str | None):
    key, name = conform_company(raw)
    return (key, name)


@F.udf(returnType=LOCATION_SCHEMA)
def _location_udf(raw: str | None):
    parsed = parse_location(raw)
    return (
        parsed.key,
        parsed.city,
        parsed.province_code,
        parsed.province,
        parsed.country_code,
        parsed.country,
        parsed.resolved,
    )


@F.udf(returnType=DoubleType())
def _amount_udf(raw: str | None):
    return parse_amount(raw)


@F.udf(returnType=StringType())
def _posted_date_udf(raw: str | None):
    return parse_posted_date(raw)


def read_landing(spark: SparkSession, landing_dir: Path) -> DataFrame:
    """Every landed partition, as one DataFrame.

    Read as text and parsed per line rather than with the JSON reader's schema
    inference. Inference samples, and a column that is absent from the sample
    and present later is silently dropped, which is exactly the failure a
    warehouse should never have.
    """
    pattern = str(landing_dir / "ingest_date=*" / "observations.ndjson.gz")
    raw = spark.read.text(pattern)
    if raw.rdd.isEmpty():
        raise ValueError(f"no landed partitions under {landing_dir}")
    fields = [
        "id", "site", "job_url", "title", "company", "location", "date_posted",
        "job_type", "is_remote", "min_amount", "max_amount", "currency", "interval",
        "_ingest_date", "_source_file",
    ]
    parsed = raw.select(
        *[F.get_json_object(F.col("value"), f"$.{name}").alias(name) for name in fields]
    )
    return parsed


def build_observations(landed: DataFrame) -> DataFrame:
    """`fact_posting_observation`: one row per posting, per source, per ingest date.

    The dedup here is deliberately narrow. Two rows sharing that triple are the
    same sighting recorded twice, usually because a day ran two overlapping
    scrapes of one source, and collapsing them is safe. Two rows sharing only
    the URL are two different sightings and both survive: that is the whole
    reason this grain exists.
    """
    conformed = (
        landed.withColumn("company_parts", _company_udf(F.col("company")))
        .withColumn("location_parts", _location_udf(F.col("location")))
        .select(
            F.col("_ingest_date").alias("ingest_date"),
            F.col("site").alias("source_key"),
            F.col("job_url"),
            F.col("title"),
            F.col("company_parts.company_key").alias("company_key"),
            F.col("location_parts.location_key").alias("location_key"),
            F.to_date(_posted_date_udf(F.col("date_posted"))).alias("date_posted"),
            F.col("job_type"),
            (F.lower(F.col("is_remote")) == "true").alias("is_remote"),
            _amount_udf(F.col("min_amount")).alias("salary_min"),
            _amount_udf(F.col("max_amount")).alias("salary_max"),
            F.col("currency"),
            F.col("interval").alias("salary_interval"),
            F.col("_source_file").alias("source_file"),
        )
    )
    # Ordered by source_file so the survivor of a duplicate triple is
    # deterministic. Without an order the row kept depends on partition
    # scheduling, and two runs over identical input would differ.
    window = Window.partitionBy("ingest_date", "source_key", "job_url").orderBy("source_file")
    return (
        conformed.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def build_postings(observations: DataFrame) -> DataFrame:
    """`fact_posting`: one row per distinct posting, an accumulating snapshot.

    Attributes are taken from the LAST observation rather than the first. A
    posting that is re-listed with a salary band or a corrected title should
    read as its current self, and first-seen would freeze whatever the earliest
    scrape happened to catch.
    """
    latest = Window.partitionBy("job_url").orderBy(F.col("ingest_date").desc())
    current = (
        observations.withColumn("_rank", F.row_number().over(latest))
        .filter(F.col("_rank") == 1)
        .select(
            "job_url", "title", "company_key", "location_key", "date_posted",
            "job_type", "is_remote", "salary_min", "salary_max", "currency",
            "salary_interval",
        )
    )
    spans = observations.groupBy("job_url").agg(
        F.min("ingest_date").alias("first_seen_date"),
        F.max("ingest_date").alias("last_seen_date"),
        F.countDistinct("ingest_date").alias("observation_count"),
        F.countDistinct("source_key").alias("source_count"),
    )
    return current.join(spans, on="job_url", how="inner").withColumn(
        # Scraper visibility, never "time to fill". Inclusive of both ends, so a
        # posting seen on one day only reads as 1 rather than 0.
        "days_visible",
        F.datediff(F.to_date("last_seen_date"), F.to_date("first_seen_date")) + 1,
    )


def build_dim_company(landed: DataFrame) -> DataFrame:
    """One row per conformed company.

    The display name is the most frequent raw spelling, not an arbitrary one. A
    dimension showing "CITY OF WATERLOO" because it sorted first, when every
    other posting writes "City of Waterloo", is a dimension nobody trusts.
    """
    parts = landed.withColumn("p", _company_udf(F.col("company"))).select(
        F.col("p.company_key").alias("company_key"),
        F.col("p.company_name").alias("company_name"),
    ).filter(F.col("company_key").isNotNull())

    counted = parts.groupBy("company_key", "company_name").agg(F.count("*").alias("n"))
    window = Window.partitionBy("company_key").orderBy(F.col("n").desc(), F.col("company_name"))
    display = (
        counted.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .select("company_key", "company_name")
    )
    variants = parts.groupBy("company_key").agg(
        F.countDistinct("company_name").alias("spelling_count")
    )
    return display.join(variants, on="company_key", how="inner")


def build_dim_location(landed: DataFrame) -> DataFrame:
    """One row per conformed location, including the ones that did not parse."""
    return (
        landed.withColumn("p", _location_udf(F.col("location")))
        .select(
            F.col("p.location_key").alias("location_key"),
            F.col("p.city").alias("city"),
            F.col("p.province_code").alias("province_code"),
            F.col("p.province").alias("province"),
            F.col("p.country_code").alias("country_code"),
            F.col("p.country").alias("country"),
            F.col("p.resolved").alias("resolved"),
            F.col("location").alias("raw_location"),
        )
        .filter(F.col("location_key").isNotNull())
        .dropDuplicates(["location_key"])
    )


def build_dim_source(landed: DataFrame) -> DataFrame:
    """One row per source site."""
    return (
        landed.select(F.col("site").alias("source_key"))
        .filter(F.col("source_key").isNotNull())
        .distinct()
        .withColumn(
            "source_type",
            F.when(F.col("source_key").startswith("company:"), F.lit("employer_ats")).otherwise(
                F.lit("job_board")
            ),
        )
        .withColumn(
            "platform",
            F.when(
                F.col("source_key").startswith("company:"),
                F.regexp_replace(F.col("source_key"), "^company:", ""),
            ).otherwise(F.col("source_key")),
        )
    )


def build_dim_date(observations: DataFrame) -> DataFrame:
    """One row per calendar date the warehouse refers to.

    Built from the dates actually present rather than from a generated calendar
    range. A date dimension padded with days nothing happened on invites a join
    that reports zero activity for a day the scraper simply did not run, which
    is a different statement.
    """
    dates = (
        observations.select(F.to_date("ingest_date").alias("date"))
        .union(observations.select(F.col("date_posted").alias("date")))
        .filter(F.col("date").isNotNull())
        .distinct()
    )
    return dates.select(
        F.date_format("date", "yyyy-MM-dd").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def write_table(df: DataFrame, warehouse_dir: Path, name: str) -> int:
    """Write one table as Parquet, replacing it whole.

    Overwrite rather than append. The transform is a pure function of the
    landing directory, so a rerun should produce the model that landing implies
    and not the model plus whatever a previous run left behind.
    """
    target = warehouse_dir / name
    df.write.mode("overwrite").parquet(str(target))
    return df.count()


def run(landing_dir: Path, warehouse_dir: Path, spark: SparkSession | None = None) -> dict[str, int]:
    owned = spark is None
    session = spark or build_session()
    try:
        landed = read_landing(session, landing_dir).cache()
        observations = build_observations(landed).cache()
        postings = build_postings(observations)
        counts = {
            "fact_posting_observation": write_table(observations, warehouse_dir, "fact_posting_observation"),
            "fact_posting": write_table(postings, warehouse_dir, "fact_posting"),
            "dim_company": write_table(build_dim_company(landed), warehouse_dir, "dim_company"),
            "dim_location": write_table(build_dim_location(landed), warehouse_dir, "dim_location"),
            "dim_source": write_table(build_dim_source(landed), warehouse_dir, "dim_source"),
            "dim_date": write_table(build_dim_date(observations), warehouse_dir, "dim_date"),
        }
        return counts
    finally:
        if owned:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing", required=True, type=Path)
    parser.add_argument("--warehouse", required=True, type=Path)
    args = parser.parse_args(argv)

    counts = run(args.landing, args.warehouse)
    width = max(len(name) for name in counts)
    for name, rows in counts.items():
        print(f"{name:<{width}}  {rows:>7} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
