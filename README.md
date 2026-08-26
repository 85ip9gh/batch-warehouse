# Batch Warehouse

Working name. A scheduled batch pipeline that lands a dimensional model of the
Canadian tech job-posting market in PostgreSQL: immutable partitioned landing,
a Spark transform that conforms and deduplicates, an idempotent load, and one
query tuned against a recorded plan.

**Status: landing, the Spark transform, and the PostgreSQL load are built.**
Orchestration, one tuned query, and the published read-only view are next. The
grain was written before any transform, on purpose: it is the decision
everything downstream inherits, and writing it afterwards means writing down
whatever the transform happened to do.

## The data

47 daily scrapes of Canadian tech postings, 143 MB, collected 2026-07-20 to
2026-08-22 across nine sources: Indeed, LinkedIn, CareerBeacon, and six employer
applicant-tracking systems (Workday, SuccessFactors, Phenom, Greenhouse, Ashby,
BambooHR).

Profiled before modelling, not after:

| measure | value |
|---|---|
| observation rows across all scrapes | 24,636 |
| distinct postings | 18,969 |
| observations per posting | 1.3 |
| distinct company strings | 6,007 |
| distinct title strings | 11,585 |
| distinct location strings | 1,818 |
| rows carrying a third-party email | 3,726 |

## Only the raw scrapes are ingested, and that was measured rather than assumed

The source directory holds 74 non-screened CSVs and 27 of them are derived:
earlier work wrote `-combined`, `-shortlist` and `-extracted` files by
re-processing that day's per-source scrapes. Ingesting those would count the
same posting several times over.

**They add 20,858 rows and exactly zero new URLs.** That is the check that
justifies dropping them: they hold nothing the 47 raw scrapes do not, so
excluding them costs no coverage.

An earlier draft of this file quoted 24,636 as 45,494 and the duplication ratio
as 2.4. Both were that double counting rather than a property of the data, and
both were caught by reading the filenames instead of trusting a glob.

## The key is `job_url`, not `id`

The scraper emits an `id` column. On the raw scrapes it is nearly clean: zero
blanks and 18,966 distinct values. It is still not the key.

**Three ids cover more than one URL.** Some are synthesised from company and
title, so distinct vacancies collide:
`cb-successfactors-scotiabank-software-engineer` covers both a Toronto and a
Scarborough posting at different URLs. Three today, and the failure mode is
silently merging real vacancies, which is the kind that never announces itself.

`job_url` is blank on **zero** rows, gives 18,969 distinct postings, and matches
the distinct `(id, url)` pair count exactly.

**The derived files are far worse on this, which is a second reason to skip
them.** Across the full non-screened set, 4,486 rows carry a blank `id` and
3,309 URLs appear under more than one. None of those defects exists in the
scrapes; every one is introduced by the re-processing. A pipeline reading the
derived files would inherit all of it.

## 1.3 is smaller than it looks, and two facts are still right

About a third of observations are repeats: a posting rescraped on a later day it
was still open. That is a modest ratio and on its own it would not justify two
fact tables.

What justifies them is the question the observation grain can answer and a
collapsed table cannot: **was this posting open on this date.** Days listed,
weekly churn, and how long a market keeps a vacancy up all live at that grain.
Collapsing first would destroy them permanently, and the storage saved would be
trivial.

## The grain

Written before any transform. Every fact table states its grain in one sentence,
and if a sentence needs the word "and" more than once the table is doing two
jobs.

### `fact_posting_observation`

> **One row per posting, per source, per ingest date on which a scrape saw it.**

The immutable landing fact, roughly 24,636 rows today and growing by one scrape
a day. Nothing is updated in place: reloading an ingest date replaces exactly
that partition and no other, which is what makes the idempotency claim testable
rather than asserted.

Degenerate dimension: `job_url`. Measures: `salary_min` and `salary_max` as
observed on that date, because a posting can be re-listed at a different band
and the observation grain is the only place that change stays visible.

### `fact_posting`

> **One row per distinct posting.**

An accumulating snapshot, 18,969 rows today, derived entirely from the
observation fact. This is where the repeats collapse.

Measures: `first_seen_date`, `last_seen_date`, `observation_count`,
`days_listed`. The last is the interesting one and it is an estimate rather than
a truth: it measures how long a posting was **visible to this scraper**, not how
long it was open, and a day the scrape did not run makes a posting look shorter
than it was. That caveat belongs in the column comment, not only here.

### Dimensions

| table | grain |
|---|---|
| `dim_company` | one row per conformed company |
| `dim_location` | one row per conformed (city, province, country) |
| `dim_source` | one row per source site |
| `dim_date` | one row per calendar date |

`dim_company` is the one with real work in it. 6,007 raw strings, 445
observations with a blank company, and the same employer appearing under several
spellings across sources. Conforming those is the modelling claim this project
is actually making.

Title normalisation is deliberately **not** a dimension yet. 11,585 raw titles
resolve into role families only with judgement, and a half-built role dimension
that quietly mislabels a third of the corpus is worse than an unnormalised
attribute on the fact. It stays a text attribute until the mapping is evidenced.

## What is not in this repository

**The raw corpus is not committed and never will be.** Three independent
reasons, each sufficient on its own:

1. **The posting text is not mine to republish.** The `description` column holds
   full employer-authored job descriptions scraped from job boards. Building a
   pipeline over them is one thing; mirroring thousands of them into a public
   repository is another.
2. **3,726 rows carry a third-party email address** in the `emails` column,
   harvested from posting bodies. That is personal data belonging to people who
   never published it here. It is dropped at ingest rather than filtered later,
   so it never enters the warehouse at all.
3. 143 MB does not belong in git regardless.

Published instead: the pipeline, the schema, the tests, the recorded query
plans, and aggregate outputs that describe the market without reproducing any
posting or contact detail.

The source directory also holds unrelated personal material that this pipeline
never touches: contact exports, outreach and referral drafts, a mail audit, and
screenshots naming real people. Ingest reads `jobspy-results-*.csv` by explicit
glob and then excludes the derived variants, rather than reading a directory, so
nothing else can be picked up by accident.

## Running it

Two suites, and the split is deliberate.

**The fast suite needs nothing at all.** 86 tests over ingest and the conforming
rules: no Spark, no JVM, no network, no container runtime, well under a second.

```
pip install --requirement requirements-dev.txt
python -m pytest -q
```

**The Spark-layer tests need pyspark and a JVM.** Ten tests covering what unit
tests cannot reach: the observation grain, the dedup survivor, and the posting
snapshot.

```
pip install --requirement requirements-dev.txt --requirement requirements-spark.txt
BW_REQUIRE_SPARK=1 python -m pytest tests/test_transform.py -q
```

`BW_REQUIRE_SPARK=1` turns the module's skip into a failure. Leave it unset on a
laptop without pyspark and the module skips, which is the point of the skip. CI
sets it, because a suite that quietly skips itself when a dependency goes
missing reports green for a gate that never ran.

### A UDF runs in a subprocess, and a subprocess inherits the environment only

Spark pickles a UDF by reference, so the Python worker imports
`warehouse.conform` for itself in a separate process. `pytest.ini`'s `pythonpath`,
an editable install's `.pth`, and any `sys.path` edit are all in-process, and
none of them reach that worker. `build_session` therefore puts the source root
on `PYTHONPATH` before the session starts.

Without it the worker dies with `ModuleNotFoundError` and does not say so. It
surfaces as a task failure, and on Windows as "Python worker exited unexpectedly
(crashed)", which reads like a platform problem and is not one. That misreading
cost two sessions and left these tests recorded as never having passed, when in
fact they had never been given an importable module.

### Running the transform itself

Writing Parquet on Windows needs Hadoop native binaries, and the usual remedy is
an unofficial `winutils.exe` build from a third-party repository. The official
image needs none of it, so the transform runs there. The tests do not write, so
they run anywhere.

```
docker run --rm -v "$PWD:/work" -w /work \
  -e PYTHONPATH=/work/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
  apache/spark:4.1.3-scala2.13-java21-python3-ubuntu \
  python3 -m warehouse.transform --landing data/landing --warehouse data/warehouse
```

Reading the landing glob logs a `FileNotFoundException` stack trace at WARN
before it succeeds. Spark probes the literal glob string for streaming-sink
metadata, does not find a file by that name, and says so loudly. The read then
resolves the glob and works. It is noise, not a failure.

## The load

`warehouse.load` mirrors the Parquet warehouse into PostgreSQL, and it is the
point where the idempotency promise becomes testable. The schema is six tables
in a `wh` schema, two facts and four dimensions, with the grain of each fact
restated as a table comment and every caveat that lives in this file also living
as a column comment, so `\d+` in psql tells the same story this README does.

The model has real foreign keys: every `company_key`, `location_key` and
`source_key` on a fact references its dimension. A test loads a fact row against
a source that is not in `dim_source` and asserts the database rejects it, so the
keys are load-bearing rather than decorative.

Two load modes, and the difference is the immutable-partition promise:

- A **full** load truncates and refills every table from the current warehouse
  in one transaction, so a posting that has left the corpus leaves the warehouse
  too.
- A **partition** load (`--ingest-date`) replaces exactly one ingest date in the
  observation fact and nothing else. Reloading a date rewrites that date and no
  other, which the load-tests job proves by loading a partition twice and
  asserting the row count does not move.

Neither `psycopg` nor `pyarrow` is imported until the load runs, so the
SQL-shaping is unit-tested with no database in the fast suite, and the
idempotency is tested against a Postgres service container in a separate job
gated by `BW_REQUIRE_PG=1`, the same way the Spark layer is gated.

```
export BW_PG_DSN='postgresql://warehouse:<password>@<g7 tailnet IP>:5432/warehouse'
python -m warehouse.load --warehouse data/warehouse --init-schema
```

The DSN carries a password, so it is read from the environment and never written
into this repository or committed anywhere. Standing the database up on g7 is in
`deploy/postgres/`.

## Open decisions

- ~~**Spark or DuckDB.**~~ **Settled 2026-08-22: Spark, in local mode.** DuckDB
  was the alternative and is closed off unless Spark proves unworkable, which it
  has not: the transform builds the full model in about 22 seconds. Recorded
  here rather than drifted into, which is what the open version of this entry
  asked for.
- **The name.** Working title only.

## What must never be claimed

- No availability or uptime language for either host.
- Not "Spark at scale" and not "big data". This is a single-node local-mode job
  over a corpus measured in hundreds of megabytes, and inflating it is the claim
  most likely to collapse under one interview question.
- `days_listed` is scraper visibility, never "time to fill".
