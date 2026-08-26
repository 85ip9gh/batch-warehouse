-- The dimensional model as PostgreSQL DDL.
--
-- One schema, six tables: two facts and four dimensions. The grain of every
-- fact is stated in README.md and restated on the table here, because the DDL
-- is the first thing a reader inspects and a grain that lives only in a markdown
-- file is a grain nobody reads. Every column that carries a caveat carries it
-- as a COMMENT, so the warning travels with the schema into psql's \d+ and any
-- catalog-reading tool rather than depending on someone finding this file.
--
-- Applying this script is idempotent: CREATE ... IF NOT EXISTS and COMMENT both
-- tolerate a rerun, so `--init-schema` is safe to pass on every load.

CREATE SCHEMA IF NOT EXISTS wh;

-- Dimensions first: the facts carry foreign keys into them.

CREATE TABLE IF NOT EXISTS wh.dim_company (
    company_key    text PRIMARY KEY,
    company_name   text   NOT NULL,
    spelling_count bigint NOT NULL
);
COMMENT ON TABLE  wh.dim_company IS 'One row per conformed company. The key folds case, punctuation and legal-form suffix; the name is the most frequent raw spelling, not an arbitrary one.';
COMMENT ON COLUMN wh.dim_company.spelling_count IS 'Distinct raw spellings that fold to this key. 1 means the employer was written one way in every posting.';

CREATE TABLE IF NOT EXISTS wh.dim_location (
    location_key  text PRIMARY KEY,
    city          text,
    province_code text,
    province      text,
    country_code  text,
    country       text,
    resolved      boolean NOT NULL,
    raw_location  text
);
COMMENT ON TABLE  wh.dim_location IS 'One row per conformed (city, province, country). An unresolved string keeps a row of its own keyed on its raw text rather than being dropped or folded into one unknown bucket.';
COMMENT ON COLUMN wh.dim_location.resolved IS 'False when the string could not be parsed to at least a province or a country. Such a row keys on its raw form and stays auditable.';

CREATE TABLE IF NOT EXISTS wh.dim_source (
    source_key  text PRIMARY KEY,
    source_type text,
    platform    text
);
COMMENT ON TABLE  wh.dim_source IS 'One row per source site. source_type splits job boards from employer ATS feeds.';

CREATE TABLE IF NOT EXISTS wh.dim_date (
    date_key   text PRIMARY KEY,
    date       date    NOT NULL,
    year       integer NOT NULL,
    month      integer NOT NULL,
    day        integer NOT NULL,
    day_name   text    NOT NULL,
    iso_week   integer NOT NULL,
    is_weekend boolean NOT NULL
);
COMMENT ON TABLE wh.dim_date IS 'One row per calendar date actually referenced, built from the dates present rather than a padded range so a join cannot report activity for a day nothing happened on.';

-- Facts second. Foreign keys reference the dimensions above.

CREATE TABLE IF NOT EXISTS wh.fact_posting_observation (
    ingest_date     date NOT NULL,
    source_key      text NOT NULL REFERENCES wh.dim_source(source_key),
    job_url         text NOT NULL,
    title           text,
    company_key     text REFERENCES wh.dim_company(company_key),
    location_key    text REFERENCES wh.dim_location(location_key),
    date_posted     date,
    job_type        text,
    is_remote       boolean,
    salary_min      double precision,
    salary_max      double precision,
    currency        text,
    salary_interval text,
    source_file     text,
    PRIMARY KEY (ingest_date, source_key, job_url)
);
COMMENT ON TABLE  wh.fact_posting_observation IS 'One row per posting, per source, per ingest date on which a scrape saw it. The immutable landing fact; reloading an ingest date replaces exactly that partition and no other.';
COMMENT ON COLUMN wh.fact_posting_observation.company_key IS 'NULL when the posting stated no company. A blank company is not folded onto an empty key.';
COMMENT ON COLUMN wh.fact_posting_observation.salary_min IS 'Salary as observed on this date. NULL means not stated, never 0: a posting that pays nothing differs from one that did not say.';

CREATE TABLE IF NOT EXISTS wh.fact_posting (
    job_url           text PRIMARY KEY,
    title             text,
    company_key       text REFERENCES wh.dim_company(company_key),
    location_key      text REFERENCES wh.dim_location(location_key),
    date_posted       date,
    job_type          text,
    is_remote         boolean,
    salary_min        double precision,
    salary_max        double precision,
    currency          text,
    salary_interval   text,
    first_seen_date   date   NOT NULL,
    last_seen_date    date   NOT NULL,
    observation_count bigint NOT NULL,
    source_count      bigint NOT NULL,
    days_visible      integer NOT NULL
);
COMMENT ON TABLE  wh.fact_posting IS 'One row per distinct posting, an accumulating snapshot derived from the observation fact. Attributes are taken from the last sighting, so a re-listed posting reads as its current self.';
COMMENT ON COLUMN wh.fact_posting.days_visible IS 'Days from first to last sighting, inclusive, so a posting seen once reads as 1. Scraper visibility only, never time-to-fill: a day the scrape did not run makes a posting look shorter than it was.';
