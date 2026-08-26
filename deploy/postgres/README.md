# PostgreSQL on g7

The warehouse database. One container, bound to the tailnet address only, data
on a named volume.

## Why here and not on the workstation

The transform runs Spark in local mode on the workstation because Spark is
memory-hungry. The database is the opposite shape: small, always-on, and it
needs to outlive any single load so a published view can read it. That is a g7
job, not a workstation one, and it keeps the workstation free of a standing
service.

## Deploy

`.env` is never committed. Create it next to this file on g7:

```
WAREHOUSE_DB_PASSWORD=<a strong password, stored in the password manager>
WAREHOUSE_DB_BIND=<g7 tailnet IP>:5432
```

Then:

```
docker compose --env-file .env up -d
```

`WAREHOUSE_DB_BIND` is an explicit `ADDRESS:PORT`. Binding to the tailnet
interface, not `0.0.0.0`, is what keeps the database off the LAN and the public
internet: the tailnet is the boundary, the same position as the Sentinel broker
and the vault MCP endpoint. There is no Cloudflare route to this container. The
tunnel later carries the read-only HTTP view, never the database port.

## Schema and first load

The schema and the load both live in the Python package, not here. From the repo
on the workstation, with the warehouse Parquet already built by the transform:

```
export BW_PG_DSN='postgresql://warehouse:<password>@<g7 tailnet IP>:5432/warehouse'
python -m warehouse.load --warehouse data/warehouse --init-schema
```

`--init-schema` is idempotent, so it is safe on every run. Omit `--ingest-date`
for a full replace; pass it to reload a single partition of the observation
fact.

## What never goes in this directory

The password, the DSN, and the tailnet address in any committed file. They live
in `.env` on g7 and in the password manager, and nowhere else.
