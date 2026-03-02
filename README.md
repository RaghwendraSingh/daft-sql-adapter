# daft-sql-adapter

Run **Spark SQL** against **Databricks Unity Catalog** using **Daft on Ray**. Queries are transpiled from Spark SQL to PostgreSQL (Daft’s dialect) with SQLGlot, then executed in a Daft Session. Supports **SELECT** (paginated Arrow IPC) and **CREATE TABLE AS SELECT** (write to Delta or Iceberg).

## Features

- **Spark SQL input** – Write queries in Spark SQL; they are transpiled to PostgreSQL before execution.
- **Unity Catalog** – Connect via `DATABRICKS_HOST` and `DATABRICKS_TOKEN` (no secrets on the CLI).
- **Table list** – Pass full Unity table names (`catalog.schema.table`); they are loaded and registered for the session.
- **SELECT** – Returns a paginated result as **Arrow IPC** plus JSON metadata (page, page_size, total_count).
- **CREATE TABLE AS SELECT** – Executes the SELECT and writes the result to an external table in **Delta** or **Iceberg** (path or table identifier).

## Installation

```bash
pip install -e .
# Optional: Iceberg writes
pip install -e ".[iceberg]"
```

## Configuration (credentials)

Credentials are read from the environment or an optional config file. **Do not** pass tokens on the command line.

- **Environment variables**
  - `DATABRICKS_HOST` – e.g. `https://<workspace-id>.cloud.databricks.com`
  - `DATABRICKS_TOKEN` – Personal access token
- **Config file** – Set `DAFT_SQL_ADAPTER_CONFIG` to the path of a file containing:
  - `databricks_host=...`
  - `databricks_token=...`

## CLI

```bash
# SELECT: paginated Arrow IPC + metadata
daft-sql-adapter --sql "SELECT * FROM catalog.schema.my_table LIMIT 10" \
  --tables "catalog.schema.my_table" \
  --page 1 --page-size 100 \
  --output result.arrow --metadata result_meta.json

# CREATE TABLE AS SELECT: write to Delta
daft-sql-adapter --sql "CREATE TABLE catalog.schema.out AS SELECT * FROM catalog.schema.my_table" \
  --tables "catalog.schema.my_table" \
  --format delta --output-path "s3://bucket/path/out"

# CREATE TABLE AS SELECT: write to Iceberg (requires pyiceberg and catalog)
daft-sql-adapter --sql "CREATE TABLE ns.out AS SELECT * FROM catalog.schema.my_table" \
  --tables "catalog.schema.my_table" \
  --format iceberg --output-path "ns.out"
```

### Options

| Option | Description |
|-------|-------------|
| `--sql` | Spark SQL query (SELECT or CREATE TABLE AS SELECT). |
| `--tables` | Comma-separated full Unity table names. |
| `--page`, `--page-size` | Pagination for SELECT (default: 1, 1000). |
| `--output` | File path for Arrow IPC data (SELECT). |
| `--metadata` | File path for JSON metadata (SELECT). |
| `--format` | For CTAS: `delta` or `iceberg`. |
| `--output-path` | For CTAS: output path (Delta) or table identifier (Iceberg). Required if LOCATION is not in the SQL. |
| `--config` | Path to config file (sets `DAFT_SQL_ADAPTER_CONFIG`). |

## Library API

```python
from daft_sql_adapter import run_sql, CtasResult, SelectResult

# SELECT
result = run_sql(
    "SELECT * FROM catalog.schema.t LIMIT 10",
    table_names=["catalog.schema.t"],
    page=1,
    page_size=100,
)
assert isinstance(result, SelectResult)
# result.data: bytes (Arrow IPC)
# result.metadata: PageMetadata (page, page_size, total_count)

# CREATE TABLE AS SELECT
result = run_sql(
    "CREATE TABLE out AS SELECT * FROM catalog.schema.t",
    table_names=["catalog.schema.t"],
    output_format="delta",
    output_path="s3://bucket/path/out",
)
assert isinstance(result, CtasResult)
# result.status == 0, result.path_or_identifier, result.format
```

## Running on a Ray cluster

Submit jobs to a Ray cluster (e.g. KubeRay) using the job submission API. The job runs on the cluster; ensure the cluster image or runtime has this package and its dependencies installed.

**Job submission server address (example):** `http://kuberay-1.dev.meesho.int`

**Minimal Ray cluster check (no Daft):**
```bash
ray job submit --address http://kuberay-1.dev.meesho.int -- \
  python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

**Run daft-sql-adapter as a submitted job:**  
Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in the job environment (e.g. via your orchestration or `--runtime-env-json`). The driver and workers will use the cluster’s Ray; Daft will use the Ray backend automatically when running on the cluster.

```bash
# From a machine where the adapter is installed and RAY_ADDRESS is set
export RAY_ADDRESS="http://kuberay-1.dev.meesho.int"
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-token>"

ray job submit --address "$RAY_ADDRESS" -- \
  daft-sql-adapter --sql "SELECT 1 AS x" --tables "" --output /tmp/out.arrow --metadata /tmp/meta.json
```

A sample script is in [scripts/ray-job-submit.sh](scripts/ray-job-submit.sh).

## Project layout (SOLID-oriented)

- `config/` – Credentials (env + optional file); no secrets in logs.
- `sql/` – Transpile (Spark → Postgres), classify (CREATE_TABLE vs SELECT), parse CTAS.
- `catalog/` – Unity client factory, table loader into Session.
- `writers/` – TableWriter abstraction; Delta and Iceberg implementations.
- `pagination/` – Slice + Arrow IPC for SELECT.
- `ctas.py` – Execute CTAS: run SELECT, then write via TableWriter.
- `runner.py` – Orchestration: credentials → Unity → load tables → transpile → execute.
- `cli.py` – CLI entry point.

## License

See repository license.
