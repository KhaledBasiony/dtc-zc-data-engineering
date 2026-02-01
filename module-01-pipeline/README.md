# Module 01 — ETL Pipeline

An ETL pipeline that downloads [NYC TLC Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) trip data and loads it into Apache Cassandra.

> **Note:** This is not a strict follow-along of the course. I mixed things up on purpose: **Cassandra** instead of PostgreSQL, **Typer** instead of Click for the CLI, plus **additional cli options**, **simple tests**, **caching**, and **async** downloads. Same ideas, different stack.

## Overview

- **Extract** — Downloads CSV.gz files from the [DataTalksClub NYC TLC data releases](https://github.com/DataTalksClub/nyc-tlc-data/releases) (yellow taxi, years 2019–2021).
- **Transform** — Reads CSV in chunks with configurable schema and date parsing.
- **Load** — Creates a keyspace and table in Cassandra (if needed) and inserts rows with a generated UUID primary key.

Downloads are cached locally so repeated runs skip files that are already present.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose  
  **or**
- Python 3.11+ with [uv](https://docs.astral.sh/uv/) and a running Cassandra instance

## Running with Docker Compose

All commands in this README assume you are in the `module-01-pipeline` directory (`cd module-01-pipeline` from the repo root).

```bash
# Start Cassandra and the ETL service
docker compose up -d

# Run the pipeline (e.g. 2021, January, 5,000 rows per file)
docker compose run --rm etl --years 2021 --months 1 --num-rows 5000

# Stop services
docker compose down
```

The ETL container connects to the `cassandra` service. Cassandra data is stored in `./db`; the download cache is in `./cache`.

## Pipeline CLI

The pipeline is a [Typer](https://typer.tiangolo.com/) app. Ensure dependencies are installed (run `uv sync` from the repo root once), then from `module-01-pipeline`:

```bash
uv run pipeline.py --help
```

| Option          | Default | Description |
|-----------------|---------|-------------|
| `--years`       | `2021`  | Years to import. Formats: `2021`, `2019,2021`, `2019-2021`. |
| `--months`      | `1`     | Months per year. Same formats (values 1–12). |
| `--chunck-size` | `1000`  | Rows per in-memory chunk when reading CSV. |
| `--num-rows`    | `10000` | Max rows to read from each file (for quick runs). |

Examples:

```bash
# Single year and month
uv run pipeline.py --years 2021 --months 1

# Range of months
uv run pipeline.py --years 2021 --months 1-3

# Multiple years, limit rows per file
uv run pipeline.py --years 2019,2020 --months 1 --num-rows 2000
```

## Environment Variables

| Variable             | Default     | Description |
|----------------------|-------------|--------------|
| `CASSANDRA_HOST`     | `127.0.0.1` | Cassandra host. |
| `CASSANDRA_PORT`     | `9042`      | Cassandra native transport port. |
| `CASSANDRA_KEYSPACE` | `dev`       | Keyspace to create and use. |
| `CASSANDRA_USERNAME` | —           | Optional; enables authentication. |
| `CASSANDRA_PASSWORD` | —           | Optional; used with `CASSANDRA_USERNAME`. |
| `TABLE_NAME`         | `yellow`    | Table name for trip data. |

Docker Compose sets `CASSANDRA_HOST=cassandra` and `CASSANDRA_PORT=9042` for the ETL service.

## Project Structure

```
module-01-pipeline/
├── README.md
├── docker-compose.yml   # Cassandra + ETL services
├── Dockerfile           # ETL image (build context: repo root)
├── pipeline.py          # ETL entrypoint and CLI
├── draft.ipynb          # Exploratory notebook
└── test_main.py         # Tests
```

The Dockerfile uses the repository root as build context so the image can use the shared `pyproject.toml` and `.python-version`.

## Tests

From `module-01-pipeline`:

```bash
uv run pytest
```
