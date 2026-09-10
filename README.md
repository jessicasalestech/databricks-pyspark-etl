# Databricks PySpark · Medallion ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Compatible-FF3621?logo=databricks&logoColor=white)
![CI](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue)

A professional portfolio project demonstrating a **Databricks + PySpark**
medallion (**bronze → silver → gold**) data pipeline with reusable, unit-tested
transformation functions and a green CI pipeline.

> **Owner:** Jessica Sales — QA / Software Engineer
> Everything in this repo is versioned in English.

---

## What this project demonstrates

- **Medallion (multi-hop) architecture** — clean separation of concerns across
  Bronze, Silver and Gold layers.
- **Reusable, pure-ish PySpark functions** in an `etl` package that are easy to
  unit-test against a local SparkSession.
- **Test-driven ETL logic** — 10 pytest tests covering NULL handling,
  deduplication, schema enforcement, aggregation correctness and typing.
- **Databricks notebooks** as `.py` source that mirror AWS Glue / Databricks
  jobs and chain Bronze → Silver → Gold.
- **Continuous Integration** — a GitHub Actions job that installs PySpark on
  JDK 17 and runs every test.

---

## Medallion architecture

```
┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
│      BRONZE          │   │      SILVER          │   │       GOLD          │
│  (landing / raw)     │──▶│  (clean / conformed) │──▶│  (aggregated / BI)  │
│                      │   │                      │   │                      │
│ • raw CSV / Parquet  │   │ • trim / lowercase   │   │ • revenue per order │
│ • source audit cols  │   │ • NULL handling      │   │ • revenue bycategory│
│ • permissive read    │   │ • dedupe on order_id │   │ • revenue by region │
│ • + ingested_at time │   │ • schema enforcement │   │ • only active orders│
└──────────────────────┘   └──────────────────────┘   └──────────────────────┘
        notebooks/               notebooks/                 notebooks/
     BRONZE_ingest.py        SILVER_transform.py          GOLD_report.py
```

Each notebook builds on the previous one. Running them in order (`BRONZE` →
`SILVER` → `GOLD`) produces the final reporting tables.

---

## Repository layout

```
databricks-pyspark-etl/
├── notebooks/            # Databricks notebooks as .py source (BRONZE, SILVER, GOLD)
├── etl/                  # Reusable PySpark transformation package (unit-testable)
├── tests/                # pytest suite for the etl package (local SparkSession)
├── data/                 # Sample CSV used by the demo & tests
├── .github/workflows/    # CI on push/PR to main (PySpark + JDK 17 + pytest)
├── requirements.txt      # pyspark, pandas, pytest
├── pyproject.toml        # project + pytest config
├── Dockerfile            # optional containerised local run
├── README.md
└── .env.example          # copy to .env — never commit real values
```

---

## How Databricks runs this

1. **Import the repo** into a Databricks workspace (Git integration or
   **Repos** → add repo).
2. Create a **cluster** with a Databricks runtime (e.g. 13.3 LTS) — PySpark is
   already installed.
3. Open **`notebooks/BRONZE_ingest.py`** and select the cluster with
   **Run notebook**.
4. The notebooks use `sys.path.insert(0, "/Workspace/Repos/databricks-pyspark-etl")`
   so the `etl` package resolves inside Databricks.
5. Each notebook registers Delta tables (`bronze_orders`, `silver_orders`,
   `gold_category_revenue`, `gold_region_revenue`) you can query from
   **Databricks SQL**.

You can also schedule the notebooks as a **Databricks Job** in the order
BRONZE → SILVER → GOLD for a production multi-hop pipeline.

---

## Run locally

### 1. Prerequisites

- Python 3.9+ (3.11 recommended)
- Java 8/11/17. **JDK 17 is strongly recommended** — see CloudOutput below.
- `pip`

### 2. Install

```bash
cd databricks-pyspark-etl
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the tests

```bash
export SPARK_LOCAL_IP=127.0.0.1      # defensive on some runners
pytest -v
```

Expected: all tests pass. Example tail:

```text
10 passed in 200.42s
```

### 4. Smoke-test the notebook logic locally

Local smoke test of the chained ETL (loads the sample CSV, applies the same
functions and prints aggregated results):

```bash
python tests/run_pipeline_demo.py
```

---

## .env.example

Copy to `.env` if you run anything against a real Databricks workspace
(Databricks CLI / REST). The repo contains **no secrets** — only placeholders.

---

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on **push/PR to
`main`** and:

1. Checks out the code (`actions/checkout@v4`).
2. Sets up **Python 3.11** (`actions/setup-python@v5`).
3. Sets up **Temurin JDK 17** with Maven cache (`actions/setup-java@v4`).
4. Installs PySpark (which uses the provided JDK 17) and runs `pytest -v`.

---

## Notes / caveats

- PySpark on **Windows** may fail to launch with **Java 21+**; the CI job pins
  **JDK 17** for a guaranteed-green run. For full local fidelity use JDK 17.
- Spark pipelines are inherently eager around `.count()` / `.collect()`; the
  sample dataset is deliberately small so local runs stay fast.
- This is a portfolio demo, not production infrastructure (no Autoloader,
  Unity Catalog, or Delta live tables — though it can be extended to them).

---

## License

MIT — free to use as a learning and portfolio piece.