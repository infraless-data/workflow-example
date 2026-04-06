# Infraless Data — Example Repository

This is a template repository for [Infraless Data](https://infralessdata.com). Define your data pipelines in YAML, write Python functions, and let the platform handle deployment and execution — no infrastructure required.

## Quick Start

### 1. Connect a repository
Create a new repo based off of this in the UI.

**Connect Repository → Create New**

### 2. Build pipelines with the AI agent or locally

- Use the **Agent** tab to describe what you want — it will write and deploy pipelines for you
- Or edit `pipelines/` and `functions/` directly in your local editor and push to deploy

### 3. Push to deploy

```bash
git add .
git commit -m "Update pipeline"
git push
```

Every push automatically deploys all pipelines. If `functions/requirements.txt` changes, the platform rebuilds your Cloud Function with the new packages (takes 2–3 min on first build, ~30s for updates).

## Repository Structure

```
your-repo/
├── pipelines/                 # Pipeline definitions (YAML)
│   └── daily_etl.yaml
├── functions/                 # Python functions
│   ├── requirements.txt       # Extra pip packages (optional)
│   ├── extract_data.py
│   ├── transform_sales.py
│   └── visualize_sales.py
└── README.md
```

## Writing Python Functions

Functions receive a BigQuery client and a Cloud Storage client as the first two arguments, followed by any positional `args` from the pipeline YAML and keyword context:

```python
# functions/extract_data.py

def main(bq, gcs, source_table: str, **context):
    """
    Args:
        bq:  google.cloud.bigquery.Client  — authenticated as this repo's service account
        gcs: google.cloud.storage.Client   — authenticated as this repo's service account
        source_table: positional arg from pipeline YAML
        **context: dataset_id, environment, execution_id, pipeline_name, task_name, project_id
    """
    import pandas as pd
    from google.cloud import bigquery

    dataset_id = context["dataset_id"]  # e.g. "r_myrepo_prod"

    df = pd.DataFrame([{"id": 1, "value": 100}])
    job = bq.load_table_from_dataframe(
        df, f"{dataset_id}.{source_table}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    return {"rows_written": len(df)}
```

Functions in the same repo can import each other:

```python
# functions/transform.py
from functions.helpers import clean_row   # works — all files are on sys.path
```

### Package dependencies

All packages your functions need must be listed in `functions/requirements.txt`. Edit it like any normal `requirements.txt`:

```
google-cloud-bigquery>=3.0
pandas>=2.0
scikit-learn
httpx==0.27.0
```

Pushing a change to `requirements.txt` triggers a Cloud Function rebuild. The only package the platform always adds is `functions-framework` (the Cloud Function runtime) — everything else is up to you.

## Pipeline Definition

```yaml
# pipelines/daily_etl.yaml
name: daily-etl
description: "Daily sales ETL"
schedule: "0 2 * * *"   # 2 AM daily (cron format, UTC)

tasks:
  - name: extract
    type: python
    function: functions/extract_data.py
    entrypoint: main
    args:
      - "raw_transactions"   # passed as source_table

  - name: transform
    type: python
    function: functions/transform_sales.py
    entrypoint: main
    depends_on:
      - extract

  - name: visualize
    type: python
    function: functions/visualize_sales.py
    entrypoint: main
    vizId: sales-dashboard   # publishes return value as a shareable chart
    depends_on:
      - transform
```

### Pipeline fields

| Field | Description |
|-------|-------------|
| `name` | Pipeline identifier (lowercase, hyphens) |
| `schedule` | Cron expression for scheduled runs (optional) |
| `tasks` | List of tasks |

### Task fields

| Field | Description |
|-------|-------------|
| `name` | Task identifier |
| `type` | `python` |
| `function` | Path to Python file |
| `entrypoint` | Function name to call (default: `main`) |
| `args` | Positional args passed after `bq` and `gcs` |
| `depends_on` | Tasks that must complete first |
| `vizId` | Publish the function's return value as a shareable visualization |

## Visualizations

Add `vizId` to a task and return a viz spec from your function:

```yaml
- name: visualize
  function: functions/visualize_sales.py
  vizId: revenue-dashboard
```

```python
def main(bq, gcs, **context):
    data = bq.query(f"SELECT region, revenue FROM `{context['dataset_id']}.sales_summary`").to_dataframe()
    chart_data = data.to_dict("records")

    # Return a dict for a single chart, or a list for a dashboard
    return [
        {"type": "metric", "title": "Total Revenue", "value": data["revenue"].sum(), "unit": "USD"},
        {"type": "bar",    "title": "Revenue by Region", "data": chart_data,
         "xKey": "region", "series": [{"key": "revenue", "label": "Revenue ($)", "color": "#3b82f6"}]},
        {"type": "table",  "title": "Breakdown", "columns": ["region", "revenue"], "data": chart_data},
    ]
```

The `vizId` becomes the public URL slug: `/v/{repoId}/{env}/revenue-dashboard`.

### Supported chart types

| Type | Required fields |
|------|-----------------|
| `bar` | `data`, `xKey`, `series` |
| `line` | `data`, `xKey`, `series` |
| `pie` | `data`, `xKey`, `series` |
| `table` | `data`, `columns` (optional) |
| `metric` | `value`, `unit` (optional), `label` (optional) |

All types support optional `title` and `subtitle`. Visualizations auto-refresh every minute and require no login to view.

## Environment Variables

Store secrets in the UI: **Settings → Environment Variables**. They are injected into your functions at runtime via `os.environ`:

```python
import os

def main(bq, gcs, **context):
    api_key = os.environ["API_KEY"]
```

## Local Development

Run functions locally against real BigQuery data using your repo's service account.

### 1. Get a local dev token

Go to **Settings → Local Dev** and click **Get Token**. Copy the `export` command shown — it generates a 1-hour access token for your repo's service account:

```bash
export GOOGLE_CLOUD_ACCESS_TOKEN=ya29.c...
```

### 2. Load your environment variables

Download `.env.development` from **Settings → Environment Variables** and source it:

```bash
set -a && source .env.development && set +a
```

### 3. Install dependencies

```bash
pip install google-cloud-bigquery google-cloud-storage pandas polars pyarrow db-dtypes
# plus anything in functions/requirements.txt
```

### 4. Run locally

```python
# local_run.py  (don't commit this file)
import os
from google.cloud import bigquery, storage
from functions.extract_data import main

PROJECT_ID = "your-gcp-project-id"   # shown in Settings > Local Dev
DATASET_ID = "r_yourrepoid_dev"       # shown in Settings > Local Dev

bq  = bigquery.Client(project=PROJECT_ID)
gcs = storage.Client(project=PROJECT_ID)

context = {
    "dataset_id":    DATASET_ID,
    "environment":   "development",
    "execution_id":  "local-test",
    "pipeline_name": "manual",
    "task_name":     "extract",
    "project_id":    PROJECT_ID,
}

result = main(bq, gcs, "raw_transactions", **context)
print(result)
```

The token expires after 1 hour — just get a fresh one from the UI when it does.

> **Security**: Never commit `local_run.py` or the token. Add both to `.gitignore`.

## Platform Features

- **AI agent**: Describe what you want and the agent writes, deploys, and monitors pipelines
- **Auto-deploy**: Push to GitHub — pipelines redeploy in seconds
- **Scheduling**: Cron schedules per pipeline
- **Manual triggers**: Run pipelines or individual tasks on-demand
- **Real-time logs**: Stream execution output as it happens
- **Cost tracking**: Per-execution compute cost tracking
- **Environment variables**: Securely stored secrets injected at runtime
- **BigQuery**: Tables live in a dedicated dataset per environment — query with standard SQL
- **Visualizations**: Shareable charts and dashboards from any task — no login required
