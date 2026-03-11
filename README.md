# Infraless Data — Example Repository

This is a template repository for [Infraless Data](https://infralessdata.com). Define your data pipelines in YAML, write Python functions, and let the platform handle deployment and execution — no infrastructure required.

## Quick Start

### 1. Connect a repository
Create a new repo based off of this in the UI.

Connect Repository -> Create New

### 2. Build pipelines with the AI agent or locally

- Use the **Agent** tab to describe what you want — it will write and deploy pipelines for you
- Or edit `pipelines/` and `functions/` directly in your local editor and push to deploy

### 3. Push to deploy

Edit your pipeline or functions, then push to trigger a redeploy:

```bash
git add .
git commit -m "Update pipeline"
git push
```

### Branch-based Environments

The platform automatically deploys to different environments based on your branch:

| Branch | Environment |
|--------|-------------|
| `main` or `master` | Production |
| `dev` or `develop` | Development |

## Repository Structure

```
your-repo/
├── config.yaml                # Repo-level config (Python version, schemas)
├── dev.py                     # Local development CLI
├── pipelines/                 # Pipeline definitions (YAML)
│   └── daily_etl.yaml         # Example ETL pipeline
├── functions/                 # Python functions
│   ├── extract_data.py        # Data extraction
│   ├── transform_sales.py     # Data transformation
│   └── visualize_sales.py     # Publishes a shareable dashboard
└── README.md
```

## Repository Configuration

The `config.yaml` file defines the Python version, Iceberg schemas, and pip packages for your repository.

```yaml
python_version: "3.11"

schemas:
  - sales
  - analytics

pip_packages: []
```

### Config Fields

| Field | Default | Description |
|-------|---------|-------------|
| `python_version` | `"3.11"` | Python version (e.g. `"3.11"`, `"3.12"`) |
| `schemas` | `[]` | Iceberg namespace names for your lakehouse tables |
| `pip_packages` | `[]` | Additional PyPI packages to install (e.g. `["pandas==2.2.0", "httpx"]`) |

Compute resources are managed automatically by Dataproc Serverless — no CPU/memory configuration needed.

## Pipeline Definition

Pipelines are defined in YAML files in the `pipelines/` directory:

```yaml
name: daily-etl
description: Daily ETL pipeline example
schedule: "0 6 * * *"  # 6 AM daily (cron format)

tasks:
  - name: extract
    type: python
    function: functions/extract_data.py
    entrypoint: main
    args:
      - "raw_transactions"
      - "sales_data"

  - name: transform
    type: python
    function: functions/transform_sales.py
    entrypoint: main
    depends_on:
      - extract
```

### Pipeline Fields

| Field | Description |
|-------|-------------|
| `name` | Pipeline identifier (lowercase, hyphens) |
| `description` | Human-readable description |
| `schedule` | Cron expression for scheduled runs (optional) |
| `tasks` | List of tasks to execute |

### Task Fields

| Field | Description |
|-------|-------------|
| `name` | Task identifier |
| `type` | Task type (`python`) |
| `function` | Path to Python file |
| `entrypoint` | Function to call (default: `main`) |
| `args` | Arguments passed to the function (as positional args) |
| `depends_on` | List of tasks that must complete first |
| `vizId` | Publish the function's return value as a shareable visualization (optional) |

## Writing Python Functions

Functions receive a SparkSession as the first argument, followed by any `args` from the pipeline definition:

```python
# functions/extract_data.py

def main(spark, source_table: str, dataset: str):
    """
    Called with args from pipeline:
      args:
        - "raw_transactions"   -> source_table
        - "sales_data"         -> dataset
    """
    # Write to an Iceberg table
    df = spark.createDataFrame([...], columns=[...])
    df.writeTo(f"sales.{source_table}").createOrReplace()
```

## Visualizations

Any task can publish a shareable chart or dashboard by adding `vizId` to its pipeline definition and returning a viz spec from the function.

```yaml
# pipelines/daily_etl.yaml
tasks:
  - name: visualize
    type: python
    function: functions/visualize_sales.py
    vizId: sales-dashboard        # <-- enables publishing
    depends_on:
      - transform
```

The `vizId` becomes the public URL slug: `/v/{repoId}/{env}/sales-dashboard`.

### Single chart

Return a dict with a `type` field and the platform publishes one chart:

```python
def main(spark):
    data = [{"month": r.month, "revenue": r.revenue} for r in df.collect()]
    return {
        "type": "bar",
        "title": "Monthly Revenue",
        "data": data,
        "xKey": "month",
        "series": [{"key": "revenue", "label": "Revenue ($)", "color": "#3b82f6"}],
    }
```

### Dashboard (multiple charts)

Return a list and the platform publishes all charts together on one page:

```python
def main(spark):
    ...
    return [
        {
            "type": "metric",
            "title": "Total Revenue",
            "value": total_revenue,
            "unit": "USD",
        },
        {
            "type": "bar",
            "title": "Revenue by Region",
            "data": chart_data,
            "xKey": "region",
            "series": [{"key": "revenue", "label": "Revenue ($)", "color": "#3b82f6"}],
        },
        {
            "type": "table",
            "title": "Region Breakdown",
            "columns": ["region", "revenue", "orders"],
            "data": chart_data,
        },
    ]
```

### Supported chart types

| Type | Description | Required fields |
|------|-------------|-----------------|
| `bar` | Bar chart | `data`, `xKey`, `series` |
| `line` | Line chart | `data`, `xKey`, `series` |
| `pie` | Pie chart | `data`, `xKey`, `series` |
| `table` | Data table | `data`, `columns` (optional) |
| `metric` | Single number | `value`, `unit` (optional), `label` (optional) |

All chart types support an optional `title` and `subtitle` field.

Visualizations auto-refresh every minute and require no login to view.

## Environment Variables

Configure environment variables in the platform UI:

1. Go to **Settings** > **Environment Variables**
2. Select your repository and environment
3. Add your variables (e.g., `API_KEY`, `DATABASE_URL`)

Variables are securely stored and injected into your functions at runtime via `os.environ`:

```python
import os

def main(spark):
    api_key = os.environ.get("API_KEY")
    database_url = os.environ.get("DATABASE_URL")
```

## Local Development

The `dev.py` CLI lets you run functions against real data from your local machine. All execution happens on the platform via the API — you just need Python and the `requests` library.

### Setup

```bash
pip install requests
```

1. Go to **Settings** > **API Keys** and generate a new key
2. Go to **Settings** > **Environment Variables**, select your repo, and click **Download .env.development**
3. Open `.env.development` and paste your API key:

```
INFRALESS_API_URL=https://infralessdata.com
INFRALESS_API_KEY=wf_your_actual_key_here
INFRALESS_REPO_ID=abc123
INFRALESS_ENVIRONMENT=development
```

### Running a function

```bash
# Run a function against development data
python dev.py run functions/extract_data.py raw_transactions sales_data

# Run against production data
python dev.py run functions/transform_sales.py --env production

# Use a different entrypoint
python dev.py run functions/extract_data.py --entrypoint run_incremental
```

Output streams as the job runs and prints logs on completion:

```
Submitting functions/extract_data.py (development)...
Running... 10s
Running... 20s
Running... 80s

── Logs ──
INFO Loaded 1,240 rows from source
INFO Wrote to sales.raw_transactions

[✓] Success in 83s
```

### How it works

`dev.py run` reads your local Python file and submits it to the platform API, which runs it with the same Iceberg catalog and environment variables as production pipelines. The CLI polls for status every 10 seconds and prints logs when the job completes.

No GCP SDK, no `gcloud`, no Spark installation needed locally. Everything goes through the API.

## Platform Features

- **AI agent**: Describe what you want and the agent writes, deploys, and monitors pipelines for you
- **Auto-deploy**: Push to GitHub and pipelines are automatically redeployed in seconds
- **Scheduling**: Run pipelines on a cron schedule
- **Manual triggers**: Run pipelines on-demand from the dashboard, or run a single task in isolation
- **Real-time logs**: View execution logs as they happen
- **Cost tracking**: Monitor compute usage and costs per execution
- **Environment variables**: Securely store and inject secrets
- **Local dev CLI**: Test functions against real data without deploying
- **Iceberg lakehouse**: Query Apache Iceberg tables with no infrastructure to manage
- **Visualizations**: Publish shareable charts and dashboards from any task — no login required to view
