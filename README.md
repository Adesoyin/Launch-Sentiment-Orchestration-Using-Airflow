# LaunchSentiment – Wikipedia Pageviews Data Pipeline
## Project Overview

LaunchSentiment is a data pipeline built to support a stock market sentiment analysis use case by tracking Wikipedia pageviews for selected companies. The assumption is that changes in pageviews reflect public interest and sentiment, which can be correlated with stock price movements.

This pipeline ingests hourly Wikipedia pageviews data, filters specific company pages, and loads the results into a PostgreSQL database for analysis.

## Selected Companies

The pipeline currently tracks pageviews for the following companies:

- Apple (Apple_Inc.)

- Facebook

- Microsoft

- Google

- Amazon

## Architecture & Design
**High-Level Flow**

- Download hourly Wikipedia pageviews file (.gz)

- Extract the gzip file into CSV format

- Filter records for selected company pages

- Load filtered data into PostgreSQL

- Analyze pageviews using SQL

**Technology Stack**

- Orchestration: Apache Airflow

- Data Source: Wikimedia Pageviews Dumps

- Processing: Python

- Storage: PostgreSQL

- File Storage: Docker-mounted volume (/opt/airflow/datalake)

- Pipeline Orchestration (Airflow)

**The pipeline is orchestrated using an Apache Airflow DAG with the following tasks:**

    download_pageviews >> extract_gz_to_csv >> filter_companies >> load_to_postgres

Tasks are chained sequentially and communicate using XComs to pass file paths.

## Database Design & Idempotency
Table Design

Data is loaded into a pre-created PostgreSQL table.

To ensure idempotency, a composite primary key was used when creating the table.
The key is a combination of:

    domain

    page_title

    view_count

This prevents duplicate records during retries or DAG re-runs.

**Insert Strategy**

    ON CONFLICT DO NOTHING

This ensures safe re-execution of the pipeline without duplicating data.

## Best Practices Implemented

**Retries:** Configured retries for transient failures on dag level.

**Failure Alerts:** Email notifications on task failure

**Idempotency:** Enforced via composite primary key

**Modular Design:** Each task has a single responsibility

**Container-Safe Paths:** Writes only to mounted volumes

**Re-runnable Pipeline:** Safe to re-run without data corruption

## Sample Analysis Query

To identify the company with the highest pageviews for the selected hour:

    SELECT
        page_title,
        SUM(viewcount) AS total_views
    FROM wiki_pages
    GROUP BY page_title
    ORDER BY total_views DESC
    LIMIT 1;