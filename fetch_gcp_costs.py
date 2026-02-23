#!/usr/bin/env python3
"""
Fetch daily cost data from Google Cloud via BigQuery billing export.

Requires:
  - BigQuery billing export enabled in the GCP project
  - Service account with roles/bigquery.dataViewer and roles/bigquery.jobUser
  - GOOGLE_APPLICATION_CREDENTIALS pointing to service account key

Environment variables:
  GCP_BILLING_PROJECT_ID  - GCP project containing the billing export
  GCP_BILLING_DATASET_ID  - BigQuery dataset name
  GCP_BILLING_TABLE_ID    - BigQuery table name (e.g., gcp_billing_export_resource_v1_XXXXXX)
"""

import os
from datetime import datetime, timedelta

from google.cloud import bigquery


PROJECT_ID = os.environ.get("GCP_BILLING_PROJECT_ID")
DATASET_ID = os.environ.get("GCP_BILLING_DATASET_ID")
TABLE_ID = os.environ.get("GCP_BILLING_TABLE_ID")


def get_client():
    """Create a BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)


def fetch_daily_costs(days=45):
    """
    Query BigQuery for daily costs per resource, service, and project.

    Returns list of dicts with keys:
      usage_date, project_id, project_name, service_name,
      resource_name, net_cost, currency
    """
    client = get_client()
    table_ref = f"`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"

    query = f"""
    SELECT
        DATE(usage_start_time) AS usage_date,
        project.id AS project_id,
        project.name AS project_name,
        service.description AS service_name,
        resource.name AS resource_name,
        ROUND(SUM(cost) + SUM(IFNULL(
            (SELECT SUM(c.amount) FROM UNNEST(credits) c), 0
        )), 4) AS net_cost,
        currency
    FROM {table_ref}
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    GROUP BY usage_date, project_id, project_name, service_name,
             resource_name, currency
    HAVING net_cost != 0
    ORDER BY usage_date DESC, net_cost DESC
    """

    print(f"Querying BigQuery for last {days} days of costs...")
    result = client.query(query).result()

    rows = []
    for row in result:
        rows.append({
            "usage_date": row.usage_date.strftime("%Y-%m-%d") if row.usage_date else None,
            "project_id": row.project_id,
            "project_name": row.project_name,
            "service_name": row.service_name,
            "resource_name": row.resource_name or "_unknown_",
            "net_cost": float(row.net_cost),
            "currency": row.currency,
        })

    print(f"  Got {len(rows)} cost entries")
    return rows


def fetch_daily_totals(days=45):
    """
    Query BigQuery for accurate daily totals (no grouping by resource).

    Returns dict of date -> total cost.
    """
    client = get_client()
    table_ref = f"`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"

    query = f"""
    SELECT
        DATE(usage_start_time) AS usage_date,
        ROUND(SUM(cost) + SUM(IFNULL(
            (SELECT SUM(c.amount) FROM UNNEST(credits) c), 0
        )), 4) AS net_cost
    FROM {table_ref}
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    GROUP BY usage_date
    HAVING net_cost != 0
    ORDER BY usage_date DESC
    """

    result = client.query(query).result()

    totals = {}
    for row in result:
        date_str = row.usage_date.strftime("%Y-%m-%d") if row.usage_date else None
        if date_str:
            totals[date_str] = float(row.net_cost)

    return totals


def validate_config():
    """Validate required environment variables."""
    missing = []
    for var in ["GCP_BILLING_PROJECT_ID", "GCP_BILLING_DATASET_ID", "GCP_BILLING_TABLE_ID"]:
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


if __name__ == "__main__":
    validate_config()
    rows = fetch_daily_costs()
    print(f"Fetched {len(rows)} rows")
    if rows:
        dates = sorted(set(r["usage_date"] for r in rows if r["usage_date"]))
        print(f"Date range: {dates[0]} to {dates[-1]}")
