#!/usr/bin/env python3
"""
Generate GCP cost directory structure from BigQuery billing export data.

Structure:
  costs/gcp/
  ├── by-resource/{resource_name}/cost.json
  ├── by-project/{project_id}/{resource_name} -> symlink
  ├── by-service/{service_name}/{resource_name} -> symlink
  └── summary.json
"""

import os
import sys
import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from fetch_gcp_costs import fetch_daily_costs, fetch_daily_totals, validate_config

COSTS_DIR = Path("costs/gcp")


def sanitize_name(name):
    """Sanitize name for filesystem use."""
    if not name:
        return "_unknown_"
    return name.replace("/", "_").replace("\\", "_").replace(":", "_").replace(" ", "_")


def generate_structure(rows, accurate_totals=None):
    """Generate the directory structure with daily costs and rolling 30-day aggregates."""
    today = datetime.now().strftime("%Y-%m-%d")

    # Clean and recreate
    if COSTS_DIR.exists():
        shutil.rmtree(COSTS_DIR)

    by_resource = COSTS_DIR / "by-resource"
    by_project = COSTS_DIR / "by-project"
    by_service = COSTS_DIR / "by-service"

    by_resource.mkdir(parents=True)
    by_project.mkdir(parents=True)
    by_service.mkdir(parents=True)

    # Determine currency from data (GCP billing is typically in one currency per billing account)
    currencies = set(r.get("currency", "USD") for r in rows)
    currency = currencies.pop() if len(currencies) == 1 else "USD"

    # Get date range
    all_dates = sorted(set(r["usage_date"] for r in rows if r.get("usage_date")))
    if accurate_totals:
        all_dates = sorted(set(all_dates) | set(accurate_totals.keys()))
    data_start = all_dates[0] if all_dates else today
    data_end = all_dates[-1] if all_dates else today

    # Rolling 30-day cutoff from end of data
    data_end_dt = datetime.strptime(data_end, "%Y-%m-%d")
    cutoff_date = (data_end_dt - timedelta(days=30)).strftime("%Y-%m-%d")

    # Aggregate by project + resource (so same-named resources in different projects stay separate)
    resources = {}
    for row in rows:
        resource_name = row.get("resource_name") or "_unknown_"
        project_id = row.get("project_id") or "_unknown_project_"
        # Key by project + resource to avoid merging across projects
        key = f"{sanitize_name(project_id)}_{sanitize_name(resource_name)}" if resource_name == "_unknown_" else sanitize_name(resource_name)
        date = row.get("usage_date")
        cost = row.get("net_cost", 0)
        service = row.get("service_name")

        if key not in resources:
            resources[key] = {
                "resource_name": resource_name if resource_name != "_unknown_" else f"_unknown_ ({project_id})",
                "project_id": project_id,
                "project_name": row.get("project_name"),
                "categories": set(),
                "category_costs": {},
                "daily_costs": {},
                "total_cost": 0,
                "rolling_30d_cost": 0,
            }

        if service:
            resources[key]["categories"].add(service)
            if service not in resources[key]["category_costs"]:
                resources[key]["category_costs"][service] = 0
            if date and date >= cutoff_date:
                resources[key]["category_costs"][service] += cost

        if date:
            if date not in resources[key]["daily_costs"]:
                resources[key]["daily_costs"][date] = 0
            resources[key]["daily_costs"][date] += cost

        resources[key]["total_cost"] += cost
        if date and date >= cutoff_date:
            resources[key]["rolling_30d_cost"] += cost

    # Calculate unallocated costs
    if accurate_totals:
        detailed_by_day = {}
        for res_data in resources.values():
            for date, cost in res_data["daily_costs"].items():
                if date not in detailed_by_day:
                    detailed_by_day[date] = 0
                detailed_by_day[date] += cost

        unallocated_daily = {}
        total_unallocated = 0
        rolling_unallocated = 0
        for date, accurate_cost in accurate_totals.items():
            detailed_cost = detailed_by_day.get(date, 0)
            diff = accurate_cost - detailed_cost
            if abs(diff) > 0.01:
                unallocated_daily[date] = round(diff, 2)
                total_unallocated += diff
                if date >= cutoff_date:
                    rolling_unallocated += diff

        if unallocated_daily:
            resources["_unallocated_"] = {
                "resource_name": "_unallocated_",
                "project_id": None,
                "project_name": None,
                "categories": {"Unallocated"},
                "daily_costs": unallocated_daily,
                "total_cost": total_unallocated,
                "rolling_30d_cost": rolling_unallocated,
            }

    # Write per-resource files and create symlinks
    projects_seen = set()
    services_seen = set()

    for safe_name, data in resources.items():
        resource_dir = by_resource / safe_name
        resource_dir.mkdir(parents=True, exist_ok=True)

        daily_sorted = sorted(data["daily_costs"].items(), key=lambda x: x[0], reverse=True)

        cost_data = {
            "provider": "gcp",
            "resource_name": data["resource_name"],
            "resource_group": data["project_id"],
            "rolling_30d_cost": round(data["rolling_30d_cost"], 2),
            "total_cost": round(data["total_cost"], 2),
            "categories": sorted(data["categories"]),
            "currency": currency,
            "last_updated": today,
            "data_range": {"start": data_start, "end": data_end},
            "daily_costs": {date: round(cost, 2) for date, cost in daily_sorted},
            "provider_metadata": {
                "project_id": data["project_id"],
                "project_name": data["project_name"],
            },
        }

        with open(resource_dir / "cost.json", "w") as f:
            json.dump(cost_data, f, indent=2)

        # Project symlinks
        project_id = sanitize_name(data["project_id"])
        if project_id:
            proj_dir = by_project / project_id
            proj_dir.mkdir(parents=True, exist_ok=True)
            link_path = proj_dir / safe_name
            if not link_path.exists():
                link_path.symlink_to(Path("../../by-resource") / safe_name)
            projects_seen.add(data["project_id"])

        # Service symlinks
        for cat in data["categories"]:
            safe_cat = sanitize_name(cat)
            if safe_cat:
                svc_dir = by_service / safe_cat
                svc_dir.mkdir(parents=True, exist_ok=True)
                link_path = svc_dir / safe_name
                if not link_path.exists():
                    link_path.symlink_to(Path("../../by-resource") / safe_name)
                services_seen.add(cat)

    # Generate summary
    rolling_total = sum(r["rolling_30d_cost"] for r in resources.values())
    total_all_time = sum(r["total_cost"] for r in resources.values())

    top_resources = sorted(resources.items(), key=lambda x: x[1]["rolling_30d_cost"], reverse=True)[:20]

    daily_totals = {}
    for data in resources.values():
        for date, cost in data["daily_costs"].items():
            if date not in daily_totals:
                daily_totals[date] = 0
            daily_totals[date] += cost

    daily_totals_sorted = {k: round(v, 2) for k, v in sorted(daily_totals.items(), reverse=True)}

    by_category_totals = {}
    for safe_name, data in resources.items():
        for cat, cat_cost in data.get("category_costs", {}).items():
            if cat not in by_category_totals:
                by_category_totals[cat] = 0
            by_category_totals[cat] += cat_cost

    by_category_totals = {k: round(v, 2) for k, v in sorted(by_category_totals.items(), key=lambda x: -x[1])}

    summary = {
        "provider": "gcp",
        "source": "bigquery-export",
        "currency": currency,
        "date": today,
        "rolling_30d_cost": round(rolling_total, 2),
        "total_all_time_cost": round(total_all_time, 2),
        "data_range": {"start": data_start, "end": data_end},
        "rolling_30d_cutoff": cutoff_date,
        "resource_count": len(resources),
        "category_count": len(services_seen),
        "project_count": len(projects_seen),
        "top_20_resources": [
            {"name": name, "rolling_30d_cost": round(data["rolling_30d_cost"], 2)}
            for name, data in top_resources
        ],
        "daily_totals": daily_totals_sorted,
        "by_category": by_category_totals,
    }

    with open(COSTS_DIR / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    return summary


def main():
    print("=" * 60)
    print("GCP Cost Structure Generator")
    print("Daily costs from BigQuery billing export")
    print("=" * 60)
    print()

    validate_config()

    print("[1] Fetching daily totals (accurate)...")
    accurate_totals = fetch_daily_totals()
    print(f"    Got {len(accurate_totals)} days of totals")

    print("[2] Fetching daily cost data (detailed)...")
    rows = fetch_daily_costs()
    print(f"    Fetched {len(rows)} cost entries")

    if not rows:
        print("    No data fetched")
        return 1

    dates = sorted(set(r["usage_date"] for r in rows if r.get("usage_date")))
    if dates:
        print(f"    Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")

    print("[3] Generating directory structure...")
    summary = generate_structure(rows, accurate_totals)

    print()
    print("=" * 60)
    print(f"Generated structure in {COSTS_DIR}/")
    print(f"  Rolling 30-day cost: ${summary['rolling_30d_cost']:,.2f} {summary['currency']}")
    print(f"  Total all-time cost: ${summary['total_all_time_cost']:,.2f} {summary['currency']}")
    print(f"  Data range: {summary['data_range']['start']} to {summary['data_range']['end']}")
    print(f"  Resources: {summary['resource_count']}")
    print(f"  Services: {summary['category_count']}")
    print(f"  Projects: {summary['project_count']}")
    print()
    print("Top 5 by rolling 30-day cost:")
    for r in summary["top_20_resources"][:5]:
        print(f"  {r['name']}: ${r['rolling_30d_cost']:,.2f}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
