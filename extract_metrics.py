#!/usr/bin/env python3
"""
ZenML 2025 Unwrapped - Data Extraction Script (Client Version)

Extracts pipeline metrics from ZenML via the standard client and generates
metrics.json for the 2025 Unwrapped static webpage.

Usage:
    python extract_metrics.py [--output metrics.json]

Requirements:
    - ZenML client: `pip install zenml`
    - Authenticated ZenML client configuration (e.g., `zenml login`)
"""

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from zenml.client import Client
from zenml.enums import ServiceState
from zenml.utils.pagination_utils import depaginate

DEFAULT_PAGE_SIZE = 500


def parse_datetime(value: datetime | str | None) -> datetime | None:
    """Parse ISO datetime string or return existing datetime."""
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        if isinstance(value, str):
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return None


def is_2025(dt: datetime | None) -> bool:
    """Check if datetime is in 2025."""
    return dt is not None and dt.year == 2025


def normalize_status(status: Any) -> str:
    """Normalize status values to a lowercase string."""
    if status is None:
        return "unknown"
    value = status.value if hasattr(status, "value") else str(status)
    return str(value).lower()


def build_pipeline_maps(pipelines: list) -> tuple[dict[str, str], dict[str, str]]:
    """Build pipeline ID/name lookup maps."""
    pipeline_name_by_id: dict[str, str] = {}
    pipeline_id_by_name: dict[str, str] = {}

    for pipeline in pipelines:
        pipeline_id = str(pipeline.id)
        pipeline_name = pipeline.name
        pipeline_name_by_id[pipeline_id] = pipeline_name
        pipeline_id_by_name[pipeline_name] = pipeline_id

    return pipeline_name_by_id, pipeline_id_by_name


def get_pipeline_id_and_name(
    run: Any,
    pipeline_name_by_id: dict[str, str],
    pipeline_id_by_name: dict[str, str],
) -> tuple[str | None, str | None]:
    """Extract pipeline ID and name from a run."""
    resources = getattr(run, "resources", None)
    pipeline = getattr(resources, "pipeline", None) if resources else None
    if pipeline:
        return str(pipeline.id), pipeline.name

    pipeline_name = None
    try:
        pipeline_name = run.config.name
    except Exception:
        pipeline_name = None

    if pipeline_name:
        pipeline_id = pipeline_id_by_name.get(pipeline_name)
        if pipeline_id:
            return pipeline_id, pipeline_name_by_id.get(pipeline_id, pipeline_name)
        return None, pipeline_name

    return None, None


def get_user_id(run: Any) -> str | None:
    """Extract user ID from a run."""
    if getattr(run, "user_id", None):
        return str(run.user_id)
    resources = getattr(run, "resources", None)
    user = getattr(resources, "user", None) if resources else None
    if user:
        return str(user.id)
    return None


def fetch_runs_for_year(
    client: Client,
    year: int = 2025,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> list:
    """Fetch pipeline runs for a specific year via the ZenML client."""
    all_runs = []
    page = client.list_pipeline_runs(
        sort_by="desc:created",
        page=1,
        size=page_size,
        hydrate=False,
    )

    while True:
        if page.index == 1:
            print(f"    First page: {len(page.items)} runs")

        for run in page.items:
            created = parse_datetime(run.created)
            if created and created.year == year:
                all_runs.append(run)
            elif created and created.year < year:
                print(f"    Reached runs from {created.year}, stopping pagination")
                return all_runs

        if page.index >= page.total_pages or not page.items:
            break

        if page.index % 5 == 0:
            print(
                f"    Fetched page {page.index}, found {len(all_runs)} runs from {year} so far..."
            )

        page = client.list_pipeline_runs(
            sort_by="desc:created",
            page=page.index + 1,
            size=page_size,
            hydrate=False,
        )

    return all_runs


def extract_raw_data(
    client: Client,
    year: int = 2025,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict:
    """Extract all raw data from the ZenML server via client."""
    print("\n📊 Extracting data from ZenML server via client...")

    print("  → Fetching users...")
    users = depaginate(client.list_users, size=page_size)
    print(f"    Found {len(users)} users")

    print("  → Fetching pipeline runs...")
    runs = fetch_runs_for_year(client, year=year, page_size=page_size)
    print(f"    Found {len(runs)} runs from {year}")

    print("  → Fetching pipelines...")
    pipelines = depaginate(client.list_pipelines, size=page_size)
    print(f"    Found {len(pipelines)} pipelines")

    print("  → Fetching artifacts...")
    artifacts = depaginate(client.list_artifacts, size=page_size)
    print(f"    Found {len(artifacts)} artifacts")

    print("  → Fetching models...")
    models = depaginate(client.list_models, size=page_size)
    print(f"    Found {len(models)} models")

    print("  → Fetching stacks...")
    stacks = depaginate(client.list_stacks, size=page_size)
    print(f"    Found {len(stacks)} stacks")

    print("  → Fetching schedules...")
    schedules = depaginate(client.list_schedules, size=page_size)
    print(f"    Found {len(schedules)} schedules")

    print("  → Fetching services...")
    services = depaginate(client.list_services, size=page_size)
    print(f"    Found {len(services)} services")

    return {
        "users": users,
        "runs": runs,
        "pipelines": pipelines,
        "artifacts": artifacts,
        "models": models,
        "stacks": stacks,
        "schedules": schedules,
        "services": services,
    }


def get_model_version_count(model: Any) -> int:
    """Return the best available model version count without triggering extra lookups."""
    for attr in ("number_of_versions", "num_versions", "version_count"):
        value = getattr(model, attr, None)
        if value is None:
            continue
        try:
            count = int(value)
        except (TypeError, ValueError):
            continue
        if count >= 0:
            return count

    if (
        getattr(model, "latest_version_name", None)
        or getattr(model, "latest_version_id", None)
        or getattr(model, "latest_version_number", None)
    ):
        return 1

    return 0


def compute_core_stats(
    runs_2025: list,
    artifacts: list,
    models: list,
    stacks: list,
    schedules: list,
    services: list,
    pipeline_name_by_id: dict[str, str],
    pipeline_id_by_name: dict[str, str],
) -> dict:
    """Compute core statistics from ZenML response objects."""
    total_runs = len(runs_2025)

    status_counts = Counter(
        normalize_status(run.status) for run in runs_2025
    )

    completed = status_counts.get("completed", 0)
    failed = status_counts.get("failed", 0)

    pipeline_ids = set()
    user_ids = set()
    for run in runs_2025:
        pipeline_id, _ = get_pipeline_id_and_name(
            run, pipeline_name_by_id, pipeline_id_by_name
        )
        if pipeline_id:
            pipeline_ids.add(pipeline_id)

        user_id = get_user_id(run)
        if user_id:
            user_ids.add(user_id)

    # Filter 2025 artifacts and models
    artifacts_2025 = [
        artifact
        for artifact in artifacts
        if is_2025(parse_datetime(artifact.created))
    ]
    models_2025 = [
        model
        for model in models
        if is_2025(parse_datetime(model.created))
    ]

    # Sum model versions
    total_model_versions = 0
    for model in models_2025:
        total_model_versions += get_model_version_count(model)

    # Active schedules and services
    active_schedules = len([s for s in schedules if s.active])
    active_services = len(
        [s for s in services if s.state == ServiceState.ACTIVE]
    )

    return {
        "total_runs": total_runs,
        "successful_runs": completed,
        "failed_runs": failed,
        "success_rate": round(completed / total_runs * 100, 1)
        if total_runs > 0
        else 0,
        "unique_pipelines": len(pipeline_ids),
        "unique_users": len(user_ids),
        "artifacts_produced": len(artifacts_2025),
        "models_created": len(models_2025),
        "model_versions": total_model_versions,
        "total_stacks": len(stacks),
        "active_schedules": active_schedules,
        "active_services": active_services,
        "status_breakdown": dict(status_counts),
    }


def compute_time_analytics(runs_2025: list) -> dict:
    """Compute time-based analytics from ZenML response objects."""
    month_names = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    day_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    empty_time_analytics = {
        "runs_per_month": [0] * 12,
        "busiest_month": None,
        "busiest_month_count": 0,
        "busiest_day": None,
        "busiest_day_count": 0,
        "busiest_hour": None,
        "busiest_hour_count": 0,
        "first_run": None,
        "last_run": None,
        "weekend_runs": 0,
        "weekday_runs": 0,
        "day_distribution": {day: 0 for day in day_names},
    }
    if not runs_2025:
        return empty_time_analytics

    run_times = []
    for run in runs_2025:
        created = parse_datetime(run.created)
        if created:
            run_times.append(created)

    if not run_times:
        return empty_time_analytics

    month_counts = Counter(dt.month for dt in run_times)
    runs_per_month = [month_counts.get(m, 0) for m in range(1, 13)]
    busiest_month_num = max(month_counts, key=month_counts.get) if month_counts else 1

    day_counts = Counter(dt.weekday() for dt in run_times)
    busiest_day_num = max(day_counts, key=day_counts.get) if day_counts else 0

    hour_counts = Counter(dt.hour for dt in run_times)
    busiest_hour = max(hour_counts, key=hour_counts.get) if hour_counts else 12

    first_run = min(run_times)
    last_run = max(run_times)

    weekend_runs = sum(1 for dt in run_times if dt.weekday() >= 5)
    weekday_runs = len(run_times) - weekend_runs

    return {
        "runs_per_month": runs_per_month,
        "busiest_month": month_names[busiest_month_num - 1],
        "busiest_month_count": month_counts[busiest_month_num],
        "busiest_day": day_names[busiest_day_num],
        "busiest_day_count": day_counts[busiest_day_num],
        "busiest_hour": busiest_hour,
        "busiest_hour_count": hour_counts[busiest_hour],
        "first_run": first_run.isoformat(),
        "last_run": last_run.isoformat(),
        "weekend_runs": weekend_runs,
        "weekday_runs": weekday_runs,
        "day_distribution": {day_names[i]: day_counts.get(i, 0) for i in range(7)},
    }


def compute_user_stats(
    runs_2025: list,
    users: list,
    pipeline_name_by_id: dict[str, str],
    pipeline_id_by_name: dict[str, str],
) -> list:
    """Compute per-user statistics from ZenML response objects."""
    user_lookup = {str(user.id): user for user in users}

    user_runs = defaultdict(list)
    for run in runs_2025:
        user_id = get_user_id(run)
        if user_id:
            user_runs[user_id].append(run)

    user_stats = []
    for user_id, runs in user_runs.items():
        user = user_lookup.get(user_id)
        if not user or getattr(user, "is_service_account", False):
            continue

        total = len(runs)
        failed = sum(
            1 for r in runs if normalize_status(r.status) == "failed"
        )
        completed = sum(
            1 for r in runs if normalize_status(r.status) == "completed"
        )

        hours = []
        for r in runs:
            created = parse_datetime(r.created)
            if created:
                hours.append(created.hour)
        avg_hour = sum(hours) / len(hours) if hours else 12

        weekend = 0
        for r in runs:
            created = parse_datetime(r.created)
            if created and created.weekday() >= 5:
                weekend += 1

        pipeline_ids = set()
        for r in runs:
            pipeline_id, _ = get_pipeline_id_and_name(
                r, pipeline_name_by_id, pipeline_id_by_name
            )
            if pipeline_id:
                pipeline_ids.add(pipeline_id)

        name = user.name or str(user_id)[:8]
        full_name = user.full_name or name
        avatar = user.avatar_url

        user_stats.append({
            "id": str(user_id),
            "name": name,
            "full_name": full_name or name,
            "avatar": avatar,
            "total_runs": total,
            "failed_runs": failed,
            "completed_runs": completed,
            "success_rate": round(completed / total * 100, 1)
            if total > 0
            else 0,
            "avg_hour": round(avg_hour, 1),
            "weekend_runs": weekend,
            "unique_pipelines": len(pipeline_ids),
        })

    return sorted(user_stats, key=lambda x: x["total_runs"], reverse=True)


def compute_top_pipelines(
    runs_2025: list,
    pipeline_name_by_id: dict[str, str],
    pipeline_id_by_name: dict[str, str],
) -> list:
    """Compute top pipelines by run count from ZenML response objects."""
    pipeline_info: dict[str, str] = {}
    pipeline_counts = Counter()

    for run in runs_2025:
        pipeline_id, pipeline_name = get_pipeline_id_and_name(
            run, pipeline_name_by_id, pipeline_id_by_name
        )
        if pipeline_id:
            pipeline_counts[pipeline_id] += 1
            if pipeline_id not in pipeline_info:
                pipeline_info[pipeline_id] = (
                    pipeline_name
                    or pipeline_name_by_id.get(pipeline_id)
                    or pipeline_id[:8]
                )

    for pipeline_id, pipeline_name in pipeline_name_by_id.items():
        pipeline_info.setdefault(pipeline_id, pipeline_name)

    top_pipelines = []
    for pipe_id, count in pipeline_counts.most_common(10):
        name = pipeline_info.get(pipe_id, pipe_id[:8])
        top_pipelines.append({
            "name": name,
            "runs": count,
        })

    return top_pipelines


def compute_awards(user_stats: list, runs_2025: list) -> dict:
    """Compute fun awards based on user stats."""
    if not user_stats:
        return {}

    awards = {}

    # Pipeline Overlord - most runs
    top_runner = max(user_stats, key=lambda x: x["total_runs"])
    awards["pipeline_overlord"] = {
        "title": "Pipeline Overlord",
        "icon": "👑",
        "description": "Ruled the pipeline kingdom",
        "user": top_runner["full_name"],
        "user_email": top_runner["name"],
        "avatar": top_runner["avatar"],
        "value": f"{top_runner['total_runs']} runs",
    }

    # Failure Champion - most failures (min 5 failures)
    users_with_failures = [u for u in user_stats if u["failed_runs"] >= 5]
    if users_with_failures:
        top_failer = max(users_with_failures, key=lambda x: x["failed_runs"])
        awards["failure_champion"] = {
            "title": "Failure Champion",
            "icon": "🔥",
            "description": "Learning through iteration",
            "user": top_failer["full_name"],
            "user_email": top_failer["name"],
            "avatar": top_failer["avatar"],
            "value": f"{top_failer['failed_runs']} failed runs",
        }

    # Success Streak - highest success rate (min 20 runs)
    qualified = [u for u in user_stats if u["total_runs"] >= 20]
    if qualified:
        most_reliable = max(qualified, key=lambda x: x["success_rate"])
        awards["success_streak"] = {
            "title": "Success Streak",
            "icon": "⭐",
            "description": "The reliable one",
            "user": most_reliable["full_name"],
            "user_email": most_reliable["name"],
            "avatar": most_reliable["avatar"],
            "value": f"{most_reliable['success_rate']}% success rate",
        }

    # Night Owl - latest average run time
    night_owl = max(
        user_stats,
        key=lambda x: x["avg_hour"]
        if x["avg_hour"] >= 18 or x["avg_hour"] <= 6
        else 0,
    )
    if night_owl["avg_hour"] >= 18 or night_owl["avg_hour"] <= 6:
        hour = int(night_owl["avg_hour"])
        minute = int((night_owl["avg_hour"] - hour) * 60)
        awards["night_owl"] = {
            "title": "Night Owl",
            "icon": "🌙",
            "description": "When everyone's asleep",
            "user": night_owl["full_name"],
            "user_email": night_owl["name"],
            "avatar": night_owl["avatar"],
            "value": f"Avg start time: {hour:02d}:{minute:02d}",
        }

    # Early Bird - earliest average run time (between 5-9 AM)
    early_birds = [u for u in user_stats if 5 <= u["avg_hour"] <= 9]
    if early_birds:
        early_bird = min(early_birds, key=lambda x: x["avg_hour"])
        hour = int(early_bird["avg_hour"])
        minute = int((early_bird["avg_hour"] - hour) * 60)
        awards["early_bird"] = {
            "title": "Early Bird",
            "icon": "🌅",
            "description": "First to production",
            "user": early_bird["full_name"],
            "user_email": early_bird["name"],
            "avatar": early_bird["avatar"],
            "value": f"Avg start time: {hour:02d}:{minute:02d}",
        }

    # Weekend Warrior - most weekend runs
    weekend_warrior = max(user_stats, key=lambda x: x["weekend_runs"])
    if weekend_warrior["weekend_runs"] > 0:
        awards["weekend_warrior"] = {
            "title": "Weekend Warrior",
            "icon": "💪",
            "description": "No rest for ML",
            "user": weekend_warrior["full_name"],
            "user_email": weekend_warrior["name"],
            "avatar": weekend_warrior["avatar"],
            "value": f"{weekend_warrior['weekend_runs']} weekend runs",
        }

    # Variety Pack - most unique pipelines
    variety = max(user_stats, key=lambda x: x["unique_pipelines"])
    if variety["unique_pipelines"] > 1:
        awards["variety_pack"] = {
            "title": "Variety Pack",
            "icon": "🎨",
            "description": "Jack of all pipelines",
            "user": variety["full_name"],
            "user_email": variety["name"],
            "avatar": variety["avatar"],
            "value": f"{variety['unique_pipelines']} different pipelines",
        }

    return awards


def generate_fun_facts(
    time_analytics: dict,
    core_stats: dict,
    user_stats: list,
    top_pipelines: list,
) -> list:
    """Generate fun facts for the unwrapped page."""
    facts = []

    if time_analytics.get("first_run"):
        first = datetime.fromisoformat(time_analytics["first_run"])
        facts.append(
            f"Your team's first run of 2025 was on {first.strftime('%B %d')} at {first.strftime('%H:%M')}"
        )

    if time_analytics.get("busiest_day"):
        facts.append(
            f"{time_analytics['busiest_day']} was your most productive day with "
            f"{time_analytics['busiest_day_count']} runs"
        )

    if time_analytics.get("busiest_month"):
        facts.append(
            f"{time_analytics['busiest_month']} was your busiest month with "
            f"{time_analytics['busiest_month_count']} pipeline runs"
        )

    if time_analytics.get("weekend_runs", 0) > 0:
        pct = round(
            time_analytics["weekend_runs"]
            / (time_analytics["weekend_runs"] + time_analytics["weekday_runs"])
            * 100
        )
        facts.append(f"{pct}% of your runs happened on weekends 🎉")

    if top_pipelines:
        facts.append(
            f"Your most-run pipeline was '{top_pipelines[0]['name']}' "
            f"with {top_pipelines[0]['runs']} executions"
        )

    if core_stats.get("success_rate", 0) >= 90:
        facts.append(
            f"Your team achieved a {core_stats['success_rate']}% success rate - impressive! 🎯"
        )
    elif core_stats.get("success_rate", 0) >= 70:
        facts.append(
            f"Your team's success rate was {core_stats['success_rate']}% - "
            "solid experimentation!"
        )

    total = core_stats.get("total_runs", 0)
    if total >= 1000:
        facts.append("You crossed the 1,000 pipeline runs milestone! 🚀")
    elif total >= 500:
        facts.append("You ran over 500 pipelines this year!")
    elif total >= 100:
        facts.append(f"You hit triple digits with {total} pipeline runs!")

    if core_stats.get("models_created", 0) > 0:
        facts.append(
            f"Your team created {core_stats['models_created']} models "
            f"with {core_stats['model_versions']} total versions"
        )

    return facts


def run_extraction(output_path: str, year: int, page_size: int) -> None:
    """Main extraction logic."""
    print("=" * 60)
    print("  ZenML 2025 Unwrapped - Data Extraction (Client Version)")
    print("=" * 60)

    print("\n🔌 Connecting to ZenML server via client...")
    client = Client()
    print("✅ Connected to ZenML server")

    raw_data = extract_raw_data(client, year=year, page_size=page_size)

    runs_target_year = raw_data["runs"]
    print(f"\n🔍 Using {len(runs_target_year)} runs from {year}")

    if not runs_target_year:
        print(f"\n⚠️  No runs found for {year}. Exiting.")
        return

    pipeline_name_by_id, pipeline_id_by_name = build_pipeline_maps(
        raw_data["pipelines"]
    )

    print("\n🧮 Computing metrics...")

    print("  → Core statistics...")
    core_stats = compute_core_stats(
        runs_target_year,
        raw_data["artifacts"],
        raw_data["models"],
        raw_data["stacks"],
        raw_data["schedules"],
        raw_data["services"],
        pipeline_name_by_id,
        pipeline_id_by_name,
    )

    print("  → Time analytics...")
    time_analytics = compute_time_analytics(runs_target_year)

    print("  → User statistics...")
    user_stats = compute_user_stats(
        runs_target_year,
        raw_data["users"],
        pipeline_name_by_id,
        pipeline_id_by_name,
    )

    print("  → Top pipelines...")
    top_pipelines = compute_top_pipelines(
        runs_target_year,
        pipeline_name_by_id,
        pipeline_id_by_name,
    )

    print("  → Awards...")
    awards = compute_awards(user_stats, runs_target_year)

    print("  → Fun facts...")
    fun_facts = generate_fun_facts(
        time_analytics, core_stats, user_stats, top_pipelines
    )

    metrics = {
        "generated_at": datetime.now().isoformat(),
        "year": year,
        "core_stats": core_stats,
        "time_analytics": time_analytics,
        "top_pipelines": top_pipelines,
        "awards": awards,
        "users": user_stats,
        "fun_facts": fun_facts,
    }

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with open(output, "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    print(f"\n✅ Metrics saved to {output}")

    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    print(f"  Total runs: {core_stats['total_runs']}")
    print(f"  Success rate: {core_stats['success_rate']}%")
    print(f"  Unique users: {core_stats['unique_users']}")
    print(f"  Unique pipelines: {core_stats['unique_pipelines']}")
    print(f"  Awards generated: {len(awards)}")
    print(f"  Fun facts: {len(fun_facts)}")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract ZenML 2025 Unwrapped metrics via the ZenML client"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="data/metrics.json",
        help="Output path for metrics.json (default: data/metrics.json)",
    )
    parser.add_argument(
        "--year",
        "-y",
        type=int,
        default=2025,
        help="Year to extract metrics for (default: 2025)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=(
            "Page size for ZenML list calls "
            f"(default: {DEFAULT_PAGE_SIZE})"
        ),
    )
    args = parser.parse_args()

    run_extraction(args.output, args.year, args.page_size)


if __name__ == "__main__":
    main()
