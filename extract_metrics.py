#!/usr/bin/env python3
"""
ZenML Unwrapped - Workspace-Wide Data Extraction Script

Extracts pipeline metrics from ZenML across ALL projects in a workspace
and generates metrics.json for the Unwrapped static webpage.

Usage:
    python extract_metrics.py [--output metrics.json] [--year 2025]
    python extract_metrics.py --exclude-projects "test-project,ci-automation"

Requirements:
    - ZenML client: `pip install zenml`
    - Authenticated ZenML client configuration (e.g., `zenml login`)
"""

import argparse
import json
import random
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from zenml.client import Client
from zenml.enums import ServiceState
from zenml.utils.pagination_utils import depaginate

DEFAULT_PAGE_SIZE = 500

# Codename word lists for anonymization (mixed themes per spec)
CODENAMES_SPACE = [
    "Nebula", "Quasar", "Andromeda", "Pulsar", "Nova", "Cosmos", "Orbit", "Eclipse",
    "Stellar", "Comet", "Galaxy", "Meteor", "Astral", "Zenith", "Photon", "Lunar"
]
CODENAMES_MYTH = [
    "Phoenix", "Titan", "Athena", "Apollo", "Hermes", "Valkyrie", "Odyssey", "Pegasus",
    "Atlas", "Orion", "Neptune", "Mercury", "Artemis", "Thor", "Zeus", "Prometheus"
]
CODENAMES_NATURE = [
    "Aurora", "Thunder", "Falcon", "Tempest", "Glacier", "Ember", "Zephyr", "Cascade",
    "Horizon", "Summit", "Tundra", "Monsoon", "Vortex", "Blaze", "Frost", "Crimson"
]


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


def is_target_year(dt: datetime | None, year: int) -> bool:
    """Check if datetime is in the target year."""
    return dt is not None and dt.year == year


def normalize_status(status: Any) -> str:
    """Normalize status values to a lowercase string."""
    if status is None:
        return "unknown"
    value = status.value if hasattr(status, "value") else str(status)
    return str(value).lower()


def build_pipeline_maps(pipelines: list) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Build pipeline ID/name lookup maps, including project mapping."""
    pipeline_name_by_id: dict[str, str] = {}
    pipeline_id_by_name: dict[str, str] = {}
    pipeline_project_by_id: dict[str, str] = {}

    for pipeline in pipelines:
        pipeline_id = str(pipeline.id)
        pipeline_name = pipeline.name
        pipeline_name_by_id[pipeline_id] = pipeline_name
        pipeline_id_by_name[pipeline_name] = pipeline_id
        # Get project name from pipeline if available
        if hasattr(pipeline, 'project') and pipeline.project:
            pipeline_project_by_id[pipeline_id] = pipeline.project.name if hasattr(pipeline.project, 'name') else str(pipeline.project)

    return pipeline_name_by_id, pipeline_id_by_name, pipeline_project_by_id


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


def fetch_projects(client: Client, exclude: set[str]) -> list:
    """Fetch all projects, filtering out excluded ones."""
    all_projects = depaginate(client.list_projects)
    return [p for p in all_projects if p.name not in exclude]


def fetch_runs_for_year(
    client: Client,
    year: int,
    project_id: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> list:
    """Fetch pipeline runs for a specific year and project via the ZenML client."""
    all_runs = []
    page = client.list_pipeline_runs(
        project=project_id,
        sort_by="desc:created",
        page=1,
        size=page_size,
        hydrate=False,
    )

    while True:
        for run in page.items:
            created = parse_datetime(run.created)
            if created and created.year == year:
                all_runs.append(run)
            elif created and created.year < year:
                return all_runs

        if page.index >= page.total_pages or not page.items:
            break

        page = client.list_pipeline_runs(
            project=project_id,
            sort_by="desc:created",
            page=page.index + 1,
            size=page_size,
            hydrate=False,
        )

    return all_runs


def extract_project_data(
    client: Client,
    project: Any,
    year: int,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict | None:
    """Extract all data for a single project. Returns None if no runs in target year."""
    project_id = str(project.id)
    project_name = project.name

    # Fetch runs for target year
    runs = fetch_runs_for_year(client, year, project_id, page_size)
    if not runs:
        return None  # Skip projects with no runs

    # Fetch project-scoped data
    pipelines = depaginate(lambda **kw: client.list_pipelines(project=project_id, **kw), size=page_size)
    artifacts = depaginate(lambda **kw: client.list_artifacts(project=project_id, **kw), size=page_size)
    models = depaginate(lambda **kw: client.list_models(project=project_id, **kw), size=page_size)
    schedules = depaginate(lambda **kw: client.list_schedules(project=project_id, **kw), size=page_size)
    services = depaginate(lambda **kw: client.list_services(project=project_id, **kw), size=page_size)

    return {
        "project_id": project_id,
        "project_name": project_name,
        "display_name": project.display_name if hasattr(project, 'display_name') else project_name,
        "runs": runs,
        "pipelines": pipelines,
        "artifacts": artifacts,
        "models": models,
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


def compute_project_stats(
    project_data: dict,
    year: int,
    pipeline_name_by_id: dict[str, str],
    pipeline_id_by_name: dict[str, str],
) -> dict:
    """Compute statistics for a single project."""
    runs = project_data["runs"]
    artifacts = project_data["artifacts"]
    models = project_data["models"]

    total_runs = len(runs)
    status_counts = Counter(normalize_status(run.status) for run in runs)
    completed = status_counts.get("completed", 0)
    failed = status_counts.get("failed", 0)

    # Get unique pipelines and users for this project
    pipeline_ids = set()
    user_ids = set()
    for run in runs:
        pipeline_id, _ = get_pipeline_id_and_name(run, pipeline_name_by_id, pipeline_id_by_name)
        if pipeline_id:
            pipeline_ids.add(pipeline_id)
        user_id = get_user_id(run)
        if user_id:
            user_ids.add(user_id)

    # Filter artifacts and models for target year
    artifacts_year = [a for a in artifacts if is_target_year(parse_datetime(a.created), year)]
    models_year = [m for m in models if is_target_year(parse_datetime(m.created), year)]

    # Compute runs per month for MoM growth
    runs_per_month = [0] * 12
    for run in runs:
        created = parse_datetime(run.created)
        if created and created.year == year:
            runs_per_month[created.month - 1] += 1

    # Calculate month-over-month growth
    mom_growth = calculate_mom_growth(runs_per_month)

    return {
        "id": project_data["project_id"],
        "name": project_data["project_name"],
        "display_name": project_data["display_name"],
        "total_runs": total_runs,
        "successful_runs": completed,
        "failed_runs": failed,
        "success_rate": round(completed / total_runs * 100, 1) if total_runs > 0 else 0,
        "unique_pipelines": len(pipeline_ids),
        "unique_users": len(user_ids),
        "artifacts_produced": len(artifacts_year),
        "models_created": len(models_year),
        "runs_per_month": runs_per_month,
        "month_over_month_growth": mom_growth,
    }


def calculate_mom_growth(runs_per_month: list[int]) -> float:
    """Calculate average month-over-month growth percentage."""
    growth_rates = []
    for i in range(1, len(runs_per_month)):
        if runs_per_month[i-1] > 0:
            growth = (runs_per_month[i] - runs_per_month[i-1]) / runs_per_month[i-1] * 100
            growth_rates.append(growth)

    return round(sum(growth_rates) / len(growth_rates), 1) if growth_rates else 0


def compute_workspace_core_stats(
    all_runs: list,
    all_artifacts: list,
    all_models: list,
    stacks: list,
    all_schedules: list,
    all_services: list,
    pipeline_name_by_id: dict[str, str],
    pipeline_id_by_name: dict[str, str],
    year: int,
    active_project_count: int,
) -> dict:
    """Compute workspace-wide core statistics."""
    total_runs = len(all_runs)

    status_counts = Counter(normalize_status(run.status) for run in all_runs)
    completed = status_counts.get("completed", 0)
    failed = status_counts.get("failed", 0)

    pipeline_ids = set()
    user_ids = set()
    for run in all_runs:
        pipeline_id, _ = get_pipeline_id_and_name(run, pipeline_name_by_id, pipeline_id_by_name)
        if pipeline_id:
            pipeline_ids.add(pipeline_id)
        user_id = get_user_id(run)
        if user_id:
            user_ids.add(user_id)

    # Filter for target year
    artifacts_year = [a for a in all_artifacts if is_target_year(parse_datetime(a.created), year)]
    models_year = [m for m in all_models if is_target_year(parse_datetime(m.created), year)]

    # Sum model versions
    total_model_versions = sum(get_model_version_count(m) for m in models_year)

    # Active schedules and services
    active_schedules = len([s for s in all_schedules if s.active])
    active_services = len([s for s in all_services if s.state == ServiceState.ACTIVE])

    return {
        "total_runs": total_runs,
        "successful_runs": completed,
        "failed_runs": failed,
        "success_rate": round(completed / total_runs * 100, 1) if total_runs > 0 else 0,
        "unique_pipelines": len(pipeline_ids),
        "unique_users": len(user_ids),
        "artifacts_produced": len(artifacts_year),
        "models_created": len(models_year),
        "model_versions": total_model_versions,
        "total_stacks": len(stacks),
        "active_schedules": active_schedules,
        "active_services": active_services,
        "active_projects": active_project_count,
        "status_breakdown": dict(status_counts),
    }


def compute_time_analytics(runs: list) -> dict:
    """Compute time-based analytics from ZenML response objects."""
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

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

    if not runs:
        return empty_time_analytics

    run_times = []
    for run in runs:
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
    runs: list,
    users: list,
    pipeline_name_by_id: dict[str, str],
    pipeline_id_by_name: dict[str, str],
) -> list:
    """Compute per-user statistics from ZenML response objects."""
    user_lookup = {str(user.id): user for user in users}

    user_runs = defaultdict(list)
    for run in runs:
        user_id = get_user_id(run)
        if user_id:
            user_runs[user_id].append(run)

    user_stats = []
    for user_id, user_run_list in user_runs.items():
        user = user_lookup.get(user_id)
        if not user or getattr(user, "is_service_account", False):
            continue

        total = len(user_run_list)
        failed = sum(1 for r in user_run_list if normalize_status(r.status) == "failed")
        completed = sum(1 for r in user_run_list if normalize_status(r.status) == "completed")

        hours = []
        for r in user_run_list:
            created = parse_datetime(r.created)
            if created:
                hours.append(created.hour)
        avg_hour = sum(hours) / len(hours) if hours else 12

        weekend = 0
        for r in user_run_list:
            created = parse_datetime(r.created)
            if created and created.weekday() >= 5:
                weekend += 1

        pipeline_ids = set()
        for r in user_run_list:
            pipeline_id, _ = get_pipeline_id_and_name(r, pipeline_name_by_id, pipeline_id_by_name)
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
            "success_rate": round(completed / total * 100, 1) if total > 0 else 0,
            "avg_hour": round(avg_hour, 1),
            "weekend_runs": weekend,
            "unique_pipelines": len(pipeline_ids),
        })

    return sorted(user_stats, key=lambda x: x["total_runs"], reverse=True)


def compute_top_pipelines(
    runs: list,
    pipeline_name_by_id: dict[str, str],
    pipeline_id_by_name: dict[str, str],
    pipeline_project_by_id: dict[str, str],
) -> list:
    """Compute top pipelines by run count, including project info."""
    pipeline_info: dict[str, dict] = {}
    pipeline_counts = Counter()

    for run in runs:
        pipeline_id, pipeline_name = get_pipeline_id_and_name(run, pipeline_name_by_id, pipeline_id_by_name)
        if pipeline_id:
            pipeline_counts[pipeline_id] += 1
            if pipeline_id not in pipeline_info:
                pipeline_info[pipeline_id] = {
                    "name": pipeline_name or pipeline_name_by_id.get(pipeline_id) or pipeline_id[:8],
                    "project": pipeline_project_by_id.get(pipeline_id, "Unknown"),
                }

    # Filter out "unlisted" pipelines (system designation for runs without explicit pipeline name)
    excluded_names = {"unlisted"}

    top_pipelines = []
    for pipe_id, count in pipeline_counts.most_common():
        info = pipeline_info.get(pipe_id, {"name": pipe_id[:8], "project": "Unknown"})

        # Skip pipelines with excluded names
        if info["name"].lower() in excluded_names:
            continue

        top_pipelines.append({
            "name": info["name"],
            "project": info["project"],
            "runs": count,
        })

        # Stop after collecting 10 pipelines
        if len(top_pipelines) >= 10:
            break

    return top_pipelines


def compute_project_leaderboards(projects: list[dict]) -> dict[str, list[str]]:
    """Compute project leaderboards (ranked lists of project names)."""
    if not projects:
        return {"most_runs": [], "highest_success_rate": [], "most_users": []}

    # Most runs
    by_runs = sorted(projects, key=lambda p: p["total_runs"], reverse=True)

    # Highest success rate (min 10 runs to qualify)
    qualified = [p for p in projects if p["total_runs"] >= 10]
    by_success = sorted(qualified, key=lambda p: p["success_rate"], reverse=True) if qualified else by_runs

    # Most users
    by_users = sorted(projects, key=lambda p: p["unique_users"], reverse=True)

    return {
        "most_runs": [p["name"] for p in by_runs],
        "highest_success_rate": [p["name"] for p in by_success],
        "most_users": [p["name"] for p in by_users],
    }


def compute_awards(user_stats: list, runs: list, projects: list[dict]) -> dict:
    """Compute awards for users and projects."""
    awards = {}

    # === User Awards ===
    if user_stats:
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
            key=lambda x: x["avg_hour"] if x["avg_hour"] >= 18 or x["avg_hour"] <= 6 else 0,
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

    # === Project Awards ===
    if projects:
        # Workhorse - most total runs
        workhorse = max(projects, key=lambda p: p["total_runs"])
        awards["workhorse_project"] = {
            "title": "Workhorse",
            "icon": "🏋️",
            "description": "The project that never sleeps",
            "project": workhorse["name"],
            "value": f"{workhorse['total_runs']:,} runs",
        }

        # Rising Star - highest MoM growth (min 20 total runs to qualify)
        growth_qualified = [p for p in projects if p["total_runs"] >= 20]
        if growth_qualified:
            rising_star = max(growth_qualified, key=lambda p: p["month_over_month_growth"])
            if rising_star["month_over_month_growth"] > 0:
                awards["rising_star_project"] = {
                    "title": "Rising Star",
                    "icon": "📈",
                    "description": "Biggest growth this year",
                    "project": rising_star["name"],
                    "value": f"+{rising_star['month_over_month_growth']}% MoM growth",
                }

    return awards


def generate_fun_facts(
    time_analytics: dict,
    core_stats: dict,
    user_stats: list,
    top_pipelines: list,
    projects: list[dict],
) -> dict[str, list[str]]:
    """Generate both specific and generic fun facts."""
    specific_facts = []
    generic_facts = []

    # First run fact
    if time_analytics.get("first_run"):
        first = datetime.fromisoformat(time_analytics["first_run"])
        specific_facts.append(
            f"Your team's first run of the year was on {first.strftime('%B %d')} at {first.strftime('%H:%M')}"
        )
        generic_facts.append(
            f"Your team's first run was on {first.strftime('%B %d')} at {first.strftime('%H:%M')}"
        )

    # Busiest day
    if time_analytics.get("busiest_day"):
        specific_facts.append(
            f"{time_analytics['busiest_day']} was your most productive day with "
            f"{time_analytics['busiest_day_count']} runs"
        )
        generic_facts.append(
            f"{time_analytics['busiest_day']} was your most productive day with "
            f"{time_analytics['busiest_day_count']} runs"
        )

    # Busiest month
    if time_analytics.get("busiest_month"):
        specific_facts.append(
            f"{time_analytics['busiest_month']} was your busiest month with "
            f"{time_analytics['busiest_month_count']} pipeline runs"
        )
        generic_facts.append(
            f"{time_analytics['busiest_month']} was your busiest month with "
            f"{time_analytics['busiest_month_count']} pipeline runs"
        )

    # Weekend runs
    if time_analytics.get("weekend_runs", 0) > 0:
        pct = round(
            time_analytics["weekend_runs"]
            / (time_analytics["weekend_runs"] + time_analytics["weekday_runs"])
            * 100
        )
        fact = f"{pct}% of your runs happened on weekends 🎉"
        specific_facts.append(fact)
        generic_facts.append(fact)

    # Top pipeline (specific has name, generic doesn't)
    if top_pipelines:
        specific_facts.append(
            f"Your most-run pipeline was '{top_pipelines[0]['name']}' "
            f"with {top_pipelines[0]['runs']} executions"
        )
        generic_facts.append(
            f"Your most popular workflow was executed {top_pipelines[0]['runs']} times"
        )

    # Success rate
    if core_stats.get("success_rate", 0) >= 90:
        fact = f"Your team achieved a {core_stats['success_rate']}% success rate - impressive! 🎯"
        specific_facts.append(fact)
        generic_facts.append(fact)
    elif core_stats.get("success_rate", 0) >= 70:
        fact = f"Your team's success rate was {core_stats['success_rate']}% - solid experimentation!"
        specific_facts.append(fact)
        generic_facts.append(fact)

    # Total runs milestones
    total = core_stats.get("total_runs", 0)
    if total >= 1000:
        fact = "You crossed the 1,000 pipeline runs milestone! 🚀"
        specific_facts.append(fact)
        generic_facts.append(fact)
    elif total >= 500:
        fact = "You ran over 500 pipelines this year!"
        specific_facts.append(fact)
        generic_facts.append(fact)
    elif total >= 100:
        specific_facts.append(f"You hit triple digits with {total} pipeline runs!")
        generic_facts.append(f"You hit triple digits with {total} pipeline runs!")

    # Models created
    if core_stats.get("models_created", 0) > 0:
        fact = (
            f"Your team created {core_stats['models_created']} models "
            f"with {core_stats['model_versions']} total versions"
        )
        specific_facts.append(fact)
        generic_facts.append(fact)

    # Project-related facts (specific only)
    if len(projects) > 1:
        top_project = max(projects, key=lambda p: p["total_runs"])
        specific_facts.append(
            f"Your '{top_project['name']}' project led the way with {top_project['total_runs']} runs!"
        )
        generic_facts.append(
            f"Your top project led the way with {top_project['total_runs']} runs!"
        )

    return {
        "specific": specific_facts,
        "generic": generic_facts,
    }


def generate_anonymization_mapping(
    project_names: list[str],
    pipeline_names: list[str],
) -> dict[str, dict[str, str]]:
    """Generate codename mappings for anonymization."""
    # Combine all codename lists and shuffle
    all_codenames = CODENAMES_SPACE + CODENAMES_MYTH + CODENAMES_NATURE
    random.shuffle(all_codenames)

    # Generate project mappings
    project_mapping = {}
    for i, name in enumerate(sorted(set(project_names))):
        if i < len(all_codenames):
            project_mapping[name] = f"Project {all_codenames[i]}"
        else:
            project_mapping[name] = f"Project {i + 1}"

    # Reshuffle for pipelines to get different assignments
    random.shuffle(all_codenames)

    # Generate pipeline mappings
    pipeline_mapping = {}
    for i, name in enumerate(sorted(set(pipeline_names))):
        if i < len(all_codenames):
            pipeline_mapping[name] = f"Pipeline {all_codenames[i]}"
        else:
            pipeline_mapping[name] = f"Pipeline {i + 1}"

    return {
        "projects": project_mapping,
        "pipelines": pipeline_mapping,
    }


def run_extraction(
    output_path: str,
    year: int,
    exclude_projects: set[str],
    page_size: int,
) -> None:
    """Main extraction logic for workspace-wide metrics."""
    print("=" * 65)
    print("  ZenML Unwrapped - Workspace Extraction")
    print("=" * 65)

    print("\n🔌 Connecting to ZenML server...")
    client = Client()
    print("✅ Connected to ZenML server")

    # Get workspace info
    workspace_name = "default"
    try:
        workspace_name = client.active_workspace.name
    except Exception:
        pass

    # Fetch all projects
    print(f"\n📂 Fetching projects from workspace: {workspace_name}")
    projects = fetch_projects(client, exclude_projects)
    total_projects = len(projects) + len(exclude_projects)

    if exclude_projects:
        print(f"   Found {total_projects} projects (excluding {len(exclude_projects)}: {', '.join(exclude_projects)})")
    else:
        print(f"   Found {len(projects)} projects")

    if not projects:
        print("\n⚠️  No projects found. Exiting.")
        return

    # Fetch workspace-level data (users and stacks)
    print("\n📊 Fetching workspace-level data...")
    print("  → Fetching users...")
    users = depaginate(client.list_users, size=page_size)
    print(f"    Found {len(users)} users")

    print("  → Fetching stacks...")
    stacks = depaginate(client.list_stacks, size=page_size)
    print(f"    Found {len(stacks)} stacks")

    # Process each project
    print(f"\n🔄 Processing {len(projects)} projects...")
    project_data_list = []
    all_runs = []
    all_artifacts = []
    all_models = []
    all_schedules = []
    all_services = []
    all_pipelines = []
    failed_projects = []
    skipped_projects = []

    for i, project in enumerate(projects, 1):
        print(f"\n  [{i}/{len(projects)}] Project: {project.name}")
        try:
            data = extract_project_data(client, project, year, page_size)
            if data is None:
                print(f"         ⚠️  0 runs in {year}, skipping")
                skipped_projects.append(project.name)
                continue

            print(f"         → {len(data['runs'])} runs from {year}")
            print(f"         → {len(data['pipelines'])} pipelines, {len(data['models'])} models")

            project_data_list.append(data)
            all_runs.extend(data["runs"])
            all_artifacts.extend(data["artifacts"])
            all_models.extend(data["models"])
            all_schedules.extend(data["schedules"])
            all_services.extend(data["services"])
            all_pipelines.extend(data["pipelines"])

        except Exception as e:
            print(f"         ❌ Error: {e} (skipped)")
            failed_projects.append((project.name, str(e)))

    if not project_data_list:
        print(f"\n⚠️  No projects with runs in {year}. Exiting.")
        return

    # Build pipeline maps from all pipelines
    pipeline_name_by_id, pipeline_id_by_name, pipeline_project_by_id = build_pipeline_maps(all_pipelines)

    # Also populate project info for pipelines from project context
    for data in project_data_list:
        for pipeline in data["pipelines"]:
            pipeline_project_by_id[str(pipeline.id)] = data["project_name"]

    print("\n🧮 Computing metrics...")

    # Compute per-project stats
    print("  → Project statistics...")
    project_stats = []
    for data in project_data_list:
        stats = compute_project_stats(data, year, pipeline_name_by_id, pipeline_id_by_name)
        project_stats.append(stats)

    # Compute workspace-wide stats
    print("  → Workspace statistics...")
    core_stats = compute_workspace_core_stats(
        all_runs, all_artifacts, all_models, stacks,
        all_schedules, all_services,
        pipeline_name_by_id, pipeline_id_by_name,
        year, len(project_stats)
    )

    print("  → Time analytics...")
    time_analytics = compute_time_analytics(all_runs)

    print("  → User statistics...")
    user_stats = compute_user_stats(all_runs, users, pipeline_name_by_id, pipeline_id_by_name)

    print("  → Top pipelines...")
    top_pipelines = compute_top_pipelines(
        all_runs, pipeline_name_by_id, pipeline_id_by_name, pipeline_project_by_id
    )

    print("  → Project leaderboards...")
    project_leaderboards = compute_project_leaderboards(project_stats)

    print("  → Awards...")
    awards = compute_awards(user_stats, all_runs, project_stats)

    print("  → Fun facts...")
    fun_facts = generate_fun_facts(time_analytics, core_stats, user_stats, top_pipelines, project_stats)

    print("  → Anonymization mappings...")
    project_names = [p["name"] for p in project_stats]
    pipeline_names = [p["name"] for p in top_pipelines]
    anonymized = generate_anonymization_mapping(project_names, pipeline_names)

    # Build final metrics object (schema v2.0)
    metrics = {
        "schema_version": "2.0",
        "generated_at": datetime.now().isoformat(),
        "year": year,
        "workspace": {
            "name": workspace_name,
            "project_count": len(project_stats),
            "excluded_projects": list(exclude_projects),
        },
        "core_stats": core_stats,
        "time_analytics": time_analytics,
        "projects": project_stats,
        "project_leaderboards": project_leaderboards,
        "top_pipelines": top_pipelines,
        "awards": awards,
        "users": user_stats,
        "fun_facts": fun_facts,
        "anonymized": anonymized,
    }

    # Write output
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with open(output, "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    print(f"\n✅ Metrics saved to {output}")

    # Summary
    print("\n" + "=" * 65)
    print("  Summary")
    print("=" * 65)
    print(f"  Projects processed: {len(project_stats)} / {len(projects)}")
    if skipped_projects:
        print(f"  Projects skipped (no runs): {len(skipped_projects)}")
    if failed_projects:
        print(f"  Projects failed: {len(failed_projects)}")
        for name, error in failed_projects:
            print(f"    - {name}: {error}")
    print(f"\n  Total runs: {core_stats['total_runs']:,}")
    print(f"  Success rate: {core_stats['success_rate']}%")
    print(f"  Unique users: {core_stats['unique_users']}")
    print(f"  Unique pipelines: {core_stats['unique_pipelines']}")
    print(f"  Awards generated: {len(awards)}")
    print(f"  Fun facts: {len(fun_facts['specific'])}")
    print("=" * 65)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract ZenML Unwrapped metrics (workspace-wide)"
    )
    parser.add_argument(
        "--output", "-o",
        default="data/metrics.json",
        help="Output path for metrics.json (default: data/metrics.json)",
    )
    parser.add_argument(
        "--year", "-y",
        type=int,
        default=2025,
        help="Year to extract metrics for (default: 2025)",
    )
    parser.add_argument(
        "--exclude-projects",
        type=str,
        default="",
        help="Comma-separated project names to exclude (e.g., 'test-project,ci-automation')",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Page size for ZenML list calls (default: {DEFAULT_PAGE_SIZE})",
    )
    args = parser.parse_args()

    # Parse exclude list
    exclude_projects = set()
    if args.exclude_projects:
        exclude_projects = {name.strip() for name in args.exclude_projects.split(",") if name.strip()}

    run_extraction(args.output, args.year, exclude_projects, args.page_size)


if __name__ == "__main__":
    main()
