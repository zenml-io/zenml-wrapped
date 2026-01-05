# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ZenML Wrapped is a "Spotify Wrapped"-style year-in-review visualization for ZenML projects. It extracts pipeline metrics from a ZenML server and displays them as an interactive static webpage.

## Architecture

```
extract_metrics.py  →  data/metrics.json  →  index.html + scripts/app.js
     (Python)              (JSON)                  (Static site)
```

- **extract_metrics.py**: Python script that connects to ZenML via the Python client, fetches 2025 pipeline data, and outputs `data/metrics.json`
- **index.html**: Single-page static site with embedded sections (hero, stats, timeline, awards, etc.)
- **scripts/app.js**: Loads `data/metrics.json` and populates the HTML with animated counters, charts, and awards
- **styles/main.css**: All styling (no build step needed)

## Commands

```bash
# Setup
uv sync
source .venv/bin/activate

# Connect to ZenML
zenml login
zenml project set <project-name>  # optional

# Extract metrics
python extract_metrics.py
python extract_metrics.py --year 2024  # different year
python extract_metrics.py --output custom/path.json

# Serve locally
python -m http.server 8000
# or: npx serve .
```

## Key Data Structures

The `data/metrics.json` file contains:
- `core_stats`: total_runs, success_rate, unique_pipelines, artifacts_produced, models_created, etc.
- `time_analytics`: runs_per_month (array of 12), busiest_month/day/hour
- `top_pipelines`: array of {name, runs}
- `awards`: keyed by award type (pipeline_overlord, night_owl, weekend_warrior, etc.)
- `users`: per-user stats including avatar, run counts, success rates
- `fun_facts`: array of generated insight strings

## Notes

- The `design/` folder is in .gitignore — never commit files from there
- No build step required — pure static HTML/CSS/JS
- Requires ZenML ≥0.93.0
