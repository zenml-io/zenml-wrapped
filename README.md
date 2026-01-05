# ZenML Wrapped

![](assets/cover.png)

Generate your team's personalized "Year in Review" for your entire ZenML workspace — see pipeline stats across all projects, top contributors, fun awards, and more.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (recommended) or pip
- Node.js (optional, for `npx serve`)

## Quick Start

### 1. Clone and set up your environment

```bash
git clone https://github.com/zenml-io/zenml-wrapped.git
cd zenml-wrapped
uv sync
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

<details>
<summary>Alternative: using pip</summary>

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install .
```
</details>

### 2. Connect to your ZenML workspace

```bash
zenml login
```

This will prompt you to select and authenticate with your ZenML server.

### 3. Extract your metrics

```bash
python extract_metrics.py
```

This extracts metrics from **all projects** in your workspace and generates `data/metrics.json`.

**Options:**
```bash
# Extract for a different year
python extract_metrics.py --year 2024

# Exclude specific projects (comma-separated)
python extract_metrics.py --exclude-projects "test-project,sandbox"
```

### 4. View your Wrapped

Serve the site locally:

**Using Python:**
```bash
python -m http.server 8000
```
Then open http://localhost:8000

**Using Node.js:**
```bash
npx serve .
```
Then open http://localhost:3000

## What You'll See

- **The Numbers** — Total runs, success rate, pipelines, artifacts, and models across your workspace
- **Project Leaderboards** — Rank projects by runs, success rate, or team size
- **Time Analytics** — Your busiest month, day, and hour
- **Top Pipelines** — Your most-run pipelines (with project labels)
- **Awards** — Fun recognition like "Pipeline Overlord", "Night Owl", "Weekend Warrior", plus project awards
- **Team Stats** — Per-user breakdown with individual achievements
- **Fun Facts** — Personalized insights about your ML journey
- **Share Cards** — Three downloadable card variants (Minimal, Standard, Detailed) for social sharing

## Anonymization

Use the 🔒 toggle in the top-right corner to switch between real names and codenames. This lets you share your stats publicly without revealing sensitive project or pipeline names.

When anonymized:
- Projects get codenames like "Nebula Station", "Phoenix Rising"
- Pipelines get codenames like "Operation Thunderbolt", "Protocol Zephyr"
- Fun facts switch to generic versions without specific names
