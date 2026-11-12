# dbxhelp: Databricks course supplements

Extra notebooks, slides and notes for Databricks courses. Each topic can be pulled into a Databricks workspace with **one line**, from a notebook running on **Serverless**.

🌐 **Site with copy-ready commands:** https://misrori.github.io/dbxhelp/

## Quick start (inside Databricks)

Open any Python notebook (Serverless), paste into a cell and run:

```python
import urllib.request as u; exec(u.urlopen("https://raw.githubusercontent.com/misrori/dbxhelp/main/dbxhelp.py").read().decode()); dbxhelp("genie/metric-views")
```

`dbxhelp()` without arguments lists all topics. Notebooks land in `/Workspace/Users/<you>/dbxhelp/<module>/<topic>/`.

## From a local terminal (git + Databricks CLI)

```bash
D="${TMPDIR:-/tmp}/dbxhelp"; (git -C "$D" pull -q 2>/dev/null || git clone -q --depth 1 https://github.com/misrori/dbxhelp.git "$D") && \
databricks workspace import-dir "$D/modules/genie-conversational-agents/metric-views" "/Workspace/Users/<you@company.com>/dbxhelp/Genie - Conversational Agents/Metric Views" --overwrite
```

Or use a **Git folder** in the UI: *Workspace → Create → Git folder → `https://github.com/misrori/dbxhelp`*.

## Topics

| Module | Topic | Contents |
|---|---|---|
| Building Reliable Conversational Agents with Genie | [Metric Views](modules/genie-conversational-agents/metric-views/) | Lecture notebook + knowledge check, Serverless hands-on demo, Markdown notes, HTML slides/document |

## Repository layout

```
catalog.json                     # registry of modules/topics (drives loader + site)
dbxhelp.py                       # loader executed inside Databricks
index.html                       # GitHub Pages landing page (generated)
modules/<module>/<topic>/
    01_lecture_*.sql|py          # Databricks notebook source (%md-sandbox styled)
    02_demo_*.py                 # runnable demo (Serverless)
    <topic>.md                   # notes, the single source for the HTML viewer
    <topic>.html                 # Document ⇄ Presentation viewer (generated)
tools/build.py                   # regenerates index.html and <topic>.html
tools/templates/                 # HTML templates
```

## Adding a new topic

1. Create `modules/<module>/<topic>/` with the notebooks (files must start with `# Databricks notebook source` or `-- Databricks notebook source`) and a `<topic>.md`.
2. Register it in `catalog.json`.
3. In the markdown, every `## ` heading becomes a new slide in presentation mode.
4. Run `python3 tools/build.py`, then commit and push. GitHub Pages serves the repo root (`main` branch, `/`).
