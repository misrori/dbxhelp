"""dbxhelp loader: import supplementary notebooks into your Databricks workspace.

Run inside any Databricks notebook cell (Serverless works):

    import urllib.request as u; exec(u.urlopen("https://raw.githubusercontent.com/misrori/dbxhelp/main/dbxhelp.py").read().decode()); dbxhelp("genie/metric-views")

    dbxhelp()                         # list available topics
    dbxhelp("genie/metric-views")     # import one topic
    dbxhelp("genie")                  # import every topic of a module
    dbxhelp("metric-views", target="/Workspace/Shared/dbxhelp")   # custom target folder
"""
import base64
import json
import urllib.parse
import urllib.request

DBXHELP_REPO = "misrori/dbxhelp"
DBXHELP_BRANCH = "main"


def _dbxhelp_fetch(path, branch):
    url = f"https://raw.githubusercontent.com/{DBXHELP_REPO}/{branch}/" + urllib.parse.quote(path)
    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.read()


def dbxhelp(topic=None, target=None, overwrite=True, include_docs=True, branch=DBXHELP_BRANCH):
    catalog = json.loads(_dbxhelp_fetch("catalog.json", branch))
    entries = [(m, t) for m in catalog["modules"] for t in m["topics"]]

    if not topic:
        print("Available topics (use dbxhelp('<module>/<topic>')):\n")
        for m, t in entries:
            print(f"  {m['id']}/{t['id']:<28} {m['title']} > {t['title']}")
        return None

    selected = [(m, t) for m, t in entries
                if topic in (f"{m['id']}/{t['id']}", t["id"], m["id"])]
    if not selected:
        raise ValueError(f"Unknown topic '{topic}'. Run dbxhelp() to list topics.")

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat, Language

    w = WorkspaceClient()
    base = (target or f"/Workspace/Users/{w.current_user.me().user_name}/dbxhelp").rstrip("/")
    languages = {"py": Language.PYTHON, "sql": Language.SQL, "scala": Language.SCALA, "r": Language.R}
    folders = []

    for m, t in selected:
        src = f"{m['folder']}/{t['folder']}"
        folder = f"{base}/{m['workspace_folder']}/{t['workspace_folder']}"
        w.workspace.mkdirs(folder)

        for nb in t["notebooks"]:
            content = _dbxhelp_fetch(f"{src}/{nb['file']}", branch)
            w.workspace.import_(
                path=f"{folder}/{nb['name']}",
                content=base64.b64encode(content).decode(),
                format=ImportFormat.SOURCE,
                language=languages[nb["file"].rsplit(".", 1)[-1].lower()],
                overwrite=overwrite,
            )
            print(f"  notebook  {folder}/{nb['name']}")

        if include_docs and t.get("doc"):
            content = _dbxhelp_fetch(f"{src}/{t['doc']}", branch)
            w.workspace.import_(
                path=f"{folder}/{t['doc']}",
                content=base64.b64encode(content).decode(),
                format=ImportFormat.AUTO,
                overwrite=overwrite,
            )
            print(f"  file      {folder}/{t['doc']}")

        folders.append((t["title"], folder))

    host = (w.config.host or "").rstrip("/")
    links = []
    for title, folder in folders:
        try:
            url = f"{host}/browse/folders/{w.workspace.get_status(folder).object_id}"
        except Exception:
            url = None
        links.append((title, folder, url))

    try:
        html = "".join(
            f'<div style="margin:4px 0">&#9989; <b>{title}</b>: '
            + (f'<a href="{url}" target="_blank">{folder}</a>' if url else folder) + "</div>"
            for title, folder, url in links
        )
        displayHTML(html)  # noqa: F821, available in Databricks notebooks
    except NameError:
        for title, folder, url in links:
            print(f"Done: {title} -> {url or folder}")
    return [folder for _, folder, _ in links]
