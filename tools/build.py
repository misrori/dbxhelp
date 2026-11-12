#!/usr/bin/env python3
"""Build the GitHub Pages site from catalog.json.

- index.html                                   landing page with copy-ready load commands
- modules/<module>/<topic>/<topic>.html        Document / Presentation viewer generated from the topic's .md

Usage:  python3 tools/build.py
"""
import html
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATES = ROOT / "tools" / "templates"


def main():
    catalog = json.loads((ROOT / "catalog.json").read_text(encoding="utf-8"))
    repo, branch = catalog["repo"], catalog["branch"]
    topic_tpl = (TEMPLATES / "topic.html").read_text(encoding="utf-8")

    for module in catalog["modules"]:
        for topic in module["topics"]:
            folder = ROOT / module["folder"] / topic["folder"]
            if not (topic.get("doc") and topic.get("html")):
                continue
            md = (folder / topic["doc"]).read_text(encoding="utf-8")
            home = os.path.relpath(ROOT / "index.html", folder)
            out = (topic_tpl
                   .replace("{{TITLE}}", html.escape(topic["title"]))
                   .replace("{{MODULE}}", html.escape(module["title"]))
                   .replace("{{HOME}}", home)
                   .replace("{{MD_FILE}}", topic["doc"])
                   .replace("{{GITHUB_URL}}", f"https://github.com/{repo}/tree/{branch}/{module['folder']}/{topic['folder']}")
                   .replace("{{MARKDOWN}}", md.replace("</script", "<\\/script")))
            (folder / topic["html"]).write_text(out, encoding="utf-8")
            print(f"built {(folder / topic['html']).relative_to(ROOT)}")

    index = ((TEMPLATES / "index.html").read_text(encoding="utf-8")
             .replace("{{REPO}}", repo)
             .replace("{{CATALOG_JSON}}", json.dumps(catalog, ensure_ascii=False).replace("</", "<\\/")))
    (ROOT / "index.html").write_text(index, encoding="utf-8")
    print("built index.html")


if __name__ == "__main__":
    main()
