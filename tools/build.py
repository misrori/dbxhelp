#!/usr/bin/env python3
"""Build generated files from catalog.json.

For every topic:
  - <doc>.md            gets a "Check yourself" section rendered from <quiz>.json (between quiz markers)
  - <html>              Document / Presentation viewer (markdown + interactive quiz)
  - notebooks with "from_doc": true are generated from the markdown (one %md cell per "## " section + quiz cell)
Plus index.html, the GitHub Pages landing page.

Usage:  python3 tools/build.py
"""
import html
import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATES = ROOT / "tools" / "templates"
QUIZ_START, QUIZ_END = "<!-- quiz:start -->", "<!-- quiz:end -->"
QUIZ_BLOCK = re.compile(re.escape(QUIZ_START) + r".*?" + re.escape(QUIZ_END) + r"\n?", re.S)


def quiz_markdown(quiz):
    out = [QUIZ_START, "## Check yourself", ""]
    for n, q in enumerate(quiz, 1):
        out += [f"**{n}. {q['q']}**", ""]
        out += [f"- {chr(97 + k)}) {o}" for k, o in enumerate(q["o"])]
        out += ["", "<details><summary>Answer</summary>", "",
                f"**{chr(97 + q['c'])})** {q['e']}", "", "</details>", ""]
    out.append(QUIZ_END)
    return "\n".join(out) + "\n"


def split_sections(md):
    sections, cur, fence = [], [], False
    for line in md.splitlines():
        if line.lstrip().startswith("```"):
            fence = not fence
        if not fence and line.startswith("## ") and cur:
            sections.append("\n".join(cur).strip())
            cur = []
        cur.append(line)
    if cur:
        sections.append("\n".join(cur).strip())
    return [s for s in sections if s]


def magic(text):
    return "\n".join("# MAGIC " + l if l else "# MAGIC" for l in text.splitlines())


def notebook_from_md(md, module, topic, quiz, quiz_js):
    banner = f"""%md-sandbox
<div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
    <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
    <div style="color: #666">
        <span style="font-weight: bold; color: #333">{html.escape(module['title'])}</span>
        <span style="margin-left: 8px; color: #999">|</span>
        <span style="margin-left: 8px">{html.escape(topic['title'])}</span>
    </div>
</div>"""
    cells = [banner] + ["%md\n" + s for s in split_sections(md)]
    if quiz:
        cells.append("%md\n## Check yourself\n\nAnswer the questions below to check your understanding.")
        cells.append("%md-sandbox\n<div id=\"dbxquiz\" style=\"font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 760px\"></div>\n"
                     "<script>\n" + quiz_js.strip() + "\ndbxQuiz('dbxquiz', " + json.dumps(quiz, ensure_ascii=False) + ");\n</script>")
    return "# Databricks notebook source\n" + "\n\n# COMMAND ----------\n\n".join(magic(c) for c in cells) + "\n"


def main():
    catalog = json.loads((ROOT / "catalog.json").read_text(encoding="utf-8"))
    repo, branch = catalog["repo"], catalog["branch"]
    topic_tpl = (TEMPLATES / "topic.html").read_text(encoding="utf-8")
    quiz_js = (TEMPLATES / "quiz.js").read_text(encoding="utf-8")

    for module in catalog["modules"]:
        for topic in module["topics"]:
            folder = ROOT / module["folder"] / topic["folder"]
            if not topic.get("doc"):
                continue
            doc_path = folder / topic["doc"]
            md = doc_path.read_text(encoding="utf-8")
            quiz = json.loads((folder / topic["quiz"]).read_text(encoding="utf-8")) if topic.get("quiz") else None

            # 1. keep the markdown's "Check yourself" section in sync with the quiz file
            if quiz:
                block = quiz_markdown(quiz)
                new_md = QUIZ_BLOCK.sub(lambda _: block, md) if QUIZ_START in md else md.rstrip() + "\n\n" + block
                if new_md != md:
                    doc_path.write_text(new_md, encoding="utf-8")
                    md = new_md
            body = QUIZ_BLOCK.sub("", md).strip() + "\n"

            # 2. HTML viewer
            if topic.get("html"):
                out = (topic_tpl
                       .replace("{{TITLE}}", html.escape(topic["title"]))
                       .replace("{{MODULE}}", html.escape(module["title"]))
                       .replace("{{HOME}}", os.path.relpath(ROOT / "index.html", folder))
                       .replace("{{MD_FILE}}", topic["doc"])
                       .replace("{{GITHUB_URL}}", f"https://github.com/{repo}/tree/{branch}/{module['folder']}/{topic['folder']}")
                       .replace("{{QUIZ_JS}}", quiz_js)
                       .replace("{{QUIZ_JSON}}", json.dumps(quiz or [], ensure_ascii=False).replace("</", "<\\/"))
                       .replace("{{MARKDOWN}}", body.replace("</script", "<\\/script")))
                (folder / topic["html"]).write_text(out, encoding="utf-8")
                print(f"built {(folder / topic['html']).relative_to(ROOT)}")

            # 3. notebooks generated from the markdown
            for nb in topic.get("notebooks", []):
                if nb.get("from_doc"):
                    (folder / nb["file"]).write_text(notebook_from_md(body, module, topic, quiz, quiz_js), encoding="utf-8")
                    print(f"built {(folder / nb['file']).relative_to(ROOT)}")

    index = ((TEMPLATES / "index.html").read_text(encoding="utf-8")
             .replace("{{REPO}}", repo)
             .replace("{{CATALOG_JSON}}", json.dumps(catalog, ensure_ascii=False).replace("</", "<\\/")))
    (ROOT / "index.html").write_text(index, encoding="utf-8")
    print("built index.html")


if __name__ == "__main__":
    main()
