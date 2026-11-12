#!/usr/bin/env python3
"""Build generated files from catalog.json.

Per topic (source: <doc>.md + optional <quiz>.json):
  - <doc>.md     "Check yourself" section kept in sync with the quiz (between quiz markers)
  - <html>       self-contained animated presentation (slides + document view + quiz)
  - notebooks with "from_doc": true -> styled %md-sandbox notebook (hero, presentation launcher,
                 one cell per section, ```python run / ```sql run blocks as runnable cells, quiz)
Plus index.html: the GitHub Pages "copy the command" page.

Usage:  python3 tools/build.py
"""
import html
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import render  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
TEMPLATES = ROOT / "tools" / "templates"
QUIZ_START, QUIZ_END = "<!-- quiz:start -->", "<!-- quiz:end -->"
QUIZ_BLOCK = re.compile(re.escape(QUIZ_START) + r".*?" + re.escape(QUIZ_END) + r"\n?", re.S)
NB_CSS = (TEMPLATES / "notebook.css").read_text(encoding="utf-8").strip()
QUIZ_JS = (TEMPLATES / "quiz.js").read_text(encoding="utf-8").strip()


def quiz_markdown(quiz):
    out = [QUIZ_START, "## Check yourself", ""]
    for n, q in enumerate(quiz, 1):
        out += [f"**{n}. {q['q']}**", ""]
        out += [f"- {chr(97 + k)}) {o}" for k, o in enumerate(q["o"])]
        out += ["", "<details><summary>Answer</summary>", "", f"**{chr(97 + q['c'])})** {q['e']}", "", "</details>", ""]
    out.append(QUIZ_END)
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------- notebook
def magic(text):
    return "\n".join("# MAGIC " + l if l else "# MAGIC" for l in text.splitlines())


def sandbox(content_html, script=""):
    """A %md-sandbox cell: no blank lines (they would end the HTML block), no bare $ (KaTeX)."""
    body = content_html.replace("$", "&#36;")
    body = "\n".join(l if l.strip() else "<span></span>" for l in body.splitlines())
    return "%md-sandbox\n<style>" + NB_CSS.replace("\n", "") + "</style>\n<div class=\"dbx\">\n" + body + "\n</div>" + (("\n" + script) if script else "")


def code_cell(lang, code):
    first = code.strip().splitlines()[0] if code.strip() else ""
    title = re.sub(r"^(#|--)\s*", "", first).strip() if first.startswith(("#", "--")) else ""
    head = f"# DBTITLE 1,{title}\n" if title else ""
    if lang == "sql":
        return head + magic("%sql\n" + code)
    return head + code


def notebook_from_md(body, module, topic, quiz, raw_html_url):
    sections = render.split_sections(body)
    cells = [sandbox(render.hero_html(sections[0], module["title"]))]
    if topic.get("html"):
        cells.append(sandbox(
            '<div class="present"><span class="big">🎞️</span><div><b>Interactive presentation</b><br>'
            '<span>Run the next cell to open the animated slides (and a document view) right here. '
            'The same HTML file sits next to this notebook. You can download it and open it in a browser for full screen.</span></div></div>'))
        cells.append(
            "# DBTITLE 1,🎞️ Open the interactive presentation\n"
            "import os, urllib.request\n"
            f'_file = "{topic["html"]}"\n'
            f'_url = "{raw_html_url}"\n'
            '_html = open(_file, encoding="utf-8").read() if os.path.exists(_file) else urllib.request.urlopen(_url).read().decode("utf-8")\n'
            "displayHTML(_html)")
    for n, sec in enumerate(sections[1:], 1):
        title = render.section_title(sec)
        blocks = render.split_blocks("\n".join(sec.splitlines()[1:]))
        head_done = False
        for b in blocks:
            if b[0] == "md":
                head = "" if head_done else render.section_head(title, n)
                cells.append(sandbox(head + render.md_to_html(b[1])))
                head_done = True
            else:
                if not head_done:
                    cells.append(sandbox(render.section_head(title, n)))
                    head_done = True
                cells.append(code_cell(b[1], b[2]))
    if quiz:
        script = ("<script>\n" + QUIZ_JS + "\ndbxQuiz('dbxquiz', " + json.dumps(quiz, ensure_ascii=False) + ");\n</script>")
        cells.append(sandbox(render.section_head("Check yourself", len(sections)) +
                             '<div id="dbxquiz" style="max-width:820px"></div>', script))
    cells.append(sandbox(
        f'<div class="footer">📦 <b>dbxhelp</b> · {html.escape(module["title"])} · {html.escape(topic["title"])} · '
        f'<a href="https://misrori.github.io/dbxhelp/" target="_blank">more materials</a> · '
        f'<a href="https://github.com/misrori/dbxhelp" target="_blank">source on GitHub</a></div>'))
    return "# Databricks notebook source\n" + "\n\n# COMMAND ----------\n\n".join(
        c if not c.startswith("%md-sandbox") else magic(c) for c in cells) + "\n"


# ---------------------------------------------------------------- html presentation
def slides_from_md(body, module):
    sections = render.split_sections(body)
    slides = [{"title": render.section_title(sections[0]), "icon": "🚀", "html": render.hero_html(sections[0], module["title"])}]
    for n, sec in enumerate(sections[1:], 1):
        title = render.section_title(sec)
        slides.append({"title": render.md_inline(title), "icon": render.icon_for(title),
                       "html": render.section_head(title, n) + render.section_body(sec)})
    return slides


def js_json(obj):
    return json.dumps(obj, ensure_ascii=False).replace("</", "<\\/")


def main():
    catalog = json.loads((ROOT / "catalog.json").read_text(encoding="utf-8"))
    repo, branch = catalog["repo"], catalog["branch"]
    topic_tpl = (TEMPLATES / "topic.html").read_text(encoding="utf-8")

    for module in catalog["modules"]:
        for topic in module["topics"]:
            folder = ROOT / module["folder"] / topic["folder"]
            if not topic.get("doc"):
                continue
            doc_path = folder / topic["doc"]
            md = doc_path.read_text(encoding="utf-8")
            quiz = json.loads((folder / topic["quiz"]).read_text(encoding="utf-8")) if topic.get("quiz") else None
            if quiz:
                block = quiz_markdown(quiz)
                new_md = QUIZ_BLOCK.sub(lambda _: block, md) if QUIZ_START in md else md.rstrip() + "\n\n" + block
                if new_md != md:
                    doc_path.write_text(new_md, encoding="utf-8")
                    md = new_md
            body = QUIZ_BLOCK.sub("", md).strip() + "\n"
            rel = f"{module['folder']}/{topic['folder']}"

            if topic.get("html"):
                out = (topic_tpl
                       .replace("{{TITLE}}", html.escape(topic["title"]))
                       .replace("{{MODULE}}", html.escape(module["title"]))
                       .replace("{{NB_CSS}}", NB_CSS)
                       .replace("{{QUIZ_JS}}", QUIZ_JS)
                       .replace("{{QUIZ_JSON}}", js_json(quiz or []))
                       .replace("{{SLIDES_JSON}}", js_json(slides_from_md(body, module))))
                (folder / topic["html"]).write_text(out, encoding="utf-8")
                print(f"built {rel}/{topic['html']}")

            raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{rel}/{topic.get('html', '')}"
            for nb in topic.get("notebooks", []):
                if nb.get("from_doc"):
                    (folder / nb["file"]).write_text(notebook_from_md(body, module, topic, quiz, raw_url), encoding="utf-8")
                    print(f"built {rel}/{nb['file']}")

    index = ((TEMPLATES / "index.html").read_text(encoding="utf-8")
             .replace("{{REPO}}", repo)
             .replace("{{CATALOG_JSON}}", js_json(catalog)))
    (ROOT / "index.html").write_text(index, encoding="utf-8")
    print("built index.html")


if __name__ == "__main__":
    main()
