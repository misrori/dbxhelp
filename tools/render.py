"""Markdown -> styled HTML for dbxhelp (notebook %md-sandbox cells and the HTML presentation).

Markdown conventions understood here:
  # Title                       first section: rendered as the hero
  ## Section                    one notebook cell / one slide per section
  ```python run / ```sql run    executable cells in the notebook ("runs in the notebook" in HTML)
  **The one idea to remember:** paragraph -> highlighted "idea" card
  > quote                       callout card
  - ❌ ... / - ✅ ...            red / green list items
"""
import html as _html
import re

import markdown
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name
from pygments.util import ClassNotFound

FENCE = re.compile(r"^```([^\n]*)\n(.*?)^```[ \t]*$", re.S | re.M)
_FMT = HtmlFormatter(nowrap=True, noclasses=True, style="github-dark")

ICONS = [
    ("check yourself", "🎯"), ("what we covered", "🗺️"), ("try it", "🧪"), ("cheat", "🧭"), ("mistake", "⚠️"),
    ("trap", "⚠️"), ("best practice", "🏆"), ("trainer", "🎓"), ("further reading", "📚"), ("requirement", "📋"),
    ("privacy", "🔐"), ("privilege", "🔐"), ("access", "🔐"), ("forgotten", "🧹"), ("delet", "🧹"),
    ("de-identif", "🎭"), ("stream", "🌊"), ("state", "🧠"), ("watermark", "🧠"), ("join", "🔗"),
    ("cdc", "🔁"), ("change data", "🔁"), ("slowly", "🔁"), ("bundle", "📦"), ("deploy", "🚀"), ("ci/cd", "🚀"),
    ("cli", "⌨️"), ("test", "🧪"), ("git", "🌿"), ("version control", "🌿"), ("trigger", "⏰"), ("schedul", "⏰"),
    ("parameter", "🎛️"), ("variable", "🎛️"), ("mode", "🎚️"), ("flow", "🔀"), ("fail", "🩹"), ("quality", "✅"),
    ("expectation", "✅"), ("monitor", "📈"), ("event log", "📈"), ("diagnos", "🩺"), ("metadata", "🏷️"),
    ("json", "🧩"), ("csv", "📄"), ("connector", "🔌"), ("enterprise", "🔌"), ("merge", "🔀"), ("compute", "🖥️"),
    ("execut", "⚙️"), ("spark", "⚙️"), ("file layout", "🗂️"), ("problem", "🔥"), ("ingestion", "📥"),
    ("load", "📥"), ("dashboard", "📊"), ("genie", "🧞"), ("measure", "📏"), ("metric", "📏"), ("yaml", "📝"),
    ("anatomy", "🧬"), ("resource", "🧱"), ("dataset", "🧱"), ("pipeline", "🏗️"), ("medallion", "🏗️"),
    ("python", "🐍"), ("modular", "🧱"), ("devops", "♾️"), ("dag", "🕸️"), ("job", "🕸️"), ("why", "💡"),
    ("what is", "💡"), ("new", "✨"), ("advanced", "🚀"), ("composab", "🧮"), ("trap", "🪤"),
]


def icon_for(title):
    t = title.lower()
    for key, icon in ICONS:
        if key in t:
            return icon
    return "✨"


def split_sections(md):
    sections, cur, fence = [], [], False
    for line in md.splitlines():
        if line.startswith("```"):
            fence = not fence
        if not fence and line.startswith("## ") and cur:
            sections.append("\n".join(cur).strip())
            cur = []
        cur.append(line)
    if cur:
        sections.append("\n".join(cur).strip())
    return [s for s in sections if s]


def code_html(code, lang, run=False):
    code = code.rstrip("\n")
    if not lang:
        return f'<pre class="diagram">{_html.escape(code)}</pre>'
    try:
        lexer = get_lexer_by_name({"sh": "bash", "shell": "bash"}.get(lang, lang))
        body = highlight(code, lexer, _FMT).rstrip("\n")
    except ClassNotFound:
        body = _html.escape(code)
    badge = '<span class="run">▶ runs in the notebook</span>' if run else ""
    return (f'<div class="code"><div class="code-bar"><i></i><i></i><i></i>'
            f'<span class="lang">{_html.escape(lang.upper())}</span>{badge}'
            f'<button class="copy" type="button">copy</button></div><pre>{body}</pre></div>')


def md_to_html(text, keep_run=True):
    codes = []

    def repl(m):
        info = m.group(1).strip().split()
        lang = info[0] if info else ""
        run = len(info) > 1 and info[1] == "run"
        if run and not keep_run:
            return ""
        codes.append(code_html(m.group(2), lang, run))
        return f"\n\nDBXCODE{len(codes) - 1}X\n\n"

    text = FENCE.sub(repl, text)
    out = markdown.markdown(text, extensions=["tables", "sane_lists"], output_format="html")
    out = re.sub(r"<p>DBXCODE(\d+)X</p>", lambda m: codes[int(m.group(1))], out)
    return enhance(out)


def enhance(h):
    h = h.replace("<blockquote>", '<blockquote class="callout">')
    h = re.sub(r"<li>(\s*(?:<p>)?)❌", r'<li class="bad">\1❌', h)
    h = re.sub(r"<li>(\s*(?:<p>)?)✅", r'<li class="good">\1✅', h)
    h = re.sub(r"<p><strong>(The one idea to remember:?)</strong>(.*?)</p>",
               r'<div class="idea"><span class="idea-icon">💡</span><div><b>\1</b>\2</div></div>', h, flags=re.S)
    h = h.replace("<table>", '<div class="tbl"><table>').replace("</table>", "</table></div>")
    h = re.sub(r'<a href="(http[^"]+)"', r'<a target="_blank" href="\1"', h)
    return h


def split_blocks(section_md):
    """[('md', text) | ('run', lang, code)] in order."""
    out, pos = [], 0
    for m in FENCE.finditer(section_md):
        info = m.group(1).strip().split()
        if len(info) > 1 and info[1] == "run":
            out.append(("md", section_md[pos:m.start()]))
            out.append(("run", info[0], m.group(2).rstrip("\n")))
            pos = m.end()
    out.append(("md", section_md[pos:]))
    return [b for b in out if not (b[0] == "md" and not b[1].strip())]


def section_title(section_md):
    first = section_md.splitlines()[0]
    return first[3:].strip() if first.startswith("## ") else first.lstrip("# ").strip()


def hero_html(intro_md, kicker):
    """First section: '# Title', blockquote meta, idea paragraph, extra text."""
    lines = intro_md.splitlines()
    title = lines[0].lstrip("# ").strip()
    rest = "\n".join(lines[1:])
    quote = "\n".join(l[1:].lstrip() for l in rest.splitlines() if l.startswith(">"))
    body = "\n".join(l for l in rest.splitlines() if not l.startswith(">"))
    chips, subtitle = [], []
    for part in [p for p in quote.split("\n") if p.strip()]:
        if not subtitle and ("·" in part or "⏱" in part):
            chips += [c.strip().strip("*").strip() for c in part.split("·") if c.strip().strip("*").strip()]
        else:
            subtitle.append(part)
    chip_html = "".join(f'<span class="chip">{md_inline(c)}</span>' for c in chips if c.strip() != kicker)
    sub_html = md_inline(" ".join(subtitle))
    title_html = _html.escape(title).replace("Recap: ", '<span class="kicker-inline">Recap</span>')
    return (f'<div class="hero"><div class="blob b1"></div><div class="blob b2"></div><div class="blob b3"></div>'
            f'<div class="hero-in"><div class="kicker">{_html.escape(kicker)}</div>'
            f'<h1>{title_html}</h1><div class="chips">{chip_html}</div>'
            f'<p class="sub">{sub_html}</p></div></div>' + md_to_html(body))


def md_inline(text):
    h = markdown.markdown(text, output_format="html").strip()
    return re.sub(r"^<p>(.*)</p>$", r"\1", h, flags=re.S)


def section_head(title, num):
    return (f'<div class="sec-head"><span class="sec-num">{num:02d}</span>'
            f'<span class="sec-icon">{icon_for(title)}</span><h2>{md_inline(title)}</h2></div>')


def section_body(section_md, keep_run=True):
    lines = section_md.splitlines()
    return md_to_html("\n".join(lines[1:]), keep_run=keep_run)
