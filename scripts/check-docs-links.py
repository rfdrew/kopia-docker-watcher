#!/usr/bin/env python3
"""Checks the links of the documentation in both languages.

For README.md, README.uk.md and every .md file under docs/:

- every relative link points at a file or directory that exists, and an
  anchor (#...) at a heading or an <a id="..."> of the target file;
- a Ukrainian file and its English source have the same headings, level by
  level, and the same anchors, so a link means the same in both languages;
- a Ukrainian file links to the Ukrainian version of a page, except in the
  language switcher on its first lines.

Anchors of headings are made the way GitHub makes them. Code blocks and code
spans are skipped. Exits 1 when it finds a problem.
"""

import os
import re
import subprocess
import sys
import unicodedata

FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
HEADING = re.compile(r"^ {0,3}(#{1,6})[ \t]+(.*?)[ \t]*#*[ \t]*$")
CODE_SPAN = re.compile(r"(`+)(.+?)\1")
WRAPPED_CODE_SPAN = re.compile(r"(`+)((?:[^\n]|\n(?!\s*\n))+?)\1")
LINK = re.compile(r"!?\[((?:[^\[\]]|\[[^\[\]]*\])*)\]\(\s*<?([^)\s>]*)>?(?:\s+\"[^\"]*\")?\s*\)")
HTML_ID = re.compile(r"<a\s+(?:id|name)=\"([^\"]+)\"", re.IGNORECASE)
TAG = re.compile(r"<[^>]+>")
SWITCHER_LINES = 5

problems = []


def report(path, line, msg):
    problems.append((path, line, msg))
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::error file={path},line={line}::{msg}")
    else:
        print(f"error: {path}:{line}: {msg}")


def lines_outside_code(text):
    """Yields (line number, line) for the lines outside fenced code blocks."""
    fence = None
    for number, line in enumerate(text.splitlines(), 1):
        m = FENCE.match(line)
        if fence is None:
            if m:
                fence = m.group(1)
                continue
            yield number, line
        elif m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence) and not line.strip()[len(m.group(1)):].strip():
            fence = None


def heading_text(raw):
    """Returns the text GitHub renders for the heading source raw."""
    text = TAG.sub("", raw)
    text = CODE_SPAN.sub(lambda m: m.group(2).strip(), text)
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    return text


def slug(text):
    """Makes an anchor the way GitHub (github-slugger) does."""
    out = []
    for ch in text.lower():
        cat = unicodedata.category(ch)
        if cat[0] in "LMN" or cat == "Pc" or ch in " -":
            out.append(ch)
    return "".join(out).replace(" ", "-")


class Page:
    def __init__(self, path):
        self.path = path
        with open(path, encoding="utf-8") as f:
            self.text = f.read()
        self.headings = []  # (line, level, anchor)
        self.anchors = set()
        self.links = []  # (line, text, target)
        seen = {}
        prose = [""] * (self.text.count("\n") + 1)
        for number, line in lines_outside_code(self.text):
            prose[number - 1] = line
            for anchor in HTML_ID.findall(line):
                self.anchors.add(anchor)
            m = HEADING.match(line)
            if m:
                base = slug(heading_text(m.group(2)))
                anchor = base if base not in seen else f"{base}-{seen[base]}"
                seen[base] = seen.get(base, 0) + 1
                self.headings.append((number, len(m.group(1)), anchor))
                self.anchors.add(anchor)
        # Links and code spans may wrap onto the next line of a paragraph.
        text = WRAPPED_CODE_SPAN.sub(lambda s: re.sub(r"[^\n]", " ", s.group(0)), "\n".join(prose))
        for m in LINK.finditer(text):
            self.links.append((text.count("\n", 0, m.start()) + 1, m.group(1), m.group(2)))


def documentation_files():
    files = [f for f in ("README.md", "README.uk.md") if os.path.isfile(f)]
    for root, _, names in os.walk("docs"):
        files += [os.path.join(root, n) for n in names if n.endswith(".md")]
    return sorted(files)


def translation_of(path):
    """Returns the Ukrainian counterpart of an English file, or None."""
    if path == "README.md":
        return "README.uk.md"
    if path.startswith("docs/en/"):
        return "docs/uk/" + path[len("docs/en/"):]
    return None


def is_ukrainian(path):
    return path == "README.uk.md" or path.startswith("docs/uk/")


def check_links(pages):
    for page in pages.values():
        base = os.path.dirname(page.path)
        for line, text, target in page.links:
            if not target or re.match(r"^[a-z][a-z0-9+.-]*:", target, re.IGNORECASE):
                continue
            file_part, _, anchor = target.partition("#")
            dest = os.path.normpath(os.path.join(base, file_part)) if file_part else page.path
            if not os.path.exists(dest):
                report(page.path, line, f"link to a missing file: {target}")
                continue
            if anchor and dest.endswith(".md"):
                target_page = pages.get(dest) or Page(dest)
                if anchor not in target_page.anchors:
                    report(page.path, line, f"link to a missing anchor: {target}")
            if is_ukrainian(page.path) and line > SWITCHER_LINES and translation_of(dest):
                uk_target = os.path.relpath(translation_of(dest), base or ".")
                report(page.path, line, f"link to the English page {target}; link to {uk_target} instead")


def check_structure(pages):
    for path, page in pages.items():
        uk_path = translation_of(path)
        if uk_path not in pages:
            continue
        uk = pages[uk_path]
        en_levels = [(level, anchor) for _, level, anchor in page.headings]
        if len(page.headings) != len(uk.headings):
            report(uk_path, 1, f"{len(uk.headings)} headings, {path} has {len(page.headings)}")
        for (en_line, en_level, en_anchor), (uk_line, uk_level, _) in zip(page.headings, uk.headings):
            if en_level != uk_level:
                report(uk_path, uk_line, f"heading level {uk_level}, {path}:{en_line} has {en_level}")
                break
        missing = sorted({a for _, a in en_levels} - uk.anchors)
        for anchor in missing:
            report(uk_path, 1, f"no anchor #{anchor} of {path}; add <a id=\"{anchor}\"></a> to its heading")


def main():
    root = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True)
    os.chdir(root.stdout.strip())
    pages = {path: Page(path) for path in documentation_files()}
    check_links(pages)
    check_structure(pages)
    links = sum(len(p.links) for p in pages.values())
    print(f"docs links: {len(pages)} files, {links} links, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
