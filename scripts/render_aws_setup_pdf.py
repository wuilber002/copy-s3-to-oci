#!/usr/bin/env python3
"""Render docs/aws-setup.md as a paginated A4 PDF.

Run with the local renderer dependencies installed under .tools/pdf-render:
  PYTHONPATH=.tools/pdf-render python3 scripts/render_aws_setup_pdf.py
"""

from __future__ import annotations

from pathlib import Path

import markdown
from weasyprint import CSS, HTML


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "aws-setup.md"
OUTPUT = ROOT / "docs" / "aws-setup.pdf"


STYLESHEET = """
@page {
  size: A4;
  margin: 17mm 16mm 18mm;

  @top-center {
    content: string(document-title);
    color: #4b5563;
    font: 8pt sans-serif;
  }

  @bottom-left {
    content: "AWS Setup";
    color: #4b5563;
    font: 8pt sans-serif;
  }

  @bottom-right {
    content: "Página " counter(page) " de " counter(pages);
    color: #4b5563;
    font: 8pt sans-serif;
  }
}

* { box-sizing: border-box; }

html { font-size: 10.5pt; }

body {
  color: #1f2937;
  font-family: "DejaVu Serif", serif;
  line-height: 1.38;
}

h1, h2, h3, h4 {
  color: #111827;
  font-family: "DejaVu Serif", serif;
  line-height: 1.18;
  break-after: avoid-page;
  page-break-after: avoid;
}

h1 {
  string-set: document-title content(text);
  font-size: 22pt;
  margin: 0 0 12pt;
}

h2 {
  font-size: 16pt;
  margin: 22pt 0 9pt;
}

h3 {
  font-size: 12.5pt;
  margin: 16pt 0 7pt;
}

p, li {
  orphans: 3;
  widows: 3;
}

p { margin: 0 0 8pt; }

ol, ul {
  margin: 0 0 9pt;
  padding-left: 20pt;
}

li { margin: 0 0 3pt; }

blockquote {
  border-left: 3pt solid #1d4ed8;
  background: #eff6ff;
  margin: 10pt 0;
  padding: 7pt 10pt;
  break-inside: avoid-page;
}

blockquote p:last-child { margin-bottom: 0; }

table {
  border-collapse: collapse;
  table-layout: fixed;
  width: 100%;
  margin: 9pt 0 12pt;
  font-family: "DejaVu Serif", serif;
  font-size: 8.2pt;
}

thead { display: table-header-group; }
tr { break-inside: avoid-page; }

th, td {
  border: 0.5pt solid #9ca3af;
  padding: 5pt 6pt;
  overflow-wrap: anywhere;
  vertical-align: top;
}

th {
  background: #e5e7eb;
  color: #111827;
  font-weight: 700;
}

code {
  background: #f3f4f6;
  border-radius: 2pt;
  font-family: "DejaVu Sans Mono", monospace;
  font-size: 0.85em;
  overflow-wrap: anywhere;
}

pre {
  background: #f3f4f6;
  border: 0.5pt solid #d1d5db;
  border-radius: 3pt;
  font-family: "DejaVu Sans Mono", monospace;
  font-size: 7.2pt;
  line-height: 1.25;
  margin: 9pt 0 12pt;
  padding: 7pt;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

pre code { background: transparent; }

a { color: #1d4ed8; text-decoration: none; }
strong { font-weight: 700; }
em { font-style: italic; }
"""


def main() -> None:
    source_text = SOURCE.read_text(encoding="utf-8")
    rendered = markdown.markdown(
        source_text,
        extensions=["tables", "fenced_code", "sane_lists"],
        output_format="html5",
    )
    html = """<!doctype html>
<html lang="pt-BR">
<head><meta charset="utf-8"><title>AWS Setup</title></head>
<body>""" + rendered + "</body></html>"
    HTML(string=html, base_url=str(ROOT)).write_pdf(OUTPUT, stylesheets=[CSS(string=STYLESHEET)])
    print(OUTPUT)


if __name__ == "__main__":
    main()
