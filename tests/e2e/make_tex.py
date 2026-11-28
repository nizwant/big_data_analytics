"""Turns deliverables/test_logs/results.json into LaTeX fragments for deliverables/Milestone_2_Tests.tex.

    python tests/e2e/make_tex.py

Writes deliverables/test_generated/{summary,acquisition,preprocessing,quality,logs}.tex. The "actual result"
column and the printouts come from the run, so the document cannot drift from what was executed.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LOGS = REPO / "deliverables" / "test_logs"
OUT = REPO / "deliverables" / "test_generated"

GROUPS = [("Acquisition", "acquisition", "Functional tests: data acquisition"),
          ("Pre-processing", "preprocessing", "Functional tests: pre-processing"),
          ("Quality", "quality", "Quality tests: data transformation")]


def esc(text: str) -> str:
    """Escapes LaTeX specials; underscores and slashes may break lines."""
    out = []
    for ch in text:
        if ch in "&%$#{}":
            out.append("\\" + ch)
        elif ch == "_":
            out.append("\\_\\allowbreak{}")
        elif ch == "\\":
            out.append("\\textbackslash{}")
        elif ch == "~":
            out.append("\\textasciitilde{}")
        elif ch == "^":
            out.append("\\textasciicircum{}")
        elif ch == "/":
            out.append("/\\allowbreak{}")
        else:
            out.append(ch)
    return "".join(out)


def status_cell(status: str) -> str:
    color = {"PASS": "green!45!black", "FAIL": "red!70!black"}.get(status, "black")
    return f"\\textbf{{\\textcolor{{{color}}}{{{status}}}}}"


def steps_cell(steps: list[str]) -> str:
    return "\\newline ".join(f"{i}.~{esc(s)}" for i, s in enumerate(steps, 1))


def table(cases: list[dict]) -> str:
    rows = []
    for c in cases:
        rows.append(" & ".join([
            f"\\textbf{{{c['id']}}}",
            f"\\textbf{{{esc(c['title'])}}}\\newline {esc(c['objective'])}",
            steps_cell(c["steps"]),
            esc(c["expected"]),
            esc(c["actual"]) + f"\\newline \\textit{{Log: Appendix, {c['id']}.}}",
            status_cell(c["status"]),
        ]) + " \\\\\n\\midrule[0.3pt]\n")
    head = ("\\textbf{ID} & \\textbf{Test objective} & \\textbf{Test steps} & \\textbf{Expected result} & "
            "\\textbf{Actual result} & \\textbf{Status} \\\\\n")
    return ("{\\footnotesize\n\\begin{longtable}{@{}L{1.1cm}L{5.8cm}L{5.4cm}L{3.9cm}L{5.5cm}L{1.3cm}@{}}\n"
            f"\\toprule\n{head}\\midrule\n\\endfirsthead\n\\toprule\n{head}\\midrule\n\\endhead\n\\bottomrule\n\\endfoot\n"
            + "".join(rows) + "\\end{longtable}}\n")


def main() -> None:
    results = json.loads((LOGS / "results.json").read_text())
    OUT.mkdir(exist_ok=True)
    for group, name, _ in GROUPS:
        cases = [c for c in results if c["group"] == group]
        (OUT / f"{name}.tex").write_text(table(cases))
    counts = Counter(c["status"] for c in results)
    rows = []
    for group, _, title in GROUPS:
        cs = [c for c in results if c["group"] == group]
        n_pass = sum(c["status"] == "PASS" for c in cs)
        rows.append(f"{esc(title)} & {len(cs)} & {n_pass} & {len(cs) - n_pass} \\\\\n")
    summary = ("\\begin{tabular}{@{}lrrr@{}}\n\\toprule\n\\textbf{Group} & \\textbf{Cases} & \\textbf{Passed} & \\textbf{Failed} \\\\\n\\midrule\n"
               + "".join(rows) + f"\\midrule\n\\textbf{{Total}} & {len(results)} & {counts.get('PASS', 0)} & {counts.get('FAIL', 0)} \\\\\n"
               "\\bottomrule\n\\end{tabular}\n")
    (OUT / "summary.tex").write_text(summary)
    # Long printouts start with a blank line before the header (idempotent for logs written before this rule)
    for f in [*(LOGS / f"{c['id']}.log" for c in results), LOGS / "DATA-EXAMPLES.log"]:
        if f.exists() and not f.read_text().startswith("\n"):
            f.write_text("\n" + f.read_text())
    logs = []
    for c in results:
        logs.append(f"\\subsection*{{{c['id']} -- {esc(c['title'])}}}\n"
                    f"\\lstinputlisting[basicstyle=\\ttfamily\\tiny,breaklines=true,breakatwhitespace=false]{{test_logs/{c['id']}.log}}\n")
    (OUT / "logs.tex").write_text("\n".join(logs))
    print(f"{len(results)} cases: {dict(counts)}")


if __name__ == "__main__":
    main()
