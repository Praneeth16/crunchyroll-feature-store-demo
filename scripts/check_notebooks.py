#!/usr/bin/env python3
"""Static checks on the Databricks notebook sources. Read-only, runs in a second.

Why this exists: a markdown cell whose first line is not `# MAGIC %md` is executed as
Python, and `# MAGIC ## 7 - Which models...` becomes a SyntaxError. Databricks reports it
from inside the run, so the cost is the whole job -- one such typo was found 28 minutes
into a run whose earlier cells had all passed. Every check here is for a failure that is
free to catch on a laptop and expensive to catch in a job.

    python3 scripts/check_notebooks.py           # all notebooks
    python3 scripts/check_notebooks.py <paths>
"""
import ast
import pathlib
import sys

CELL = "# COMMAND ----------"


def cells(text):
    """(start_line, [lines]) per notebook cell."""
    out, cur, start = [], [], 1
    for i, line in enumerate(text.splitlines(), start=1):
        if line.rstrip() == CELL:
            out.append((start, cur))
            cur, start = [], i + 1
        else:
            cur.append(line)
    out.append((start, cur))
    return out


def check(path):
    text = path.read_text()
    problems = []

    # 1. The file has to be valid Python once the MAGIC comments are stripped, which is
    #    what `ast.parse` sees. This catches the ordinary syntax errors.
    try:
        ast.parse(text)
    except SyntaxError as e:
        problems.append(f"{path}:{e.lineno}: SyntaxError: {e.msg}")

    for start, body in cells(text):
        stripped = [ln for ln in body if ln.strip()]
        if not stripped:
            continue
        magic = [ln for ln in stripped if ln.lstrip().startswith("# MAGIC")]
        # 2. A cell that is mostly MAGIC lines but does not OPEN with a magic directive
        #    is a markdown cell missing its `%md`, and Databricks will run it as code.
        if magic and len(magic) == len(stripped):
            first = stripped[0].strip()
            if not first.startswith("# MAGIC %"):
                problems.append(
                    f"{path}:{start}: markdown cell does not start with '# MAGIC %md' "
                    f"(first line: {first[:60]!r}) -- Databricks will execute it as Python")
        # 3. A `%pip install` has to be the whole cell: anything after it runs before the
        #    interpreter restarts and is silently lost.
        for ln in stripped[1:]:
            if "%pip" in ln and not ln.lstrip().startswith("# MAGIC"):
                problems.append(f"{path}:{start}: %pip must be its own MAGIC cell")
    return problems


def main(argv):
    paths = ([pathlib.Path(a) for a in argv]
             or sorted(pathlib.Path("notebooks").rglob("*.py")))
    bad = []
    for p in paths:
        bad += check(p)
    for b in bad:
        print(b)
    print(f"checked {len(paths)} notebooks: {len(bad)} problem(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
