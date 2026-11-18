"""SQL templating and execution helpers.

Notebook SQL uses {{token}} placeholders so the same SQL runs against any study
version. Tokens are resolved by Study.tokens() (versioned sandbox tables, source
tables, snapshot date).
"""
from __future__ import annotations
import re

_TOKEN = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


def render(sql: str, tokens: dict) -> str:
    """Replace {{token}} placeholders. Unknown tokens raise KeyError (fail loudly)."""
    def _sub(m):
        key = m.group(1)
        if key not in tokens:
            raise KeyError(f"Unknown SQL token {{{{{key}}}}}. Known: {sorted(tokens)}")
        return str(tokens[key])
    return _TOKEN.sub(_sub, sql)


def split_statements(sql: str) -> list[str]:
    """Split on ';' that are outside quotes, backticks and comments.

    Comment-only fragments are dropped. Comments inside a statement are kept.
    """
    stmts, cur = [], []
    i, n = 0, len(sql)
    quote = None          # "'" or '"' or '`'
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if quote:
            cur.append(ch)
            if ch == quote:
                if quote == "'" and nxt == "'":      # escaped quote ''
                    cur.append(nxt); i += 1
                else:
                    quote = None
        elif ch in ("'", '"', "`"):
            quote = ch; cur.append(ch)
        elif ch == "-" and nxt == "-":               # line comment
            j = sql.find("\n", i)
            j = n if j == -1 else j
            cur.append(sql[i:j]); i = j; continue
        elif ch == "/" and nxt == "*":               # block comment
            j = sql.find("*/", i + 2)
            j = n if j == -1 else j + 2
            cur.append(sql[i:j]); i = j; continue
        elif ch == ";":
            stmts.append("".join(cur)); cur = []
        else:
            cur.append(ch)
        i += 1
    stmts.append("".join(cur))
    return [s.strip() for s in stmts if _has_code(s)]


def _has_code(stmt: str) -> bool:
    no_block = re.sub(r"/\*.*?\*/", "", stmt, flags=re.S)
    no_line = re.sub(r"--[^\n]*", "", no_block)
    return bool(no_line.strip())


def is_query(stmt: str) -> bool:
    body = re.sub(r"/\*.*?\*/", "", stmt, flags=re.S)
    body = re.sub(r"--[^\n]*", "", body).strip().lower()
    return body.startswith(("select", "with", "show", "describe", "values"))


def run(spark, sql: str, tokens: dict, show: bool = False, display=None, quiet: bool = False):
    """Render + execute every statement. Returns the DataFrame of the last statement.

    show=True displays the last statement when it is a query (mirrors a %sql cell).
    """
    rendered = render(sql, tokens)
    stmts = split_statements(rendered)
    last = None
    for k, s in enumerate(stmts):
        last = spark.sql(s)
        if not quiet and not is_query(s):
            head = " ".join(s.split())[:90]
            print(f"  ok [{k + 1}/{len(stmts)}] {head}")
    if show and last is not None and display is not None and is_query(stmts[-1]):
        display(last)
    return last
