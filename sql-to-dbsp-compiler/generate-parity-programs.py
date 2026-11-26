#!/usr/bin/env python3
"""Generate the fixed SQL programs for compare-with-base.sh."""
from pathlib import Path
import shutil
import random
import re
import sys

rnd = random.Random(42)
TYPES = ["INT", "BIGINT", "VARCHAR", "DECIMAL(12,2)", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN"]


def table(i, ncols):
    cols = ["  id BIGINT NOT NULL PRIMARY KEY", "  k INT NOT NULL", "  ts TIMESTAMP NOT NULL"]
    for c in range(ncols):
        cols.append(f"  c{c} {TYPES[c % len(TYPES)]}")
    return f"CREATE TABLE t{i} (\n" + ",\n".join(cols) + "\n);\n"


def wide(ntables=12, ncols=16, nviews=35):
    """Many wide tables and a mix of filter, join, aggregate and union views."""
    s = [table(i, ncols) for i in range(ntables)]
    for v in range(nviews):
        a, b = rnd.randrange(ntables), rnd.randrange(ntables)
        kind = v % 5
        if kind == 0:
            s.append(f"CREATE VIEW v{v} AS SELECT id, k, c0 + 1 AS x, UPPER(c2) AS u, c3 * 2 AS d "
                     f"FROM t{a} WHERE c0 > {v % 100} AND c7 AND c2 LIKE 'a%';\n")
        elif kind == 1:
            s.append(f"CREATE VIEW v{v} AS SELECT x.id, x.c0, y.c3, x.c2 || y.c2 AS s "
                     f"FROM t{a} x JOIN t{b} y ON x.k = y.k WHERE x.c4 > y.c4;\n")
        elif kind == 2:
            s.append(f"CREATE VIEW v{v} AS SELECT k, COUNT(*) AS n, SUM(c3) AS s, MAX(c5) AS m, "
                     f"AVG(c4) AS a, COUNT(DISTINCT c2) AS dc FROM t{a} GROUP BY k;\n")
        elif kind == 3:
            s.append(f"CREATE VIEW v{v} AS SELECT id, k FROM t{a} WHERE c0 IS NOT NULL "
                     f"UNION ALL SELECT id, k FROM t{b} WHERE c8 < 10;\n")
        else:
            s.append(f"CREATE VIEW v{v} AS SELECT id, CASE WHEN c0 < 10 THEN 'lo' WHEN c0 < 100 THEN 'mid' "
                     f"ELSE 'hi' END AS band, COALESCE(c1, 0) + CAST('12' AS BIGINT) AS z, "
                     f"TIMESTAMPADD(DAY, 1, ts) AS t1 FROM t{a} "
                     f"WHERE k IN (SELECT k FROM t{b} WHERE c7);\n")
    return "".join(s)


def chain(n=32):
    """Deep chain of views, each built on the previous one, with aggregates and joins."""
    s = [table(0, 10), table(1, 10)]
    s.append("CREATE VIEW w0 AS SELECT id, k, c0, c2, c3 FROM t0;\n")
    for v in range(1, n):
        p = f"w{v - 1}"
        if v % 4 == 0:
            s.append(f"CREATE VIEW w{v} AS SELECT p.id, p.k, p.c0 + q.c0 AS c0, p.c2, p.c3 "
                     f"FROM {p} p JOIN t1 q ON p.k = q.k;\n")
        elif v % 4 == 1:
            s.append(f"CREATE VIEW w{v} AS SELECT id, k, c0 * 2 - {v} AS c0, c2, c3 + 1 AS c3 "
                     f"FROM {p} WHERE c0 > {v};\n")
        elif v % 4 == 2:
            s.append(f"CREATE VIEW w{v} AS SELECT MIN(id) AS id, k, SUM(c0) AS c0, MAX(c2) AS c2, "
                     f"SUM(c3) AS c3 FROM {p} GROUP BY k;\n")
        else:
            s.append(f"CREATE VIEW w{v} AS SELECT id, k, c0, c2, c3 FROM {p} "
                     f"UNION ALL SELECT id, k, c0, c2, c3 FROM {p} WHERE c0 < 0;\n")
    s.append(f"CREATE MATERIALIZED VIEW final_v AS SELECT * FROM w{n - 1};\n")
    return "".join(s)


def huge_expr(ncols=48, nviews=8):
    """Very wide projections with many expressions and constants to fold."""
    s = [table(0, ncols)]
    for v in range(nviews):
        exprs = []
        for c in range(ncols):
            t = TYPES[c % len(TYPES)]
            if t in ("INT", "BIGINT", "DOUBLE", "DECIMAL(12,2)"):
                exprs.append(f"(c{c} + {c} * 3 - (2 + 5)) * CAST('{v + 1}' AS INT) AS e{c}")
            elif t == "VARCHAR":
                exprs.append(f"SUBSTRING(TRIM(c{c}) || 'x{c}', 1, 10) AS e{c}")
            elif t == "BOOLEAN":
                exprs.append(f"(c{c} AND TRUE) OR (1 = 0) AS e{c}")
            else:
                exprs.append(f"c{c} AS e{c}")
        s.append(f"CREATE VIEW h{v} AS SELECT id, " + ", ".join(exprs) + f" FROM t0 WHERE c0 > {v};\n")
    return "".join(s)


if __name__ == "__main__":
    output = Path(sys.argv[1])
    output.mkdir(parents=True, exist_ok=True)
    for name, program in [("wide", wide()), ("chain", chain()), ("huge_expr", huge_expr())]:
        (output / f"{name}.sql").write_text(program)
    source = Path(__file__).resolve().parent
    # A constant identity hash makes large optimizer maps slow. Keep every TPC-DS view.
    tpcds = (source / "SQL-compiler/src/test/resources/tpcds.sql").read_text()
    schema, *views = re.split(r"(?=^CREATE VIEW )", tpcds, flags=re.MULTILINE)
    if not views:
        sys.exit("No TPC-DS views found. Check the fixture format.")
    for start in range(0, len(views), 6):
        (output / f"tpcds-{start + 1:02}.sql").write_text(schema + "".join(views[start:start + 6]))
    for path in (source / "parity-programs").glob("*.sql"):
        shutil.copyfile(path, output / path.name)
