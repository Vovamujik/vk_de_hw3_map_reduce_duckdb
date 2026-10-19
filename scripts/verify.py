"""Этап 4: результаты MapReduce и DuckDB совпадают полностью.

Оба набора читаются прямо из RustFS и сравниваются поэлементно: множество ключей и каждое число.

    uv run python -m scripts.verify | tee logs/05_verify.txt
"""

import csv
import hashlib
import io
import math
import sys

from hw import s3, settings

COUNT_FILES = ["skills.csv", "pairs.csv", "triples.csv"]
MR = f"{settings.RESULTS_PREFIX}/mapreduce"
DUCK = f"{settings.RESULTS_PREFIX}/duckdb"
ANALYSIS = f"{settings.RESULTS_PREFIX}/analysis"


def load_counts(key: str) -> dict[tuple, int]:
    rows = csv.DictReader(io.StringIO(s3.read_text(key)))
    return {tuple(v for k, v in row.items() if k.startswith("skill")): int(row["vacancies"]) for row in rows}


def load_salary(key: str) -> dict[tuple, tuple[int, int, float]]:
    out = {}
    for row in csv.DictReader(io.StringIO(s3.read_text(key))):
        combo = tuple(row[f"skill_{i}"] for i in range(1, int(row["k"]) + 1))
        out[combo] = (int(row["offers"]), int(row["employers"]), float(row["median"]))
    return out


def load_control(key: str) -> dict[str, int]:
    return {row["metric"]: int(row["value"]) for row in csv.DictReader(io.StringIO(s3.read_text(key)))}


def fingerprint(data: dict) -> str:
    h = hashlib.sha256()
    for key, value in sorted(data.items()):
        h.update(("\x1f".join(key) + "\t" + str(value) + "\n").encode())
    return h.hexdigest()[:16]


def compare(name: str, a: dict, b: dict, same) -> bool:
    only_a, only_b = a.keys() - b.keys(), b.keys() - a.keys()
    diff = [k for k in a.keys() & b.keys() if not same(a[k], b[k])]
    ok = not (only_a or only_b or diff)
    print(f"{name:<18} {len(a):>8,} {len(b):>8,} {len(only_a):>7} {len(only_b):>7} {len(diff):>9}   {'OK' if ok else 'РАСХОЖДЕНИЕ'}")
    for k in list(only_a)[:3] + list(only_b)[:3] + diff[:3]:
        print("    пример:", k, a.get(k), b.get(k))
    return ok


def main() -> None:
    all_ok = True
    mr_ctl, db_ctl = load_control(f"{MR}/control.csv"), load_control(f"{DUCK}/control.csv")
    print(f"{'контрольное число':<18} {'MapReduce':>10} {'DuckDB':>10}")
    for metric in sorted(mr_ctl.keys() | db_ctl.keys()):
        same = mr_ctl.get(metric) == db_ctl.get(metric)
        all_ok &= same
        print(f"{metric:<18} {mr_ctl.get(metric, 0):>10,} {db_ctl.get(metric, 0):>10,}   {'OK' if same else 'РАСХОЖДЕНИЕ'}")
    print()
    print(f"{'файл':<18} {'MapReduce':>8} {'DuckDB':>8} {'только MR':>7} {'только DB':>7} {'разные числа':>9}")
    for name in COUNT_FILES:
        mr, db = load_counts(f"{MR}/{name}"), load_counts(f"{DUCK}/{name}")
        all_ok &= compare(name, mr, db, lambda x, y: x == y)
        print(f"{'':<18} сумма частот {sum(mr.values()):,} = {sum(db.values()):,}; "
              f"SHA-256 {fingerprint(mr)} / {fingerprint(db)}")

    mr_sal, db_sal = load_salary(f"{MR}/combo_salary.csv"), load_salary(f"{ANALYSIS}/combo_salary.csv")
    all_ok &= compare(
        "combo_salary.csv", mr_sal, db_sal,
        lambda x, y: x[:2] == y[:2] and math.isclose(x[2], y[2], rel_tol=1e-12, abs_tol=1e-6),
    )
    print(f"{'':<18} (предложений, работодателей, медиана) для всех Go-вакансий: MapReduce {mr_sal[()]}, DuckDB {db_sal[()]}")
    print("\nИТОГ:", "результаты полностью совпадают" if all_ok else "ЕСТЬ РАСХОЖДЕНИЯ")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
