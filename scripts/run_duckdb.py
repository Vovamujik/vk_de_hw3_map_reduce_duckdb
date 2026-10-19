"""Этап 3, способ 2: те же частоты SQL-запросами DuckDB к тому же объекту в RustFS.

    uv run python -m scripts.run_duckdb | tee logs/04_duckdb.txt
    uv run python -m scripts.run_duckdb --variant naive --no-save      # бонус, «до»
    uv run python -m scripts.run_duckdb --source parquet --no-save     # бонус, колоночный формат
"""

import argparse
import hashlib
import json
import shutil
import time
import uuid

from hw import duck, s3, settings
from hw.profiling import Profile

OUT = settings.RESULTS_DIR / "duckdb"
FILES = ["skills.csv", "pairs.csv", "triples.csv"]


def source_expr(source: str, naive: bool) -> str:
    if source == "parquet":
        return duck.parquet_source()
    key = settings.RAW_KEY if source == "raw" else settings.SEGMENT_CSV_KEY
    return duck.csv_auto_source(key) if naive else duck.csv_source(key)


def explain(con, source: str) -> None:
    """Профиль DuckDB по операторам: сканирование объекта в RustFS и самый тяжёлый запрос (тройки)."""
    scan = (
        f"SELECT count(*) FROM {source} "
        "WHERE regexp_matches(name, '(^|[^A-Za-z0-9_])(?i:go|golang)([^A-Za-z0-9_]|$)')"
    )
    triples = (
        "SELECT a.skill, b.skill, c.skill, count(*) FROM go_skills a "
        "JOIN go_skills b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill "
        "JOIN go_skills c ON c.vacancy_id = a.vacancy_id AND b.skill < c.skill GROUP BY ALL"
    )
    for title, sql in [("сканирование объекта и фильтр Go", scan), ("тройки навыков", triples)]:
        print(f"-- EXPLAIN ANALYZE: {title}")
        print(con.sql(f"EXPLAIN ANALYZE {sql}").fetchall()[0][1])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--variant", choices=["materialized", "naive"], default="materialized")
    ap.add_argument("--source", choices=["raw", "parquet", "segment"], default="raw")
    ap.add_argument("--threads", type=int)
    ap.add_argument("--no-save", action="store_true", help="не сохранять результаты (для замеров)")
    ap.add_argument("--explain", action="store_true", help="напечатать EXPLAIN ANALYZE")
    ap.add_argument("--metrics-json", help="дописать строку JSON с метриками запуска в этот файл")
    args = ap.parse_args()

    naive = args.variant == "naive"
    out = settings.WORK_DIR / f"duckdb-{uuid.uuid4().hex[:8]}" if args.no_save else OUT
    out.mkdir(parents=True, exist_ok=True)
    source = source_expr(args.source, naive)
    con = duck.connect(threads=args.threads)
    threads = con.sql("SELECT current_setting('threads')").fetchone()[0]
    print(f"DuckDB {duck.duckdb.__version__}, потоков {threads}, вариант {args.variant}")
    print(f"Источник: {source}")

    timings = {}
    with Profile() as prof:
        t0 = time.perf_counter()
        if naive:
            duck.run_sql(con, "naive_counts.sql", SOURCE=source, OUT=out.as_posix())
        else:
            duck.run_sql(con, "go_segment.sql", SOURCE=source)
            timings["load_s"] = round(time.perf_counter() - t0, 3)
            duck.run_sql(con, "skill_counts.sql", OUT=out.as_posix())
        timings["total_s"] = round(time.perf_counter() - t0, 3)

    if not naive:
        n_vac, n_with_skills, n_rows = con.sql(
            "SELECT (SELECT count(*) FROM go_vacancies), count(DISTINCT vacancy_id), count(*) FROM go_skills"
        ).fetchone()
        print(f"go_vacancies: {n_vac:,} вакансий; go_skills: {n_rows:,} строк (вакансий с навыками {n_with_skills:,})")
    distinct = {}
    for name in FILES:
        path = (out / name).as_posix()
        distinct[name] = con.sql(f"SELECT count(*) FROM read_csv('{path}', header = true)").fetchone()[0]
        print(f"-- {name}: {distinct[name]:,} строк, топ-10")
        for row in con.sql(f"SELECT * FROM read_csv('{path}', header = true, all_varchar = true) LIMIT 10").fetchall():
            print(f"{row[-1]:>6}  {' + '.join(row[:-1])}")
    print("фазы, с:", ", ".join(f"{k} {v}" for k, v in timings.items()))
    print("профиль:", ", ".join(f"{k} {v}" for k, v in prof.metrics.items()))
    if args.explain and not naive:
        explain(con, source)

    if args.metrics_json:
        h = hashlib.sha256()
        for name in FILES:  # файлы отсортированы ORDER BY, поэтому побайтовый хеш устойчив
            h.update((out / name).read_bytes())
        config = {k: v for k, v in vars(args).items() if k not in ("metrics_json", "no_save")}
        row = {"method": "duckdb", "job": "skills", **config, "threads_used": threads, **prof.metrics,
               **timings, "distinct": distinct, "digest": h.hexdigest()[:16]}
        with open(args.metrics_json, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    if args.no_save:
        shutil.rmtree(out, ignore_errors=True)
    else:
        keys = s3.upload_dir(OUT, f"{settings.RESULTS_PREFIX}/duckdb")
        print("Результаты в RustFS:", ", ".join(f"s3://{settings.BUCKET}/{k}" for k in keys))


if __name__ == "__main__":
    main()
