"""Бонус: замеры «до» и «после» по времени и памяти.

Каждая конфигурация запускается отдельным процессом (чтобы пик памяти не смешивался),
REPS раз подряд; в сводку идёт медиана времени и максимум пика памяти. Отпечаток результата
(digest) у всех конфигураций одного метода обязан совпасть — оптимизация не меняет ответ.

    uv run python -m scripts.benchmark --suite all | tee logs/08_benchmark.txt
"""

import argparse
import json
import statistics
import subprocess
import sys

from hw import settings

BENCH = settings.RESULTS_DIR / "bench"
MR = [sys.executable, "-m", "scripts.run_mapreduce", "--job", "skills", "--no-save"]
DB = [sys.executable, "-m", "scripts.run_duckdb", "--no-save"]

SUITES = {
    "mapreduce": [
        ("mr_baseline", "MR «до»: 1 процесс, разбор всех строк, без combiner, shuffle сортировкой",
         MR + ["--variant", "baseline"]),
        ("mr_w1_plain", "MR: 1 процесс, построчное декодирование, hash-shuffle в файлы; без предфильтра и combiner",
         MR + ["--workers", "1", "--no-prefilter", "--no-combiner"]),
        ("mr_w1_comb", "+ combiner на стороне map", MR + ["--workers", "1", "--no-prefilter"]),
        ("mr_w1_regex", "+ предфильтр строк, первая версия (регулярка с lookbehind)",
         MR + ["--workers", "1", "--prefilter-impl", "regex"]),
        ("mr_w1_pref", "+ предфильтр строк, итоговая версия (поиск по литералу)", MR + ["--workers", "1"]),
        ("mr_w8_regex", "8 процессов, первая версия предфильтра", MR + ["--workers", "8", "--prefilter-impl", "regex"]),
        ("mr_w2", "+ 2 процесса", MR + ["--workers", "2"]),
        ("mr_w4", "+ 4 процесса", MR + ["--workers", "4"]),
        ("mr_w8", "+ 8 процессов (как в запуске этапа 3)", MR + ["--workers", "8"]),
        ("mr_w12", "+ 12 процессов", MR + ["--workers", "12"]),
        ("mr_w8_s8", "8 процессов, сплит 8 МиБ", MR + ["--workers", "8", "--split-mb", "8"]),
        ("mr_w8_s64", "8 процессов, сплит 64 МиБ", MR + ["--workers", "8", "--split-mb", "64"]),
        ("mr_w8_s128", "8 процессов, сплит 128 МиБ", MR + ["--workers", "8", "--split-mb", "128"]),
        ("mr_segment", "MR по выделенному сегменту Go, 1 процесс", MR + ["--source", "segment", "--workers", "1"]),
    ],
    "duckdb": [
        ("db_naive", "DuckDB «до»: 3 запроса, каждый читает CSV с автоопределением", DB + ["--variant", "naive"]),
        ("db_csv_t1", "DuckDB: одно чтение CSV в таблицу, 1 поток", DB + ["--threads", "1"]),
        ("db_csv", "DuckDB: одно чтение CSV в таблицу, все потоки", DB),
        ("db_parquet", "DuckDB: Parquet, 13 столбцов, zstd", DB + ["--source", "parquet"]),
        ("db_segment", "DuckDB: выделенный сегмент Go (CSV)", DB + ["--source", "segment"]),
    ],
}
WARMUP = [MR + ["--workers", "8"], DB]  # прогрев кеша страниц RustFS и сети, в замеры не идёт


def run_config(cid: str, desc: str, cmd: list[str], reps: int, raw_path) -> dict:
    tmp = BENCH / f"{cid}.tmp.jsonl"
    tmp.unlink(missing_ok=True)
    for _ in range(reps):
        proc = subprocess.run([*cmd, "--metrics-json", str(tmp)], capture_output=True, text=True, cwd=settings.ROOT)
        if proc.returncode:
            sys.exit(f"{cid} упал:\n{proc.stderr}")
    rows = [json.loads(line) for line in tmp.read_text(encoding="utf-8").splitlines()]
    tmp.unlink()
    with open(raw_path, "a", encoding="utf-8") as f:
        for rep, row in enumerate(rows):
            f.write(json.dumps({"config": cid, "rep": rep, **row}, ensure_ascii=False) + "\n")
    walls = [r["wall_s"] for r in rows]
    summary = {
        "config": cid, "desc": desc, "method": rows[0]["method"], "reps": len(rows),
        "wall_median_s": round(statistics.median(walls), 2), "wall_min_s": min(walls), "wall_max_s": max(walls),
        "wall_median_raw_s": statistics.median(walls),
        "cpu_median_s": round(statistics.median(r["cpu_s"] for r in rows), 2),
        "peak_rss_tree_mb": max(r["peak_rss_tree_mb"] for r in rows),
        "maxrss_main_mb": max(r["maxrss_main_mb"] for r in rows),
        "maxrss_child_mb": max(r["maxrss_child_mb"] for r in rows),
        "bytes_read": rows[0].get("bytes"),
        "digests": sorted({r.get("digest") for r in rows}),
    }
    print(f"{cid:<12} {summary['wall_median_s']:>7.2f} с  [{summary['wall_min_s']:.2f}–{summary['wall_max_s']:.2f}]  "
          f"CPU {summary['cpu_median_s']:>6.2f} с  пик RSS {summary['peak_rss_tree_mb']:>7.1f} МБ  "
          f"digest {','.join(map(str, summary['digests']))}  — {desc}", flush=True)
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--suite", choices=[*SUITES, "all"], default="all")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--only", nargs="*", help="только эти конфигурации")
    ap.add_argument("--tag", default="main", help="имя набора результатов в results/bench/")
    args = ap.parse_args()

    BENCH.mkdir(parents=True, exist_ok=True)
    raw_path = BENCH / f"{args.tag}.jsonl"
    raw_path.unlink(missing_ok=True)
    print("Прогрев...", flush=True)
    for cmd in WARMUP:
        subprocess.run(cmd, capture_output=True, cwd=settings.ROOT, check=True)
    print(f"Повторов на конфигурацию: {args.reps}; время — медиана [мин–макс], память — максимум пика RSS")
    summaries = []
    for suite in SUITES if args.suite == "all" else [args.suite]:
        print(f"\n== {suite}")
        for cid, desc, cmd in SUITES[suite]:
            if not args.only or cid in args.only:
                summaries.append(run_config(cid, desc, cmd, args.reps, raw_path))
    for method in {s["method"] for s in summaries}:
        digests = {d for s in summaries if s["method"] == method for d in s["digests"]}
        print(f"{method}: {'результат одинаковый во всех конфигурациях' if len(digests) == 1 else 'РАЗНЫЕ РЕЗУЛЬТАТЫ'}"
              f" ({', '.join(map(str, digests))})")
    (BENCH / f"{args.tag}_summary.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
