"""Этап 3, способ 1: своя реализация MapReduce читает CSV из RustFS чанками.

    uv run python -m scripts.run_mapreduce | tee logs/03_mapreduce.txt
    uv run python -m scripts.run_mapreduce --job skills --variant baseline --no-save   # бонус, «до»
"""

import argparse
import csv
import dataclasses
import hashlib
import json
from pathlib import Path

from hw import mapreduce as mr
from hw import normalize as N
from hw import s3, settings
from hw.jobs import JOBS
from hw.profiling import Profile

OUT = settings.RESULTS_DIR / "mapreduce"
COUNT_FILES = {
    1: ("skills.csv", ["skill"]),
    2: ("pairs.csv", ["skill_1", "skill_2"]),
    3: ("triples.csv", ["skill_1", "skill_2", "skill_3"]),
}
SOURCES = {"raw": settings.RAW_KEY, "segment": settings.SEGMENT_CSV_KEY}
PREFILTERS = {"fast": N.go_candidate_lines, "regex": N.go_candidate_lines_regex}


def write_counts(results: dict, out: Path) -> None:
    for k, (name, cols) in COUNT_FILES.items():
        rows = sorted(((key, n) for key, n in results.items() if len(key) == k), key=lambda kv: (-kv[1], kv[0]))
        with open(out / name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([*cols, "vacancies"])
            w.writerows([*key, n] for key, n in rows)


def write_salary(results: dict, out: Path) -> None:
    rows = sorted(results.items(), key=lambda kv: (len(kv[0]), kv[0]))
    with open(out / "combo_salary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["k", "skill_1", "skill_2", "skill_3", "offers", "employers", "median"])
        for key, (offers, employers, median) in rows:
            w.writerow([len(key), *key, *[""] * (3 - len(key)), offers, employers, repr(median)])


def digest(results: dict) -> str:
    """Отпечаток результата: одинаковый у всех конфигураций, если они посчитали одно и то же."""
    h = hashlib.sha256()
    for key, value in sorted(results.items()):
        h.update(f"{key!r}\t{value!r}\n".encode())
    return h.hexdigest()[:16]


def summarize(job_name: str, res: mr.RunResult, metrics: dict) -> dict:
    st = res.map_stats
    fields = ("records", "parsed", "malformed", "emitted", "keys", "bytes", "requests")
    total = {f: sum(s.get(f, 0) for s in st) for f in fields}
    by_k: dict[int, int] = {}
    for key in res.results:
        by_k[len(key)] = by_k.get(len(key), 0) + 1
    print(f"== MapReduce, задача {job_name}")
    print(f"сплитов {res.splits}, GET-запросов {total['requests']}, прочитано {total['bytes']:,} байт")
    print(f"записей {total['records']:,}, разобрано как CSV {total['parsed'] or total['records']:,}, битых строк {total['malformed']}")
    if total["emitted"]:
        print(f"map выдал {total['emitted']:,} пар (key, value), в shuffle ушло {total['keys']:,} ключей")
    print("уникальных ключей по размеру комбинации:", ", ".join(f"k={k}: {n:,}" for k, n in sorted(by_k.items())))
    print("фазы, с:", ", ".join(f"{k} {v:.2f}" if isinstance(v, float) else f"{k} {v:,}" for k, v in res.timings.items()))
    print("профиль:", ", ".join(f"{k} {v}" for k, v in metrics.items()))
    return {**total, "distinct": by_k}


def print_top(results: dict, n: int = 10) -> None:
    for k, (name, _) in COUNT_FILES.items():
        top = sorted(((v, key) for key, v in results.items() if len(key) == k), key=lambda t: (-t[0], t[1]))[:n]
        print(f"-- топ-{n}, {name}")
        for v, key in top:
            print(f"{v:>6}  {' + '.join(key)}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--job", choices=["skills", "salary", "all"], default="all")
    ap.add_argument("--variant", choices=["optimized", "baseline"], default="optimized")
    ap.add_argument("--source", choices=list(SOURCES), default="raw")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--split-mb", type=int, default=32)
    ap.add_argument("--no-combiner", action="store_true")
    ap.add_argument("--no-prefilter", action="store_true")
    ap.add_argument("--prefilter-impl", choices=list(PREFILTERS), default="fast",
                    help="regex — первая, медленная версия предфильтра (для замера «до»)")
    ap.add_argument("--no-save", action="store_true", help="не писать результаты (для замеров)")
    ap.add_argument("--metrics-json", help="дописать строку JSON с метриками запуска в этот файл")
    args = ap.parse_args()

    key, split_size = SOURCES[args.source], args.split_mb * 2**20
    print(f"Источник: s3://{settings.BUCKET}/{key}, вариант {args.variant}")
    OUT.mkdir(parents=True, exist_ok=True)
    for job_name in ["skills", "salary"] if args.job == "all" else [args.job]:
        job = dataclasses.replace(JOBS[job_name], prefilter=PREFILTERS[args.prefilter_impl])
        with Profile() as prof:
            if args.variant == "baseline":
                res = mr.run_baseline(job, key, split_size=split_size)
            else:
                res = mr.run(
                    job, key, split_size=split_size, workers=args.workers, reducers=args.reducers,
                    use_combiner=not args.no_combiner, use_prefilter=not args.no_prefilter,
                )
        stats = summarize(job.name, res, prof.metrics)
        if job_name == "skills":
            print_top(res.results)
        else:
            offers, employers, median = res.results[()]
            print(f"Go-вакансии с зарплатой: {offers} уникальных предложений от {employers} работодателей, "
                  f"медиана {median:,.0f} руб. на руки")
        if not args.no_save:
            (write_counts if job_name == "skills" else write_salary)(res.results, OUT)
        if args.metrics_json:
            config = {k: v for k, v in vars(args).items() if k not in ("metrics_json", "no_save")}
            row = {"method": "mapreduce", **config, "job": job_name, **prof.metrics, **res.timings,
                   **{k: v for k, v in stats.items() if k != "distinct"}, "digest": digest(res.results)}
            with open(args.metrics_json, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print()
    if not args.no_save:
        keys = s3.upload_dir(OUT, f"{settings.RESULTS_PREFIX}/mapreduce")
        print("Результаты в RustFS:", ", ".join(f"s3://{settings.BUCKET}/{k}" for k in keys))


if __name__ == "__main__":
    main()
