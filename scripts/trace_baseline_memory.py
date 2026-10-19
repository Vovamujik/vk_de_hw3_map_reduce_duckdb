"""Куда уходит память наивного MapReduce: куча Python (tracemalloc) против RSS процесса.

Повторяет цикл run_baseline() по сплитам и после каждого пятого печатает RSS и пик tracemalloc.

    uv run python -m scripts.trace_baseline_memory | tee logs/08d_tracemalloc.txt
"""

import csv
import io
import resource
import tracemalloc

import psutil

from hw import mapreduce as mr
from hw import s3, settings
from hw.jobs import SKILL_COUNTS

MB = 2**20


def main() -> None:
    proc = psutil.Process()
    size = s3.object_size(settings.RAW_KEY)
    cols = mr.read_header(settings.RAW_KEY)
    splits = mr.make_splits(size, 32 * MB)
    pairs = []
    tracemalloc.start()
    print(f"{'сплит':>5} {'RSS, МБ':>8} {'куча Python сейчас, МБ':>23} {'пик кучи, МБ':>13} {'пар (key, value)':>17}")
    for split in splits:
        task = mr.MapTask(SKILL_COUNTS, settings.RAW_KEY, split, size, cols, 1, "", False, False)
        body, _, _ = mr._owned_body(task)
        for fields in csv.reader(io.StringIO(body.decode("utf-8"))):  # как в run_baseline()
            pairs.extend(SKILL_COUNTS.mapper(fields, cols))
        if split.index % 5 == 0 or split.index == len(splits) - 1:
            cur, peak = tracemalloc.get_traced_memory()
            print(f"{split.index:>5} {proc.memory_info().rss / MB:>8.0f} {cur / MB:>23.0f} {peak / MB:>13.0f} {len(pairs):>17,}")
    print(f"ru_maxrss: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / MB:.0f} МБ")


if __name__ == "__main__":
    main()
