"""Профиль одного map-таска по функциям (cProfile) — где тратится время внутри MapReduce.

    uv run python -m scripts.profile_map_task | tee logs/08b_cprofile_after.txt
    uv run python -m scripts.profile_map_task --prefilter-impl regex | tee logs/08b_cprofile_before.txt
"""

import argparse
import cProfile
import dataclasses
import pstats
import sys

from hw import mapreduce as mr
from hw import normalize as N
from hw import s3, settings
from hw.jobs import SKILL_COUNTS

PREFILTERS = {"fast": N.go_candidate_lines, "regex": N.go_candidate_lines_regex}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--prefilter-impl", choices=list(PREFILTERS), default="fast")
    args = ap.parse_args()
    job = dataclasses.replace(SKILL_COUNTS, prefilter=PREFILTERS[args.prefilter_impl])
    size = s3.object_size(settings.RAW_KEY)
    cols = mr.read_header(settings.RAW_KEY)
    split = mr.make_splits(size, 32 * 2**20)[5]
    settings.WORK_DIR.mkdir(exist_ok=True)
    spill = settings.WORK_DIR / "profile"
    spill.mkdir(exist_ok=True)
    task = mr.MapTask(job, settings.RAW_KEY, split, size, cols, 4, str(spill), True, True)
    mr.run_map_task(task)  # прогрев соединения
    prof = cProfile.Profile()
    prof.enable()
    stats = mr.run_map_task(task)
    prof.disable()
    print(f"предфильтр: {args.prefilter_impl}; сплит {split.index}: байты [{split.start:,}, {split.end:,})")
    print(f"статистика таска: {stats}")
    pstats.Stats(prof, stream=sys.stdout).strip_dirs().sort_stats("tottime").print_stats(12)


if __name__ == "__main__":
    main()
