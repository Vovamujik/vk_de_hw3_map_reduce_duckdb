"""Профиль хранилища: с какой скоростью RustFS отдаёт объект при N параллельных Range-запросах.

Только чтение, без разбора: это потолок для любого способа обработки. Параллельно
снимается загрузка CPU контейнера RustFS (docker stats).

    uv run python -m scripts.bench_storage | tee logs/08c_storage.txt
"""

import json
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from hw import s3, settings
from hw.mapreduce import make_splits

MB = 2**20
CONCURRENCY = [1, 2, 4, 8, 16]
REPS = 3


class DockerCpu:
    """Фоновый опрос `docker stats` контейнера RustFS; 100 % = одно ядро."""

    def __init__(self, container: str = "hw3-rustfs"):
        self.container, self.samples, self._stop = container, [], threading.Event()

    def __enter__(self):
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
        return self

    def _poll(self) -> None:
        while not self._stop.is_set():
            out = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", self.container],
                capture_output=True, text=True,
            ).stdout.strip().rstrip("%")
            if out:
                self.samples.append(float(out))

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()


def read_all(splits, workers: int) -> int:
    def fetch(split):
        return len(s3.get_range(settings.RAW_KEY, split.start, split.end - 1))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return sum(pool.map(fetch, splits))


def main() -> None:
    size = s3.object_size(settings.RAW_KEY)
    splits = make_splits(size, 32 * MB)
    s3.client()  # клиент создаётся до замера; boto3-клиент потокобезопасен
    read_all(splits, 8)  # прогрев
    print(f"Объект {size:,} байт, {len(splits)} диапазонов по 32 МиБ, повторов {REPS}")
    print(f"{'потоков':>8} {'время, с':>9} {'МиБ/с':>7} {'CPU RustFS, % (ср./макс.)':>26}")
    rows = []
    for workers in CONCURRENCY:
        walls, cpu = [], []
        for _ in range(REPS):
            with DockerCpu() as dc:
                t0 = time.perf_counter()
                assert read_all(splits, workers) == size
                walls.append(time.perf_counter() - t0)
            cpu += dc.samples
        wall = statistics.median(walls)
        row = {"workers": workers, "wall_s": round(wall, 2), "mib_s": round(size / MB / wall),
               "rustfs_cpu_avg": round(statistics.mean(cpu)) if cpu else None, "rustfs_cpu_max": max(cpu, default=None)}
        rows.append(row)
        print(f"{workers:>8} {row['wall_s']:>9.2f} {row['mib_s']:>7} {str(row['rustfs_cpu_avg']) + ' / ' + str(row['rustfs_cpu_max']):>26}")
    out = settings.RESULTS_DIR / "bench"
    out.mkdir(parents=True, exist_ok=True)
    (out / "storage.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
