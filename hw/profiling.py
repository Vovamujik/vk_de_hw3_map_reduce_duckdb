"""Замер блока кода: время, процессорное время и пик памяти (RSS) всего дерева процессов."""

import resource
import sys
import threading
import time

import psutil

MB = 2**20
# ru_maxrss на macOS — в байтах, на Linux — в КиБ
_MAXRSS_UNIT = 1 if sys.platform == "darwin" else 1024


class Profile:
    """with Profile() as p: ...; затем p.metrics.

    Фоновый поток раз в interval секунд суммирует RSS процесса и всех дочерних
    (воркеров пула), а в конце берётся процессорное время себя и завершившихся детей.
    """

    def __init__(self, interval: float = 0.02):
        self.interval = interval
        self.metrics: dict = {}

    def __enter__(self):
        self._proc = psutil.Process()
        self._stop = threading.Event()
        self._peak_tree = self._peak_self = 0
        self._peak_procs = 1
        self._self0 = resource.getrusage(resource.RUSAGE_SELF)
        self._child0 = resource.getrusage(resource.RUSAGE_CHILDREN)
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._t0 = time.perf_counter()
        self._thread.start()
        return self

    def _sample(self) -> None:
        while True:
            try:
                rss_self = self._proc.memory_info().rss
                children = self._proc.children(recursive=True)
            except psutil.Error:
                rss_self, children = 0, []
            rss_children = 0
            for child in children:
                try:
                    rss_children += child.memory_info().rss
                except psutil.Error:
                    pass  # воркер успел завершиться
            self._peak_self = max(self._peak_self, rss_self)
            self._peak_tree = max(self._peak_tree, rss_self + rss_children)
            self._peak_procs = max(self._peak_procs, 1 + len(children))
            if self._stop.wait(self.interval):
                break

    def __exit__(self, *exc) -> None:
        wall = time.perf_counter() - self._t0
        self._stop.set()
        self._thread.join()
        self1 = resource.getrusage(resource.RUSAGE_SELF)
        child1 = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_self = (self1.ru_utime - self._self0.ru_utime) + (self1.ru_stime - self._self0.ru_stime)
        cpu_children = (child1.ru_utime - self._child0.ru_utime) + (child1.ru_stime - self._child0.ru_stime)
        self.metrics = {
            "wall_s": round(wall, 3),
            "cpu_s": round(cpu_self + cpu_children, 3),
            "cpu_util": round((cpu_self + cpu_children) / wall, 2) if wall else 0.0,
            "peak_rss_tree_mb": round(self._peak_tree / MB, 1),
            "peak_rss_main_mb": round(self._peak_self / MB, 1),
            "maxrss_main_mb": round(self1.ru_maxrss * _MAXRSS_UNIT / MB, 1),
            "maxrss_child_mb": round(child1.ru_maxrss * _MAXRSS_UNIT / MB, 1),
            "peak_procs": self._peak_procs,
        }
