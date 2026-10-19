# ДЗ 3: MapReduce и DuckDB поверх RustFS (вариант Go)

Частоты навыков, пар и троек в Go-вакансиях hh.ru — своим MapReduce и SQL в DuckDB над объектом в RustFS, сверка, зарплаты, регионы, профилирование.

**Отчёт:** [`report/hw3_mapreduce_duckdb_report.pdf`](report/hw3_mapreduce_duckdb_report.pdf)

## Где что

- `hw/` — код: свой MapReduce (`mapreduce.py`, `jobs.py`), правила варианта (`normalize.py`), DuckDB (`duck.py`)
- `sql/` — SQL для DuckDB
- `scripts/` — запуск этапов, сверка и замеры (всё по порядку — `bash scripts/run_all.sh`)
- `results/`, `logs/` — результаты и выводы команд, вошедшие в отчёт
- `tests/` — тесты движка MapReduce

