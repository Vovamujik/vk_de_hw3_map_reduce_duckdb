#!/usr/bin/env bash
# Весь конвейер по порядку: этапы 1–6, тесты, бонус и профилирование (~15 минут).
#   bash scripts/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs
py() { uv run python -m "$@"; }

py scripts.upload | tee logs/02_upload.txt
bash scripts/stand_info.sh | tee logs/01_rustfs.txt
py scripts.ec_layout | tee logs/01b_ec_layout.txt
py scripts.run_mapreduce | tee logs/03_mapreduce.txt
# Отказ маппера: процесс-воркер со сплитом 5 умирает на первой попытке, таск перезапускается
py scripts.run_mapreduce --job skills --fail-split 5 --fail-mode crash --no-save | tee logs/03b_mapper_failure.txt
py scripts.run_duckdb --explain | tee logs/04_duckdb.txt
py scripts.analyze | tee logs/06_analysis.txt
py scripts.verify | tee logs/05_verify.txt
uv run pytest -q tests | tail -3 | tee logs/09_tests.txt
py scripts.make_curated | tee logs/07_curated.txt
py scripts.profile_map_task --prefilter-impl regex | tee logs/08b_cprofile_before.txt
py scripts.profile_map_task | tee logs/08b_cprofile_after.txt
py scripts.bench_storage | tee logs/08c_storage.txt
py scripts.trace_baseline_memory | tee logs/08d_tracemalloc.txt
py scripts.benchmark | tee logs/08_benchmark.txt
py scripts.list_bucket | tee logs/10_bucket.txt
# Рисунки и PDF собираются, только если рядом лежит исходник отчёта (в репозиторий он не входит)
if [ -f report/report.tex ]; then
    py scripts.make_figures
    py scripts.build_report
fi
