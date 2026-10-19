"""Бонус: раскладка данных под задачу — делается один раз, потом читается в разы меньше байт.

1. curated/vacancies.parquet — все вакансии, только 13 нужных анализу столбцов (без description), zstd.
2. segments/go/it_vacancies_go.csv — только вакансии варианта Go, все 23 столбца.

    uv run python -m scripts.make_curated | tee logs/07_curated.txt
"""

import time

from hw import duck, s3, settings

COLUMNS = ("vacancy_id, employer_id, name, role, area, experience, schedule, employment, "
           "salary_from, salary_to, currency, gross, key_skills")
GO = "regexp_matches(name, '(^|[^A-Za-z0-9_])(?i:go|golang)([^A-Za-z0-9_]|$)')"


def main() -> None:
    con = duck.connect()
    raw = duck.csv_source()
    parquet_url, segment_url = settings.s3_url(settings.PARQUET_KEY), settings.s3_url(settings.SEGMENT_CSV_KEY)
    t0 = time.perf_counter()
    con.execute(f"COPY (SELECT {COLUMNS} FROM {raw}) TO '{parquet_url}' (FORMAT parquet, COMPRESSION zstd)")
    t1 = time.perf_counter()
    con.execute(f"COPY (SELECT * FROM {raw} WHERE {GO}) TO '{segment_url}' (HEADER)")
    t2 = time.perf_counter()

    raw_size = s3.object_size(settings.RAW_KEY)
    print(f"Исходный объект s3://{settings.BUCKET}/{settings.RAW_KEY}: {raw_size:,} байт")
    for key, took in [(settings.PARQUET_KEY, t1 - t0), (settings.SEGMENT_CSV_KEY, t2 - t1)]:
        size = s3.object_size(key)
        print(f"s3://{settings.BUCKET}/{key}: {size:,} байт ({100 * size / raw_size:.1f} % исходного), "
              f"записан за {took:.1f} с")
    rows_parquet = con.sql(f"SELECT count(*) FROM {duck.parquet_source()}").fetchone()[0]
    rows_segment = con.sql(f"SELECT count(*) FROM {duck.csv_source(settings.SEGMENT_CSV_KEY)}").fetchone()[0]
    print(f"Строк: Parquet {rows_parquet:,}, сегмент Go {rows_segment:,}")
    print("Parquet: row group, столбец, сжатый и несжатый размер")
    con.sql(
        f"SELECT row_group_id, path_in_schema AS column, total_compressed_size AS compressed, "
        f"total_uncompressed_size AS uncompressed FROM parquet_metadata('{parquet_url}') "
        f"WHERE row_group_id = 0 ORDER BY column_id"
    ).show(max_width=200)


if __name__ == "__main__":
    main()
