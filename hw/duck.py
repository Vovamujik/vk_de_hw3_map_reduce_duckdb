"""DuckDB поверх RustFS: подключение через httpfs и выполнение SQL-шаблонов из sql/."""

import re

import duckdb

from hw import settings


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    scheme, host = settings.S3_ENDPOINT.split("://", 1)
    con.execute(
        f"""CREATE OR REPLACE SECRET rustfs (
            TYPE s3, KEY_ID {_quote(settings.S3_ACCESS_KEY)}, SECRET {_quote(settings.S3_SECRET_KEY)},
            ENDPOINT {_quote(host)}, URL_STYLE 'path', USE_SSL {str(scheme == "https").lower()},
            REGION {_quote(settings.S3_REGION)})"""
    )
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    return con


def csv_source(key: str = settings.RAW_KEY) -> str:
    """Все столбцы как текст — так DuckDB видит ровно те же строки, что и csv-модуль Python."""
    return f"read_csv({_quote(settings.s3_url(key))}, header = true, all_varchar = true)"


def csv_auto_source(key: str = settings.RAW_KEY) -> str:
    return f"read_csv_auto({_quote(settings.s3_url(key))})"


def parquet_source(key: str = settings.PARQUET_KEY) -> str:
    return f"read_parquet({_quote(settings.s3_url(key))})"


def render(name: str, **params) -> str:
    """Подставляет ${NAME} в SQL-файл; неизвестное имя — ошибка, а не пустая строка."""
    sql = (settings.SQL_DIR / name).read_text(encoding="utf-8")
    return re.sub(r"\$\{(\w+)\}", lambda m: str(params[m.group(1)]), sql)


def run_sql(con: duckdb.DuckDBPyConnection, name: str, **params) -> None:
    con.execute(render(name, ALIASES=settings.ALIASES_CSV.as_posix(), **params))
