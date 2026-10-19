"""Этап 2: загрузка датасета вакансий в RustFS и проверка целостности.

    uv run python -m scripts.upload | tee logs/02_upload.txt
"""

import hashlib
import subprocess
import time

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from hw import s3, settings

MB = 2**20


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8 * MB):
            h.update(chunk)
    return h.hexdigest()


def sha256_object(key: str) -> str:
    h = hashlib.sha256()
    body = s3.client().get_object(Bucket=settings.BUCKET, Key=key)["Body"]
    for chunk in body.iter_chunks(8 * MB):
        h.update(chunk)
    return h.hexdigest()


def ensure_bucket() -> None:
    c = s3.client()
    try:
        c.head_bucket(Bucket=settings.BUCKET)
        print(f"Бакет {settings.BUCKET} уже есть")
    except ClientError:
        c.create_bucket(Bucket=settings.BUCKET)
        print(f"Создан бакет {settings.BUCKET}")


def main() -> None:
    src = settings.SOURCE_CSV
    print(f"RustFS: {settings.S3_ENDPOINT}")
    print(f"Источник: {src.name} ({src.stat().st_size:,} байт)")
    local_sha = sha256_file(src)
    print(f"SHA-256 локально: {local_sha}")

    ensure_bucket()
    cfg = TransferConfig(multipart_threshold=16 * MB, multipart_chunksize=16 * MB, max_concurrency=8)
    t0 = time.perf_counter()
    s3.client().upload_file(str(src), settings.BUCKET, settings.RAW_KEY, Config=cfg)
    took = time.perf_counter() - t0
    size = src.stat().st_size
    print(f"Загружено s3://{settings.BUCKET}/{settings.RAW_KEY} за {took:.1f} с ({size / MB / took:.0f} МиБ/с)")

    head = s3.client().head_object(Bucket=settings.BUCKET, Key=settings.RAW_KEY)
    print(f"HEAD: размер {head['ContentLength']:,} байт, ETag {head['ETag']}")
    remote_sha = sha256_object(settings.RAW_KEY)
    print(f"SHA-256 из RustFS: {remote_sha}")
    print("Целостность:", "OK, хеши совпали" if remote_sha == local_sha else "ОШИБКА, хеши разные")

    print("\nОбъекты с префиксом raw/:")
    for obj in s3.client().list_objects_v2(Bucket=settings.BUCKET, Prefix="raw/").get("Contents", []):
        print(f"  {obj['Size']:>13,}  {obj['Key']}")

    print("\nСколько этот объект занимает на каждой папке-диске RustFS (EC 2+2: 2 части данных + 2 чётности):")
    du = subprocess.run(
        ["docker", "exec", "hw3-rustfs", "sh", "-c", f"du -sb /data/rustfs*/{settings.BUCKET}/{settings.RAW_KEY}"],
        capture_output=True, text=True,
    )
    print(du.stdout.strip() or du.stderr.strip())


if __name__ == "__main__":
    main()
