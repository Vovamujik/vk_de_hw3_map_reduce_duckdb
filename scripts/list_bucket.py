"""Что лежит в бакете RustFS: исходный объект, производные объекты и результаты обоих способов.

    uv run python -m scripts.list_bucket | tee logs/10_bucket.txt
"""

from hw import s3, settings


def main() -> None:
    paginator = s3.client().get_paginator("list_objects_v2")
    total = count = 0
    print(f"s3://{settings.BUCKET}/")
    for page in paginator.paginate(Bucket=settings.BUCKET):
        for obj in page.get("Contents", []):
            print(f"  {obj['Size']:>13,}  {obj['LastModified']:%Y-%m-%d %H:%M}  {obj['Key']}")
            total, count = total + obj["Size"], count + 1
    print(f"объектов: {count}, всего {total:,} байт")


if __name__ == "__main__":
    main()
