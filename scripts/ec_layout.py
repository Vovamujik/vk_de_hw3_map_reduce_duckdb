"""Как объект разложен по папкам-дискам RustFS: схема Erasure Coding из xl.meta каждого диска.

xl.meta — это "XL2 " + msgpack; файл читается из контейнера через docker exec (декодер из ДЗ №2).

    uv run python -m scripts.ec_layout | tee logs/01b_ec_layout.txt
"""

import io
import subprocess
import sys

import msgpack

from hw import settings

CONTAINER = "hw3-rustfs"
DISKS = [f"/data/rustfs{i}" for i in range(4)]


def read_version(disk: str, key: str) -> dict:
    path = f"{disk}/{settings.BUCKET}/{key}/xl.meta"
    raw = subprocess.run(["docker", "exec", CONTAINER, "cat", path], capture_output=True, check=True).stdout
    if raw[:4] != b"XL2 ":
        sys.exit(f"{path}: не xl.meta")
    meta_block = next(msgpack.Unpacker(io.BytesIO(raw[8:]), raw=False, strict_map_key=False))
    inner = list(msgpack.Unpacker(io.BytesIO(meta_block), raw=False, strict_map_key=False))
    return msgpack.unpackb(inner[4], raw=False, strict_map_key=False)["V2Obj"]


def main() -> None:
    key = sys.argv[1] if len(sys.argv) > 1 else settings.RAW_KEY
    print(f"Объект s3://{settings.BUCKET}/{key}")
    first = None
    for disk in DISKS:
        v = read_version(disk, key)
        first = first or v
        role = "данные " if v["EcIndex"] <= v["EcM"] else "чётность"
        print(f"{disk}: шард {v['EcIndex']} из {v['EcM'] + v['EcN']} — {role}")
    parts = first.get("PartNums") or [1]
    print(f"Схема: EcM = {first['EcM']} (части данных), EcN = {first['EcN']} (части чётности); "
          f"блок {first['EcBSize']:,} байт; частей multipart: {len(parts)}; порядок дисков EcDist = {first['EcDist']}")


if __name__ == "__main__":
    main()
