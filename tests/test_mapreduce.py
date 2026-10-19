"""Тесты своего MapReduce и правил варианта Go.

RustFS подменяется словарём в памяти (hw.s3.get_range / hw.s3.object_size отдают bytearray, как
настоящие). Эталон — csv-модуль над всем текстом файла и тот же mapper. Правила normalize.py
дополнительно сверяются с теми же SQL-файлами в DuckDB.

    uv run pytest -q tests
"""

import csv
import functools
import io
import itertools
import math
import multiprocessing
import operator
import random
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import duckdb
import pytest

from hw import duck, jobs, mapreduce, s3, settings
from hw import normalize as N

KEY = "test/data.csv"

# ---------------------------------------------------------------- фейковое хранилище


def fake_get_range(objects: dict[str, bytes], key: str, start: int, end: int) -> bytearray:
    """Как S3: байты [start, end] включительно, конец обрезается по размеру, пустой диапазон — 416."""
    data = objects[key]
    if not 0 <= start <= end or start >= len(data):
        raise ValueError(f"InvalidRange bytes={start}-{end} for object of {len(data)} bytes")
    return bytearray(data[start : end + 1])


def install_fake_s3(objects: dict[str, bytes]) -> None:
    """Инициализатор процессов пула: spawn не наследует monkeypatch родителя."""
    s3.get_range = functools.partial(fake_get_range, objects)
    s3.object_size = lambda key: len(objects[key])


@pytest.fixture
def store(monkeypatch, tmp_path) -> dict[str, bytes]:
    objects: dict[str, bytes] = {}
    monkeypatch.setattr(s3, "get_range", functools.partial(fake_get_range, objects))
    monkeypatch.setattr(s3, "object_size", lambda key: len(objects[key]))
    monkeypatch.setattr(settings, "WORK_DIR", tmp_path / "work")
    return objects


# ---------------------------------------------------------------- задачи и эталон


def map_fields(fields, cols):
    """Ключ на всю запись и на каждое поле: потерянная, лишняя или испорченная строка видна сразу."""
    yield ("row", *fields), 1
    for name, i in cols.items():
        yield (name, fields[i]), 1


def map_values(fields, cols):
    for name, i in cols.items():
        yield (name,), fields[i]


def reduce_sorted(key, values):
    return sorted(values)


COUNT_FIELDS = mapreduce.Job("count_fields", map_fields, jobs.reduce_sum, combiner=operator.add)
COLLECT_VALUES = mapreduce.Job("collect_values", map_values, reduce_sorted)  # без combiner: все сырые значения


def reference(job: mapreduce.Job, data: bytes) -> dict:
    rows = list(csv.reader(io.StringIO(data.decode("utf-8"))))
    if not rows:
        return {}
    cols = {name: i for i, name in enumerate(rows[0])}
    groups = defaultdict(list)
    for fields in rows[1:]:
        if len(fields) == len(cols):
            for key, value in job.mapper(fields, cols):
                groups[key].append(value)
    return {key: job.reducer(key, values) for key, values in groups.items()}


def data_lines(data: bytes) -> int:
    """Физических строк после заголовка (последняя может быть без перевода строки)."""
    lines = data.count(b"\n") + (1 if data and not data.endswith(b"\n") else 0)
    return max(lines - 1, 0)


def check_engines(job, data, split_size, *, workers=1, reducers=3):
    """run() во всех режимах и run_baseline() дают ровно эталон; записи посчитаны по одному разу."""
    expected = reference(job, data)
    combiners = (True, False) if job.combiner else (False,)
    prefilters = (True, False) if job.prefilter else (False,)
    for use_combiner, use_prefilter in itertools.product(combiners, prefilters):
        res = mapreduce.run(
            job, KEY, split_size=split_size, workers=workers, reducers=reducers,
            use_combiner=use_combiner, use_prefilter=use_prefilter,
        )
        mode = f"split_size={split_size} combiner={use_combiner} prefilter={use_prefilter}"
        assert res.results == expected, mode
        assert sum(st["records"] for st in res.map_stats) == data_lines(data), mode
    base = mapreduce.run_baseline(job, KEY, split_size=split_size)
    assert base.results == expected, f"baseline split_size={split_size}"
    assert sum(st["records"] for st in base.map_stats) == data_lines(data)


# ---------------------------------------------------------------- синтетические CSV

TOKENS = list("abcxyz0129 _-;.") + [",", '"', '""', ",\"", "go", "Go", "GOLANG", "ц", "Ж", "é", "😀", "\u00a0"]


def random_text(rng: random.Random, n: int) -> str:
    return "".join(rng.choice(TOKENS) for _ in range(n))


def make_csv(rng, rows, *, long_rows=(), long_len=0, trailing_newline=True, noise=True) -> bytes:
    """CSV через csv.writer: запятые и кавычки в полях экранированы; иногда битые и пустые строки."""
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(["id", "name", "text", "num"])
    for i in range(rows):
        if noise and rng.random() < 0.05:
            out.write(rng.choice([f"{i},only,three\n", "\n", '"one quoted field"\n']))
            continue
        text = random_text(rng, long_len) if i in long_rows else random_text(rng, rng.randint(0, 10))
        w.writerow([str(i), random_text(rng, rng.randint(0, 6)), text, str(rng.randint(-9, 9))])
    data = out.getvalue().encode("utf-8")
    return data if trailing_newline else data.rstrip(b"\n")


def fixed_width_csv(rows: int) -> bytes:
    """Все строки ровно по 10 байт: сплит кратного размера кончается точно на переводе строки."""
    lines = ["id1,id2,c\n"] + [f"{i:03d},{i % 7:02d},{i % 3:02d}\n" for i in range(rows)]
    assert all(len(line) == 10 for line in lines)
    return "".join(lines).encode()


SMALL = make_csv(random.Random(1), 40)
SMALL_NO_NL = make_csv(random.Random(2), 40, trailing_newline=False)
RNG_SIZES = random.Random(3).sample(range(1, 201), 12)


# ---------------------------------------------------------------- чтение сплитов


@pytest.mark.parametrize("tail_step", [1, 3, mapreduce.TAIL_STEP])
@pytest.mark.parametrize(
    "data",
    [SMALL, SMALL_NO_NL, fixed_width_csv(30), make_csv(random.Random(4), 5, long_rows={2}, long_len=500)],
    ids=["small", "no_trailing_newline", "fixed_width", "long_line"],
)
def test_read_split_owns_every_line_exactly_once(store, monkeypatch, data, tail_step):
    """Для любого размера сплита 1..200: тела сплитов, склеенные по порядку, — ровно весь файл."""
    store[KEY] = data
    monkeypatch.setattr(mapreduce, "TAIL_STEP", tail_step)
    for split_size in range(1, 201):
        pos = 0
        for split in mapreduce.make_splits(len(data), split_size):
            body, requests, _ = mapreduce.read_split(KEY, split, len(data))
            if body:
                assert split.start <= pos < split.end, (split_size, split)  # первый байт строки — в сплите
                assert pos == 0 or data[pos - 1 : pos] == b"\n", (split_size, split)
                assert body == data[pos : pos + len(body)], (split_size, split)
                assert body.endswith(b"\n") or pos + len(body) == len(data), (split_size, split)
            pos += len(body)
            assert requests >= 1
        assert pos == len(data), split_size


@pytest.mark.parametrize("split_size", sorted({1, 2, 3, 4, 5, 7, 11, 16, 29, 64, 97, 128, 200, *RNG_SIZES}))
@pytest.mark.parametrize("job", [COUNT_FIELDS, COLLECT_VALUES], ids=["count", "collect"])
def test_run_matches_reference_small_splits(store, job, split_size):
    store[KEY] = SMALL
    check_engines(job, SMALL, split_size)


@pytest.mark.parametrize("split_size", [1, 2, 3, 5, 13, 50, 199, 10**6])
def test_run_file_without_trailing_newline(store, split_size):
    store[KEY] = SMALL_NO_NL
    check_engines(COUNT_FIELDS, SMALL_NO_NL, split_size)


@pytest.mark.parametrize("split_size", [1, 9, 10, 11, 19, 20, 21, 30, 100, 310, 320])
def test_run_splits_ending_exactly_on_newline(store, split_size):
    data = fixed_width_csv(31)
    store[KEY] = data
    check_engines(COUNT_FIELDS, data, split_size)


def test_run_split_boundaries_at_every_newline(store):
    """Первый сплит кончается сразу после n-го перевода строки, на нём или байтом позже."""
    data = SMALL
    store[KEY] = data
    newlines = [i for i, b in enumerate(data) if b == 10][:12]
    for split_size in sorted({s for nl in newlines for s in (nl, nl + 1, nl + 2) if s > 0}):
        check_engines(COUNT_FIELDS, data, split_size, reducers=2)


@pytest.mark.parametrize("trailing_newline", [True, False])
@pytest.mark.parametrize("split_size", [1000, 4096, 65535, 65536, 65537, 100_000, 150_001, 10**7])
def test_run_lines_longer_than_split_and_tail_step(store, split_size, trailing_newline):
    """Строки ~100 КБ — длиннее сплита и TAIL_STEP (64 КиБ): первая, средняя и последняя.

    Поле короче 131 072 символов — предела csv.field_size_limit() по умолчанию.
    """
    data = make_csv(random.Random(5), 12, long_rows={0, 6, 11}, long_len=70_000, trailing_newline=trailing_newline)
    assert data.count(b"\n") >= 12 and max(map(len, data.split(b"\n"))) > mapreduce.TAIL_STEP
    store[KEY] = data
    check_engines(COUNT_FIELDS, data, split_size, reducers=2)


@pytest.mark.parametrize("tail_step", [1, 2, 7])
@pytest.mark.parametrize("split_size", [1, 3, 8, 33])
def test_run_with_tiny_tail_step(store, monkeypatch, tail_step, split_size):
    monkeypatch.setattr(mapreduce, "TAIL_STEP", tail_step)
    data = make_csv(random.Random(6), 8, long_rows={3}, long_len=120)
    store[KEY] = data
    check_engines(COUNT_FIELDS, data, split_size, reducers=2)


@pytest.mark.parametrize("data", [b"id,name,text\n", b"id,name,text"], ids=["with_newline", "without_newline"])
@pytest.mark.parametrize("split_size", [1, 3, 100])
def test_run_header_only_file(store, data, split_size):
    store[KEY] = data
    check_engines(COUNT_FIELDS, data, split_size)


def test_run_header_and_one_row_without_newline(store):
    data = b"id,name\n1,go"
    store[KEY] = data
    for split_size in range(1, len(data) + 2):
        check_engines(COUNT_FIELDS, data, split_size, reducers=2)


QUOTED_ROWS = [
    ["1", "a,b", 'say "hi"'],
    ["2", '"', ",,"],
    ["3", '""', "x"],
    ["4", "", ""],
    ["5", " lead", "trail "],
    ["6", 'Цgo,"Go"', "golang"],
    ["7", '","', '"a","b"'],
    ["8", "ok", "Ж😀,é"],
]


def test_run_quoted_fields_with_commas_and_doubled_quotes(store):
    out = io.StringIO()
    csv.writer(out, lineterminator="\n").writerows([["id", "name", "text"], *QUOTED_ROWS])
    data = out.getvalue().encode()
    assert b'""' in data and b'","' in data
    store[KEY] = data
    assert reference(COLLECT_VALUES, data)[("name",)] == sorted(row[1] for row in QUOTED_ROWS)
    for split_size in range(1, 61):
        check_engines(COUNT_FIELDS, data, split_size, reducers=2)


@pytest.fixture
def spawn_pool(store, monkeypatch):
    """Пул на spawn (как на macOS по умолчанию); фейк S3 ставится в каждом дочернем процессе."""
    ctx = multiprocessing.get_context("spawn")
    pool = functools.partial(ProcessPoolExecutor, mp_context=ctx, initializer=install_fake_s3, initargs=(store,))
    monkeypatch.setattr(mapreduce, "ProcessPoolExecutor", pool)
    return store


def test_run_two_workers(spawn_pool):
    data = make_csv(random.Random(7), 60, long_rows={10}, long_len=70_000)
    vac = make_vacancies(random.Random(8), 150)
    spawn_pool[KEY] = data
    check_engines(COUNT_FIELDS, data, 7, workers=2)
    spawn_pool[KEY] = vac
    for job in (jobs.SKILL_COUNTS, jobs.SALARY_BY_COMBINATION):
        res = mapreduce.run(job, KEY, split_size=512, workers=2, reducers=3)
        assert res.results == reference(job, vac)


PARENT_CLIENT = "boto3-client-of-parent"


def map_client_in_worker(fields, cols):
    yield ("worker_uses_parent_client", str(s3._client == PARENT_CLIENT)), 1


@pytest.fixture
def default_start_method_fork():
    """Как на Linux в Python 3.11–3.13: пул без mp_context берёт fork."""
    old = multiprocessing.get_start_method(allow_none=True)
    multiprocessing.set_start_method("fork", force=True)
    yield
    multiprocessing.set_start_method(old, force=True)


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_workers_do_not_inherit_parent_s3_client(store, monkeypatch, default_start_method_fork):
    """run() создаёт клиента в родителе (object_size, read_header) до запуска пула. При fork дочерние
    процессы получают его вместе с keep-alive сокетом к RustFS, ответы на Range-запросы разных
    процессов перемешиваются, и счётчики молча расходятся. У каждого процесса должен быть свой клиент."""
    monkeypatch.setattr(s3, "_client", PARENT_CLIENT)
    real_pool = ProcessPoolExecutor
    monkeypatch.setattr(
        mapreduce, "ProcessPoolExecutor",
        lambda *a, **kw: real_pool(*a, initializer=install_fake_s3, initargs=(store,), **kw),
    )
    data = make_csv(random.Random(13), 30, noise=False)
    store[KEY] = data
    job = mapreduce.Job("client_probe", map_client_in_worker, jobs.reduce_sum, combiner=operator.add)
    res = mapreduce.run(job, KEY, split_size=64, workers=2, reducers=2)
    assert res.results == {("worker_uses_parent_client", "False"): 30}


# ---------------------------------------------------------------- задачи варианта Go

VACANCY_COLUMNS = [
    "vacancy_id", "employer_id", "name", "role", "area", "experience", "schedule", "employment",
    "salary_from", "salary_to", "currency", "gross", "key_skills", "description",
]
TITLES = [
    "Go developer", "Golang разработчик", "Senior GO engineer", "go", "GoLang", "Backend (Go/Python)",
    "Разработчик Go", "Go-разработчик", '"Go" developer', "Go, Kotlin", "Lead Golang/Go", "Go1 dev",
    "MongoDB DBA", "Django dev", "Algorithm engineer", "go_dev", "Gopher", "1go", "Цgo", "goд",
    "GOLANGX", "Python", "Ego", "Cargo", "Google",
]
SKILLS = [
    "Go", "golang", "GoLang", " Docker ", "docker", "PostgreSQL", "postgres", "k8s", "Kubernetes", "Git",
    "Linux", "\u00a0Redis\u200b", "REST API", "a,b", 'say "hi"', "", " ", "C#", "С#", "Kafka",
    "Apache Kafka", "SQL", "gRPC", "Микросервисы", "Microservices", "ätest", "zeta", "Ω", "😀",
]
DESCRIPTIONS = ["", "Мы пишем на Go", "mongo и django", "go, golang, GO", "без ключевых слов", 'кавычки "go"']


def make_vacancies(rng: random.Random, rows: int, *, employer_ids=None) -> bytes:
    employer_ids = employer_ids or [str(i) for i in range(1, 16)]
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(VACANCY_COLUMNS)
    prev = None
    for i in range(rows):
        if prev and rng.random() < 0.2:  # то же предложение в другом городе
            row = dict(prev, vacancy_id=str(10_000 + i), area=rng.choice(["Москва", "Казань", "Пермь"]))
        else:
            row = {
                "vacancy_id": str(10_000 + i),
                "employer_id": rng.choice(employer_ids),
                "name": rng.choice(TITLES),
                "role": "Программист, разработчик",
                "area": rng.choice(["Москва", "Санкт-Петербург", "Новосибирск"]),
                "experience": rng.choice(["Нет опыта", "От 1 года до 3 лет", "От 3 до 6 лет", "Более 6 лет"]),
                "schedule": "Полный день",
                "employment": "Полная занятость",
                "salary_from": rng.choice(["", "50000", "100000", "150000.5", "300000", "9000", "2000000"]),
                "salary_to": rng.choice(["", "120000", "250000", "400000", "11000"]),
                "currency": rng.choice(["RUR", "RUR", "RUR", "USD", ""]),
                "gross": rng.choice(["true", "false", ""]),
                "key_skills": ";".join(rng.sample(SKILLS, rng.randint(0, 7))),
                "description": rng.choice(DESCRIPTIONS),
            }
        w.writerow([row[c] for c in VACANCY_COLUMNS])
        prev = row
    return out.getvalue().encode("utf-8")


VACANCIES = make_vacancies(random.Random(9), 400)


@pytest.mark.parametrize("split_size", [37, 256, 1000, 4096, 10**7])
@pytest.mark.parametrize("job", [jobs.SKILL_COUNTS, jobs.SALARY_BY_COMBINATION], ids=["skills", "salary"])
def test_go_jobs_match_reference(store, job, split_size):
    store[KEY] = VACANCIES
    expected = reference(job, VACANCIES)
    assert expected, "в синтетике должны быть Go-вакансии"
    check_engines(job, VACANCIES, split_size)


def duckdb_results(csv_path, out_dir) -> tuple[dict, dict]:
    """Те же SQL-файлы, что в run_duckdb/analyze, но над локальной копией CSV."""
    source = f"read_csv('{csv_path.as_posix()}', header = true, all_varchar = true)"
    con = duckdb.connect()
    try:
        duck.run_sql(con, "go_segment.sql", SOURCE=source)
        duck.run_sql(con, "skill_counts.sql", OUT=out_dir.as_posix())
        duck.run_sql(con, "salary.sql", MIN_OFFERS=1, MIN_EMPLOYERS=1)
        salary = {
            tuple(s for s in (s1, s2, s3) if s is not None): (offers, employers, median)
            for s1, s2, s3, offers, employers, median in con.execute(
                "SELECT skill_1, skill_2, skill_3, offers, employers, median FROM combo_salary"
            ).fetchall()
        }
    finally:
        con.close()
    counts = {}
    for name in ("skills.csv", "pairs.csv", "triples.csv"):
        with open(out_dir / name, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                counts[tuple(v for k, v in row.items() if k.startswith("skill"))] = int(row["vacancies"])
    return counts, salary


def assert_salary_equal(mr: dict, db: dict) -> None:
    assert mr.keys() == db.keys()
    diff = {
        k: (mr[k], db[k]) for k in mr
        if mr[k][:2] != db[k][:2] or not math.isclose(mr[k][2], db[k][2], rel_tol=1e-12, abs_tol=1e-6)
    }
    assert not diff


def test_mapreduce_matches_duckdb_sql(store, tmp_path):
    path = tmp_path / "vacancies.csv"
    path.write_bytes(VACANCIES)
    store[KEY] = VACANCIES
    counts, salary = duckdb_results(path, tmp_path)
    assert mapreduce.run(jobs.SKILL_COUNTS, KEY, split_size=777, workers=1).results == counts
    assert_salary_equal(mapreduce.run(jobs.SALARY_BY_COMBINATION, KEY, split_size=777, workers=1).results, salary)


def test_employers_without_id_counted_like_duckdb(store, tmp_path):
    """Пустой employer_id: count(DISTINCT employer_id) в SQL его не считает — MapReduce тоже не должен."""
    data = make_vacancies(random.Random(10), 300, employer_ids=["", "1", "2", "3"])
    path = tmp_path / "vacancies.csv"
    path.write_bytes(data)
    store[KEY] = data
    _, salary = duckdb_results(path, tmp_path)
    assert_salary_equal(mapreduce.run(jobs.SALARY_BY_COMBINATION, KEY, split_size=777, workers=1).results, salary)


# ---------------------------------------------------------------- предфильтр go_candidate_lines

REF_GO_WORD = re.compile(rb"(?<![A-Za-z0-9_])go(?:lang)?(?![A-Za-z0-9_])", re.I)

ADVERSARIAL = [
    b"GO", b"go", b"Go", b"gO", b"GoLang", b"GOLANG", b"golangx", b"golang_", b"golang1", b"golan", b"golangg",
    b"mongo", b"_go", b"go_", b"go1", b"1go", "Цgo".encode(), "goд".encode(), "Го".encode(), b"django",
    b"algorithm", b"ego", b"gopher", b"cargo", b"gogo", b"go go", b"mongo go", b"mongo,go", b"x,mongo,django,golang",
    b'"Go"', b"(go)", b"go-dev", b"go.dev", b"go/python", b"go\xc2\xa0dev", b"G O", b"g", b"o", b"",
    b"mongo golang mongo", b"django,algorithm,ego", b"1,Go developer,docker", b"Golang;go;GO",
]


def ref_candidates(body: bytes) -> list[bytes]:
    return [line for line in body.split(b"\n") if REF_GO_WORD.search(line)]


def candidates(body: bytes) -> list[bytes]:
    return [bytes(line) for line in N.go_candidate_lines(bytearray(body))]


@pytest.mark.parametrize("line", ADVERSARIAL, ids=lambda b: b.decode("utf-8", "replace") or "empty")
def test_go_candidate_lines_single_line(line):
    for body in (line, line + b"\n", b"x\n" + line, b"x\n" + line + b"\ny"):
        assert candidates(body) == ref_candidates(body), body


def test_go_candidate_lines_adversarial_chunks():
    body = b"\n".join(ADVERSARIAL)
    assert candidates(body) == ref_candidates(body)
    assert candidates(body + b"\n") == ref_candidates(body + b"\n")
    edge = b"go,first\nmiddle mongo\nlast,golang"  # совпадение в самом начале и в самом конце чанка
    assert candidates(edge) == [b"go,first", b"last,golang"]


def test_go_candidate_lines_fuzz():
    rng = random.Random(11)
    alphabet = [b"g", b"o", b"G", b"O", b"lang", b"LaNg", b"l", b"a", b"n", b"_", b"1", b"x", b"m", b"d",
                "Ц".encode(), b" ", b",", b'"', b"\n", b"go", b"golang"]
    for _ in range(20_000):
        body = b"".join(rng.choice(alphabet) for _ in range(rng.randint(0, 40)))
        assert candidates(body) == ref_candidates(body), body


# ---------------------------------------------------------------- is_go_title против RE2 в DuckDB


def duckdb_title_pattern() -> str:
    sql = (settings.SQL_DIR / "go_segment.sql").read_text(encoding="utf-8")
    return re.search(r"regexp_matches\(name, ('[^']*')\)", sql).group(1)


def duckdb_go_titles(titles: list[str]) -> list[bool]:
    con = duckdb.connect()
    try:
        sql = f"SELECT list_transform(?::VARCHAR[], lambda t: regexp_matches(t, {duckdb_title_pattern()}))"
        return con.execute(sql, [titles]).fetchone()[0]
    finally:
        con.close()


TITLE_CASES = [
    "Go", "GO", "go", "gO", "Golang", "GOLANG", "GoLang developer", "Senior Go", "Go-разработчик", "Разработчик Go",
    "(Go)", '"Go"', "Go/Python", "Python/Go", "Go,Kotlin", "go.dev", "Go\u00a0dev", "Go\tdev", "go\u0301",
    "mongo", "MongoDB", "Django", "Algorithm", "Ego", "Cargo", "Gopher", "Google", "golangx", "golang_", "_go",
    "go_", "go1", "1go", "Go1", "Цgo", "goд", "Го", "Golan", "gogo", "Gо (кириллическая о)", "\uff27\uff2f", "\uff27o",
    "Kgo", "\u212ago", "ſgo", "İgo", "ıgo", "go\u212a", "", " ", "mongo go", "django golang",
]


def test_is_go_title_matches_duckdb_re2():
    assert [N.is_go_title(t) for t in TITLE_CASES] == duckdb_go_titles(TITLE_CASES)


def test_is_go_title_fuzz_matches_duckdb_re2():
    rng = random.Random(12)
    alphabet = ["g", "o", "G", "O", "lang", "LANG", "l", "a", "n", "_", "1", "Ц", "д", " ", "-", "/", "(", '"',
                "\u212a", "ſ", "İ", "ı", "ǅ", "\uff27", "\uff4f", "é", "\u00a0", "\u0301", "😀", "\t", "go", "golang"]
    titles = ["".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12))) for _ in range(5000)]
    got, want = [N.is_go_title(t) for t in titles], duckdb_go_titles(titles)
    assert [t for t, a, b in zip(titles, got, want, strict=True) if a != b] == []


# ---------------------------------------------------------------- зарплата и навыки


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("100000", "200000", "RUR", "false"), 150_000),
        (("100000", "200000", "RUR", ""), 150_000),
        (("100000", "", "RUR", "true"), 87_000),
        (("", "200000", "RUR", "false"), 200_000),
        (("", "", "RUR", "false"), None),
        (("100000", "200000", "USD", "false"), None),
        (("100000", "200000", "", "false"), None),
        (("100000", "200000", "rur", "false"), None),
        (("100000", "", "RUR", "TRUE"), 100_000),  # gross сравнивается строго с 'true', как в SQL
        (("0", "100000", "RUR", "false"), 50_000),  # «0» — граница вилки, а не её отсутствие
        (("10000", "", "RUR", "false"), 10_000),
        (("9999.99", "", "RUR", "false"), None),
        (("11494", "", "RUR", "true"), None),  # 11494 × 0,87 < 10 000
        (("1500000", "", "RUR", "false"), 1_500_000),
        (("1500000.01", "", "RUR", "false"), None),
        (("1724137", "", "RUR", "true"), 1724137 * 0.87),
    ],
)
def test_salary_net_rub_cases(args, expected):
    assert N.salary_net_rub(*args) == expected


def test_salary_net_rub_matches_duckdb_sql():
    values = ["", "0", "9999", "10000", "10000.0", "11494.25", "11495", "100000", "150000.5", "1500000",
              "1500000.01", "1724137.9", "1724138", "2000000", "1e5", "-100000"]
    rows = [
        (i, *args)
        for i, args in enumerate(itertools.product(values, values, ["RUR", "USD", "", "rur"], ["true", "false", ""]))
    ]
    sql = (settings.SQL_DIR / "salary.sql").read_text(encoding="utf-8")
    go_salary = re.search(r"CREATE OR REPLACE TABLE go_salary AS.*?;", sql, re.S).group(0)
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE raw (i INT, salary_from VARCHAR, salary_to VARCHAR, currency VARCHAR, gross VARCHAR)")
        con.executemany("INSERT INTO raw VALUES (?, ?, ?, ?, ?)", rows)
        # read_csv(all_varchar) читает пустое поле как NULL
        con.execute("CREATE TABLE go_vacancies AS SELECT i, nullif(salary_from, '') AS salary_from, "
                    "nullif(salary_to, '') AS salary_to, nullif(currency, '') AS currency, nullif(gross, '') AS gross FROM raw")
        con.execute(go_salary)
        db = dict(con.execute("SELECT i, salary FROM go_salary").fetchall())
    finally:
        con.close()
    py = {i: s for i, *args in rows if (s := N.salary_net_rub(*args)) is not None}
    assert py == db


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("", []),
        (";", []),
        (" ; \u00a0 ;\u200b;\ufeff", []),
        ("Go;golang;GoLang;go lang;Голанг", ["go"]),
        ("Docker; docker ;DOCKER", ["docker"]),
        ("k8s;Kubernetes;kuber", ["kubernetes"]),
        ("b;a;c;a", ["a", "b", "c"]),
        ("C#;С#", ["c#"]),  # вторая С — кириллическая, склеивается словарём синонимов
        ("\u00a0Redis\u200b;REDIS", ["redis"]),
        ("a,b;say \"hi\"", ["a,b", 'say "hi"']),
        ("Ω;z;ä;😀;A", ["a", "z", "ä", "ω", "😀"]),
    ],
)
def test_vacancy_skills_cases(field, expected):
    assert N.vacancy_skills(field) == expected


def duckdb_skills(fields: list[str]) -> dict[int, list[str]]:
    """go_skills из sql/go_segment.sql над таблицей, где каждая строка — Go-вакансия с данным key_skills."""
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE src AS SELECT i::VARCHAR AS vacancy_id, '1' AS employer_id, 'Go' AS name, "
            "NULL AS role, NULL AS area, NULL AS experience, NULL AS schedule, NULL AS employment, NULL AS salary_from, "
            "NULL AS salary_to, NULL AS currency, NULL AS gross, nullif(f, '') AS key_skills "
            "FROM unnest(?::VARCHAR[]) WITH ORDINALITY AS t(f, i)",
            [fields],
        )
        duck.run_sql(con, "go_segment.sql", SOURCE="src")
        rows = con.execute(
            "SELECT vacancy_id::INT, list_sort(list(skill)) FROM go_skills GROUP BY vacancy_id"
        ).fetchall()
    finally:
        con.close()
    return {i - 1: skills for i, skills in rows}


def test_vacancy_skills_matches_duckdb_sql():
    fields = [";".join(c) for c in itertools.combinations(SKILLS, 2)] + list(SKILLS) + [
        " ; ;", "Go;;go", "\ufeffGo\ufeff", "\u200b\u00a0\t Go \r", "Apache Kafka;kafka;KAFKA ",
        "Ä;ä;Ж;ж;Ё;ё", "ǅ;ǆ;Ǆ", "ẞ;ß",
    ]
    got = {i: N.vacancy_skills(f) for i, f in enumerate(fields) if N.vacancy_skills(f)}
    assert got == duckdb_skills(fields)


@pytest.mark.parametrize("skill", ["ΟΔΟΣ", "İstanbul"])
def test_skill_lowercase_matches_duckdb_lower(skill):
    """Python str.lower() — полные правила Unicode (сигма в конце слова, İ → i + точка), DuckDB lower() — нет."""
    assert N.vacancy_skills(skill) == duckdb_skills([skill])[0]


def test_skill_combinations():
    assert list(N.skill_combinations(["a", "b", "c"], k_min=0)) == [
        (), ("a",), ("b",), ("c",), ("a", "b"), ("a", "c"), ("b", "c"), ("a", "b", "c"),
    ]
    assert list(N.skill_combinations([])) == []
    assert list(N.skill_combinations([], k_min=0)) == [()]


# ---------------------------------------------------------------- отказ маппера


def test_map_task_error_is_retried(store):
    """Исключение в map-таске: таск перезапускается, результат тот же, что без отказа."""
    data = make_vacancies(random.Random(21), 150)
    store[KEY] = data
    res = mapreduce.run(jobs.SKILL_COUNTS, KEY, split_size=512, workers=1, reducers=2,
                        failure=mapreduce.Failure(split=1, mode="error"))
    assert res.results == reference(jobs.SKILL_COUNTS, data)
    assert any("map-таск 1: попытка 1" in e for e in res.retries)


def test_worker_crash_is_retried(spawn_pool):
    """Процесс-воркер умирает посреди map-фазы: пул пересоздаётся, незавершённые таски идут заново."""
    data = make_vacancies(random.Random(22), 150)
    spawn_pool[KEY] = data
    res = mapreduce.run(jobs.SKILL_COUNTS, KEY, split_size=512, workers=2, reducers=2,
                        failure=mapreduce.Failure(split=2, mode="crash"))
    assert res.results == reference(jobs.SKILL_COUNTS, data)
    assert any("BrokenProcessPool" in e for e in res.retries)
