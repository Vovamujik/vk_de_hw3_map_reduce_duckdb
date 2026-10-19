"""Этапы 5 и 6: влияние навыков на зарплату и сравнение трёх регионов (DuckDB, SQL в sql/).

    uv run python -m scripts.analyze | tee logs/06_analysis.txt
"""

import json

import numpy as np

from hw import duck, s3, settings
from hw.profiling import Profile

OUT = settings.RESULTS_DIR / "analysis"
MIN_OFFERS, MIN_EMPLOYERS = 15, 5  # комбинация должна встречаться в 15+ предложениях от 5+ работодателей
REGIONS = ["Москва", "Санкт-Петербург", "Екатеринбург"]  # три крупнейших региона по числу Go-вакансий
TOP = 10
N_PERM, SEED = 5000, 20261019  # перестановочный тест: число перестановок и зерно генератора
MID_EXPERIENCE = "От 3 до 6 лет"  # самый частый уровень опыта: сравнение внутри него убирает влияние опыта
EXPORT = [
    "salary_overview", "salary_by_experience", "salary_by_skill_count", "salary_quartiles",
    "skill_quartile_share", "salary_bounds", "salary_both_bounds", "combo_salary", "combo_effect",
    "combo_robustness", "region_summary", "region_skills", "region_salary_by_experience", "region_tests",
]
ORDER_UP = "delta DESC, above_pct DESC, offers DESC, combo"
ORDER_DOWN = "delta ASC, above_pct ASC, offers DESC, combo"


def show(con, title: str, sql: str, max_rows: int = 40) -> None:
    print(f"\n== {title}")
    con.sql(sql).show(max_width=250, max_rows=max_rows)


def robustness(con) -> None:
    """Насколько устойчива разница медиан у каждой комбинации-кандидата.

    p_value — двусторонний перестановочный тест: как часто случайное разбиение предложений с навыками
    на группы того же размера даёт разницу медиан не меньше наблюдаемой. mid_delta — та же разница
    только среди предложений с опытом 3–6 лет (если в группе «с комбинацией» их хотя бы 5).
    """
    offers = con.sql("SELECT vacancy_id, salary, experience FROM go_offers "
                     "WHERE vacancy_id IN (SELECT vacancy_id FROM go_skills) ORDER BY vacancy_id").fetchall()
    pos = {vid: i for i, (vid, _, _) in enumerate(offers)}
    salary = np.array([s for _, s, _ in offers])
    mid = np.array([e == MID_EXPERIENCE for *_, e in offers])
    members = con.sql(
        "SELECT e.k, e.combo, list(x.vacancy_id) FROM combo_effect e JOIN offer_combos x "
        "ON x.k = e.k AND x.skill_1 = e.skill_1 AND x.skill_2 IS NOT DISTINCT FROM e.skill_2 "
        "AND x.skill_3 IS NOT DISTINCT FROM e.skill_3 GROUP BY e.k, e.combo ORDER BY e.k, e.combo"
    ).fetchall()
    rng = np.random.default_rng(SEED)
    shuffled = salary[np.argsort(rng.random((N_PERM, len(salary))), axis=1)]
    rows = []
    for k, combo, ids in members:
        mask = np.zeros(len(salary), dtype=bool)
        mask[[pos[v] for v in ids]] = True
        m = int(mask.sum())
        observed = float(np.median(salary[mask]) - np.median(salary[~mask]))
        diffs = np.median(shuffled[:, :m], axis=1) - np.median(shuffled[:, m:], axis=1)
        p_value = (np.count_nonzero(np.abs(diffs) >= abs(observed) - 1e-9) + 1) / (N_PERM + 1)
        with_mid, without_mid = mask & mid, ~mask & mid
        mid_delta = float(np.median(salary[with_mid]) - np.median(salary[without_mid])) if with_mid.sum() >= 5 else None
        rows.append((k, combo, observed, round(p_value, 4), int(with_mid.sum()), mid_delta))
    con.execute("CREATE OR REPLACE TABLE combo_robustness "
                "(k INTEGER, combo VARCHAR, observed_delta DOUBLE, p_value DOUBLE, mid_offers INTEGER, mid_delta DOUBLE)")
    con.executemany("INSERT INTO combo_robustness VALUES (?, ?, ?, ?, ?, ?)", rows)


def region_tests(con) -> None:
    """Разница медиан зарплат между регионами и её перестановочный p (по уникальным предложениям региона)."""
    by_area: dict[str, np.ndarray] = {}
    for area in REGIONS:
        rows = con.sql("SELECT salary FROM region_offers WHERE area = ?", params=[area]).fetchall()
        by_area[area] = np.array([r[0] for r in rows])
    rng = np.random.default_rng(SEED)
    out = []
    for i, a in enumerate(REGIONS):
        for b in REGIONS[i + 1:]:
            x, y = by_area[a], by_area[b]
            both = np.concatenate([x, y])
            observed = float(np.median(x) - np.median(y))
            shuffled = both[np.argsort(rng.random((N_PERM, len(both))), axis=1)]
            diffs = np.median(shuffled[:, :len(x)], axis=1) - np.median(shuffled[:, len(x):], axis=1)
            p_value = (np.count_nonzero(np.abs(diffs) >= abs(observed) - 1e-9) + 1) / (N_PERM + 1)
            out.append((a, b, len(x), len(y), observed, round(p_value, 4)))
    con.execute("CREATE OR REPLACE TABLE region_tests "
                "(area_a VARCHAR, area_b VARCHAR, offers_a INTEGER, offers_b INTEGER, median_diff DOUBLE, p_value DOUBLE)")
    con.executemany("INSERT INTO region_tests VALUES (?, ?, ?, ?, ?, ?)", out)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = duck.connect()
    regions = ", ".join(f"({i}, '{area}')" for i, area in enumerate(REGIONS, 1))
    with Profile() as prof:
        duck.run_sql(con, "go_segment.sql", SOURCE=duck.csv_source())
        duck.run_sql(con, "salary.sql", MIN_OFFERS=MIN_OFFERS, MIN_EMPLOYERS=MIN_EMPLOYERS)
        duck.run_sql(con, "regions.sql", REGIONS=regions)
        robustness(con)
        region_tests(con)
    print("профиль:", ", ".join(f"{k} {v}" for k, v in prof.metrics.items()))
    print(f"пороги: комбинация в >= {MIN_OFFERS} предложениях от >= {MIN_EMPLOYERS} работодателей")

    show(con, "Сегмент Go: профессиональные роли", "SELECT role, count(*) vacancies FROM go_vacancies "
         "GROUP BY role ORDER BY vacancies DESC")
    show(con, "Сегмент Go: Go — название сервиса, а не язык",
         "SELECT name, count(*) vacancies, count(salary_from) + count(salary_to) with_salary FROM go_vacancies "
         "WHERE name ILIKE '%яндекс go%' GROUP BY name")
    show(con, "Сегмент Go: навыки с zero-width space по краям",
         "SELECT count(DISTINCT raw) skills, count(*) mentions FROM (SELECT unnest(string_split(key_skills, ';')) raw "
         "FROM go_vacancies) WHERE raw LIKE '%' || chr(8203) || '%'")
    show(con, "Без нормализации (только обрезка пробелов): самые частые навыки",
         "SELECT trim(raw) skill, count(*) vacancies FROM (SELECT unnest(string_split(key_skills, ';')) raw "
         "FROM go_vacancies) GROUP BY 1 ORDER BY 2 DESC LIMIT 4")
    show(con, "Без синонимов (обрезка и нижний регистр): самые частые пары",
         "WITH s AS (SELECT DISTINCT vacancy_id, lower(trim(raw)) skill FROM (SELECT vacancy_id, "
         "unnest(string_split(key_skills, ';')) raw FROM go_vacancies) WHERE trim(raw) <> '') "
         "SELECT a.skill skill_1, b.skill skill_2, count(*) vacancies FROM s a JOIN s b "
         "ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill GROUP BY ALL ORDER BY vacancies DESC LIMIT 3")
    show(con, "Пример копий: больше всего записей с одним названием у одного работодателя",
         "SELECT employer_id, name, count(*) records, count(DISTINCT area) cities, "
         "count(DISTINCT (salary_from, salary_to, gross)) salary_ranges, max(c) max_identical FROM "
         "(SELECT *, count(*) OVER (PARTITION BY employer_id, name, key_skills, salary_from, salary_to, gross) c "
         "FROM go_salary) GROUP BY employer_id, name ORDER BY records DESC LIMIT 3")

    show(con, "Этап 5. Зарплаты Go-вакансий (руб./мес. на руки, по уникальным предложениям)",
         "SELECT * REPLACE (p25::INT AS p25, median::INT AS median, p75::INT AS p75) FROM salary_overview")
    show(con, "Медиана по опыту", "SELECT experience, offers, p25::INT p25, median::INT median, p75::INT p75 "
         "FROM salary_by_experience")
    show(con, "Медиана по числу навыков в вакансии", "SELECT skills, offers, median::INT median FROM salary_by_skill_count")
    show(con, "Как указана вилка (одна граница берётся как точка)",
         "SELECT bounds, offers, median::INT median, share_of_q4_pct, without_skills FROM salary_bounds")
    show(con, "Предложения без списка навыков",
         "SELECT count(*) offers, count(DISTINCT employer_id) employers, max(n) top_employer_offers, "
         "sum(only_to) only_to, median(salary)::INT median FROM (SELECT employer_id, salary, "
         "count(*) OVER (PARTITION BY employer_id) n, (salary_from IS NULL)::INT only_to FROM go_offers "
         "WHERE vacancy_id NOT IN (SELECT vacancy_id FROM go_skills))")
    show(con, "Квартили зарплаты и навыки",
         "SELECT quartile, offers, min_salary::INT min_salary, max_salary::INT max_salary, median::INT median, "
         "avg_skills, top_skills FROM salary_quartiles")
    show(con, "Навыки, которые чаще встречаются в верхнем квартиле (Q4), чем в нижнем (Q1)",
         f"SELECT * FROM skill_quartile_share ORDER BY q4_minus_q1 DESC LIMIT {TOP}")
    show(con, "Навыки, которые чаще встречаются в нижнем квартиле (Q1), чем в верхнем (Q4)",
         f"SELECT * FROM skill_quartile_share ORDER BY q4_minus_q1 ASC LIMIT {TOP}")
    show(con, "Проверка на предложениях с обеими границами вилки (квартили внутри этой части)",
         "SELECT quartile, offers, median::INT median, avg_skills, without_skills FROM salary_both_bounds")
    print("\nкандидатов (k: число):", con.sql("SELECT k, count(*) FROM combo_effect GROUP BY k ORDER BY k").fetchall())
    print(f"p — перестановочный тест ({N_PERM} перестановок); mid — разница медиан среди предложений с опытом 3–6 лет")
    cols = ("combo, offers, employers, median_with::INT median_with, median_without::INT median_without, "
            "delta::INT delta, delta_pct, above_pct, r.p_value p, r.mid_offers mid_n, r.mid_delta::INT mid_delta")
    for k, label in [(1, "навыки"), (2, "пары навыков"), (3, "тройки навыков")]:
        for order, verb in [(ORDER_UP, "повышают"), (ORDER_DOWN, "понижают")]:
            show(con, f"{label}: сильнее всего {verb} медиану",
                 f"SELECT {cols} FROM combo_effect JOIN combo_robustness r USING (k, combo) "
                 f"WHERE k = {k} ORDER BY {order} LIMIT {TOP}")

    show(con, "Этап 6. Три региона и весь сегмент",
         "SELECT * REPLACE (p25::INT AS p25, median::INT AS median, p75::INT AS p75) FROM region_summary")
    show(con, "Топ-10 навыков по регионам (доля среди вакансий региона с навыками)",
         f"SELECT area, rnk, skill, postings, share_pct, segment_share_pct, lift FROM region_skills "
         f"WHERE rnk <= {TOP} ORDER BY ord, rnk", max_rows=40)
    show(con, "Отличительные навыки региона (5+ вакансий от 3+ работодателей, наибольший лифт к сегменту)",
         "SELECT area, skill, postings, employers, share_pct, segment_share_pct, lift FROM region_skills "
         "WHERE postings >= 5 AND employers >= 3 "
         "QUALIFY row_number() OVER (PARTITION BY ord ORDER BY lift DESC, postings DESC, skill) <= 5 ORDER BY ord, lift DESC")
    show(con, "Разница медиан между регионами (перестановочный тест)",
         "SELECT area_a, area_b, offers_a, offers_b, median_diff::INT median_diff, p_value FROM region_tests")
    show(con, "Медиана по опыту в регионах",
         "SELECT area, experience, offers, median::INT median FROM region_salary_by_experience")

    for table in EXPORT:
        con.execute(f"COPY {table} TO '{(OUT / table).as_posix()}.csv' (HEADER)")
    rel = con.sql("SELECT * FROM salary_overview")
    overview = dict(zip(rel.columns, rel.fetchone()))
    summary = {"min_offers": MIN_OFFERS, "min_employers": MIN_EMPLOYERS, "regions": REGIONS,
               "overview": overview, "profile": prof.metrics}
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    keys = s3.upload_dir(OUT, f"{settings.RESULTS_PREFIX}/analysis")
    print(f"\nРезультаты в RustFS: {len(keys)} файлов в s3://{settings.BUCKET}/{settings.RESULTS_PREFIX}/analysis/")


if __name__ == "__main__":
    main()
