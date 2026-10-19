-- Этап 5. Зарплаты Go-вакансий и навыки. Правила — как в hw/normalize.py (salary_net_rub, OFFER_FIELDS).
-- Нужны go_vacancies и go_skills (sql/go_segment.sql); ${MIN_OFFERS}, ${MIN_EMPLOYERS} — пороги поддержки.

-- Зарплата одной точкой, руб./мес. на руки: только рубли; середина вилки или известная граница;
-- «до вычета налогов» × 0,87; всё вне 10 тыс.–1,5 млн — выброс.
CREATE OR REPLACE TABLE go_salary AS
WITH parsed AS (
    SELECT *, TRY_CAST(salary_from AS DOUBLE) AS lo, TRY_CAST(salary_to AS DOUBLE) AS hi
    FROM go_vacancies
    WHERE currency = 'RUR' AND (salary_from IS NOT NULL OR salary_to IS NOT NULL)
), point AS (
    SELECT *,
           CASE WHEN lo IS NOT NULL AND hi IS NOT NULL THEN (lo + hi) / 2 ELSE coalesce(lo, hi) END
           * CASE WHEN gross = 'true' THEN 0.87 ELSE 1.0 END AS salary
    FROM parsed
)
SELECT * EXCLUDE (lo, hi) FROM point
WHERE salary BETWEEN 10000 AND 1500000;

-- Уникальные предложения: копии одной вакансии (тот же работодатель, должность, навыки и вилка) — одно.
CREATE OR REPLACE TABLE go_offers AS
SELECT * FROM go_salary
QUALIFY row_number() OVER (
    PARTITION BY employer_id, name, key_skills, salary_from, salary_to, currency, gross ORDER BY vacancy_id) = 1;

-- Комбинации навыков каждого предложения; k = 0 — «все предложения».
CREATE OR REPLACE TABLE offer_combos AS
WITH sk AS (
    SELECT g.vacancy_id, g.skill FROM go_skills g JOIN go_offers o ON o.vacancy_id = g.vacancy_id
)
SELECT vacancy_id, 0 AS k, NULL::VARCHAR AS skill_1, NULL::VARCHAR AS skill_2, NULL::VARCHAR AS skill_3
FROM go_offers
UNION ALL
SELECT vacancy_id, 1, skill, NULL, NULL FROM sk
UNION ALL
SELECT a.vacancy_id, 2, a.skill, b.skill, NULL
FROM sk a JOIN sk b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill
UNION ALL
SELECT a.vacancy_id, 3, a.skill, b.skill, c.skill
FROM sk a
JOIN sk b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill
JOIN sk c ON c.vacancy_id = a.vacancy_id AND b.skill < c.skill;

-- Для сверки с MapReduce: все комбинации, число предложений и работодателей, медиана.
CREATE OR REPLACE TABLE combo_salary AS
SELECT c.k, c.skill_1, c.skill_2, c.skill_3,
       count(*) AS offers, count(DISTINCT o.employer_id) AS employers, median(o.salary) AS median
FROM offer_combos c JOIN go_offers o USING (vacancy_id)
GROUP BY ALL
ORDER BY k, skill_1, skill_2, skill_3;

-- Влияние комбинации: медиана предложений с ней против медианы остальных предложений со списком навыков
-- (предложения без навыков ничего не говорят о комбинации, поэтому в «без неё» не входят).
-- В парах и тройках нет самого go: он есть почти в каждом предложении, и «go + X» — это просто X.
CREATE OR REPLACE TABLE combo_effect AS
WITH skilled AS (
    SELECT * FROM go_offers WHERE vacancy_id IN (SELECT vacancy_id FROM go_skills)
), cand AS (
    SELECT row_number() OVER (ORDER BY k, skill_1, skill_2, skill_3) AS id, *
    FROM combo_salary
    WHERE k BETWEEN 1 AND 3 AND offers >= ${MIN_OFFERS} AND employers >= ${MIN_EMPLOYERS}
      AND (k = 1 OR NOT list_contains([skill_1, skill_2, skill_3], 'go'))
), has AS (
    SELECT cand.id, x.vacancy_id
    FROM cand JOIN offer_combos x
      ON x.k = cand.k AND x.skill_1 = cand.skill_1
     AND x.skill_2 IS NOT DISTINCT FROM cand.skill_2 AND x.skill_3 IS NOT DISTINCT FROM cand.skill_3
), without AS (
    SELECT cand.id, count(*) AS offers_without, median(o.salary) AS median_without
    FROM cand CROSS JOIN skilled o
    WHERE NOT EXISTS (SELECT 1 FROM has WHERE has.id = cand.id AND has.vacancy_id = o.vacancy_id)
    GROUP BY cand.id
), above AS (
    -- Зарплаты круглые (250, 300 тыс.), у многих комбинаций медиана одинакова. Доля предложений
    -- выше общей медианы (предложений с навыками) различает их устойчивее и служит вторым ключом сортировки.
    SELECT has.id, round(100.0 * avg((o.salary > (SELECT median(salary) FROM skilled))::INT), 1) AS above_pct
    FROM has JOIN go_offers o USING (vacancy_id)
    GROUP BY has.id
)
SELECT cand.k, concat_ws(' + ', cand.skill_1, cand.skill_2, cand.skill_3) AS combo,
       cand.skill_1, cand.skill_2, cand.skill_3, cand.offers, cand.employers,
       cand.median AS median_with, w.offers_without, w.median_without,
       cand.median - w.median_without AS delta,
       round(100 * (cand.median - w.median_without) / w.median_without, 1) AS delta_pct,
       above.above_pct
FROM cand JOIN without w USING (id) JOIN above USING (id)
ORDER BY k, delta DESC, above_pct DESC, offers DESC, combo;

-- Общая картина: сколько вакансий с зарплатой, сколько отсеяно, квартили по предложениям.
CREATE OR REPLACE TABLE salary_overview AS
SELECT
    (SELECT count(*) FROM go_vacancies) AS go_postings,
    (SELECT count(*) FROM go_vacancies WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL) AS with_salary,
    (SELECT count(*) FROM go_vacancies
      WHERE (salary_from IS NOT NULL OR salary_to IS NOT NULL) AND currency <> 'RUR') AS non_rub,
    (SELECT count(*) FROM go_salary) AS rub_valid,
    count(*) AS offers,
    count(DISTINCT employer_id) AS employers,
    count(*) FILTER (WHERE gross = 'true') AS offers_gross,
    count(*) FILTER (WHERE vacancy_id IN (SELECT vacancy_id FROM go_skills)) AS offers_with_skills,
    quantile_cont(salary, 0.25) AS p25,
    median(salary) AS median,
    quantile_cont(salary, 0.75) AS p75,
    round(avg(salary)) AS mean
FROM go_offers;

CREATE OR REPLACE TABLE salary_by_experience AS
SELECT experience, count(*) AS offers,
       quantile_cont(salary, 0.25) AS p25, median(salary) AS median, quantile_cont(salary, 0.75) AS p75
FROM go_offers
GROUP BY experience
ORDER BY CASE experience WHEN 'Нет опыта' THEN 0 WHEN 'От 1 года до 3 лет' THEN 1
                         WHEN 'От 3 до 6 лет' THEN 2 ELSE 3 END;

CREATE OR REPLACE TABLE salary_by_skill_count AS
WITH n AS (
    SELECT o.vacancy_id, o.salary, count(g.skill) AS skills
    FROM go_offers o LEFT JOIN go_skills g USING (vacancy_id)
    GROUP BY ALL
)
SELECT CASE WHEN skills = 0 THEN '0' WHEN skills <= 3 THEN '1–3' WHEN skills <= 6 THEN '4–6'
            WHEN skills <= 9 THEN '7–9' ELSE '10+' END AS skills,
       count(*) AS offers, median(salary) AS median
FROM n
GROUP BY 1
ORDER BY min(n.skills);

-- Квартили зарплаты: границы — p25, медиана, p75; в каждом — топ навыков и их доля.
CREATE OR REPLACE TABLE offer_quartile AS
SELECT o.vacancy_id, o.salary,
       CASE WHEN o.salary <= s.p25 THEN 1 WHEN o.salary <= s.median THEN 2
            WHEN o.salary <= s.p75 THEN 3 ELSE 4 END AS quartile
FROM go_offers o, salary_overview s;

CREATE OR REPLACE TABLE salary_quartiles AS
WITH per_offer AS (
    SELECT q.quartile, q.vacancy_id, q.salary, count(g.skill) AS skills
    FROM offer_quartile q LEFT JOIN go_skills g USING (vacancy_id)
    GROUP BY ALL
), qs AS (
    SELECT quartile, count(*) AS offers, min(salary) AS min_salary, max(salary) AS max_salary,
           median(salary) AS median, count(*) FILTER (WHERE skills > 0) AS with_skills,
           round(avg(skills) FILTER (WHERE skills > 0), 1) AS avg_skills
    FROM per_offer GROUP BY quartile
), cnt AS (
    SELECT q.quartile, g.skill, count(*) AS n
    FROM offer_quartile q JOIN go_skills g USING (vacancy_id)
    GROUP BY q.quartile, g.skill
), ranked AS (
    SELECT *, row_number() OVER (PARTITION BY quartile ORDER BY n DESC, skill) AS rnk FROM cnt
), top AS (
    SELECT r.quartile,
           string_agg(r.skill || ' ' || round(100.0 * r.n / qs.with_skills)::INT || '%', ', ' ORDER BY r.rnk) AS top_skills
    FROM ranked r JOIN qs USING (quartile)
    WHERE r.rnk <= 8
    GROUP BY r.quartile
)
SELECT qs.*, top.top_skills
FROM qs JOIN top USING (quartile)
ORDER BY quartile;

-- Доля предложений с навыком в нижнем и верхнем квартилях (навыки с поддержкой не ниже порога).
CREATE OR REPLACE TABLE skill_quartile_share AS
WITH denom AS (
    SELECT quartile, count(*) AS n FROM offer_quartile
    WHERE vacancy_id IN (SELECT vacancy_id FROM go_skills) GROUP BY quartile
), cnt AS (
    SELECT g.skill, q.quartile, count(*) AS n
    FROM offer_quartile q JOIN go_skills g USING (vacancy_id) GROUP BY ALL
), wide AS (
    SELECT c.skill,
           sum(c.n) AS offers,
           round(100.0 * sum(c.n) FILTER (WHERE c.quartile = 1) / any_value(d1.n), 1) AS q1_pct,
           round(100.0 * sum(c.n) FILTER (WHERE c.quartile = 4) / any_value(d4.n), 1) AS q4_pct
    FROM cnt c, (SELECT n FROM denom WHERE quartile = 1) d1, (SELECT n FROM denom WHERE quartile = 4) d4
    GROUP BY c.skill
)
SELECT skill, offers::BIGINT AS offers, coalesce(q1_pct, 0) AS q1_pct, coalesce(q4_pct, 0) AS q4_pct,
       round(coalesce(q4_pct, 0) - coalesce(q1_pct, 0), 1) AS q4_minus_q1
FROM wide
WHERE offers >= ${MIN_OFFERS}
ORDER BY q4_minus_q1 DESC, skill;

-- Как указана вилка: одна граница берётся как точка, и «до X» завышает оценку сильнее всего.
CREATE OR REPLACE TABLE salary_bounds AS
SELECT CASE WHEN o.salary_from IS NOT NULL AND o.salary_to IS NOT NULL THEN 'обе границы'
            WHEN o.salary_from IS NOT NULL THEN 'только «от»' ELSE 'только «до»' END AS bounds,
       count(*) AS offers, median(o.salary) AS median,
       round(100.0 * count(*) FILTER (WHERE q.quartile = 4)
             / (SELECT count(*) FROM offer_quartile WHERE quartile = 4), 1) AS share_of_q4_pct,
       count(*) FILTER (WHERE o.vacancy_id NOT IN (SELECT vacancy_id FROM go_skills)) AS without_skills
FROM go_offers o JOIN offer_quartile q USING (vacancy_id)
GROUP BY 1
ORDER BY 1;

-- Проверка на предложениях с обеими границами вилки: квартили внутри этой части и число навыков.
CREATE OR REPLACE TABLE salary_both_bounds AS
WITH b AS (
    SELECT vacancy_id, salary FROM go_offers WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL
), cut AS (
    SELECT quantile_cont(salary, 0.25) AS p25, median(salary) AS med, quantile_cont(salary, 0.75) AS p75 FROM b
), q AS (
    SELECT b.*, CASE WHEN salary <= p25 THEN 1 WHEN salary <= med THEN 2 WHEN salary <= p75 THEN 3 ELSE 4 END AS quartile
    FROM b, cut
), n AS (
    SELECT q.quartile, q.vacancy_id, q.salary, count(g.skill) AS skills
    FROM q LEFT JOIN go_skills g USING (vacancy_id)
    GROUP BY q.quartile, q.vacancy_id, q.salary
)
SELECT quartile, count(*) AS offers, median(salary) AS median,
       round(avg(skills) FILTER (WHERE skills > 0), 1) AS avg_skills, count(*) FILTER (WHERE skills = 0) AS without_skills
FROM n
GROUP BY quartile
ORDER BY quartile;
