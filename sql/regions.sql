-- Этап 6. Сравнение трёх регионов. Нужны go_vacancies, go_skills (sql/go_segment.sql) и go_salary (sql/salary.sql).
-- ${REGIONS} — список (порядок, регион); последней строкой в сводке идёт весь сегмент Go для сравнения.

CREATE OR REPLACE TABLE region_list AS
SELECT * FROM (VALUES ${REGIONS}) AS t(ord, area);

-- Уникальные предложения с зарплатой внутри региона: копии одной вакансии в одном городе — одно.
CREATE OR REPLACE TABLE region_offers AS
SELECT * FROM go_salary
QUALIFY row_number() OVER (
    PARTITION BY area, employer_id, name, key_skills, salary_from, salary_to, currency, gross ORDER BY vacancy_id) = 1;

CREATE OR REPLACE TABLE region_summary AS
WITH scoped AS (
    SELECT r.ord, r.area AS scope, v.* EXCLUDE (area) FROM go_vacancies v JOIN region_list r USING (area)
    UNION ALL
    SELECT 99, 'Весь сегмент Go', v.* EXCLUDE (area) FROM go_vacancies v
), nsk AS (
    SELECT vacancy_id, count(*) AS skills FROM go_skills GROUP BY vacancy_id
), post AS (
    SELECT s.ord, s.scope,
           count(*) AS postings,
           round(100.0 * count(*) / (SELECT count(*) FROM go_vacancies), 1) AS share_pct,
           count(DISTINCT s.employer_id) AS employers,
           round(100.0 * avg((s.vacancy_id IN (SELECT vacancy_id FROM go_salary))::INT), 1) AS with_salary_pct,
           round(100.0 * avg((s.schedule = 'Удаленная работа')::INT), 1) AS remote_pct,
           round(100.0 * avg((s.experience = 'Нет опыта')::INT), 1) AS exp_none_pct,
           round(100.0 * avg((s.experience = 'От 1 года до 3 лет')::INT), 1) AS exp_1_3_pct,
           round(100.0 * avg((s.experience = 'От 3 до 6 лет')::INT), 1) AS exp_3_6_pct,
           round(100.0 * avg((s.experience = 'Более 6 лет')::INT), 1) AS exp_6_pct,
           round(100.0 * avg((nsk.vacancy_id IS NOT NULL)::INT), 1) AS with_skills_pct,
           round(avg(nsk.skills), 1) AS avg_skills
    FROM scoped s LEFT JOIN nsk USING (vacancy_id)
    GROUP BY s.ord, s.scope
), offers AS (
    SELECT r.ord, o.salary FROM region_offers o JOIN region_list r USING (area)
    UNION ALL
    SELECT 99, salary FROM go_offers
), sal AS (
    SELECT ord, count(*) AS salary_offers, quantile_cont(salary, 0.25) AS p25,
           median(salary) AS median, quantile_cont(salary, 0.75) AS p75
    FROM offers GROUP BY ord
)
SELECT post.*, sal.salary_offers, sal.p25, sal.median, sal.p75
FROM post JOIN sal USING (ord)
ORDER BY ord;

-- Навыки по регионам: доля вакансий с навыком среди вакансий региона с навыками и «лифт» к сегменту.
CREATE OR REPLACE TABLE region_skills AS
WITH base AS (
    SELECT r.ord, r.area, g.vacancy_id, g.skill
    FROM go_skills g JOIN go_vacancies v USING (vacancy_id) JOIN region_list r USING (area)
), denom AS (
    SELECT ord, count(DISTINCT vacancy_id) AS n FROM base GROUP BY ord
), seg AS (
    SELECT skill, count(*) / (SELECT count(DISTINCT vacancy_id) FROM go_skills) AS seg_share
    FROM go_skills GROUP BY skill
), cnt AS (
    SELECT b.ord, b.area, b.skill, count(*) AS postings, count(DISTINCT v.employer_id) AS employers
    FROM base b JOIN go_vacancies v USING (vacancy_id)
    GROUP BY b.ord, b.area, b.skill
)
SELECT cnt.ord, cnt.area, cnt.skill, cnt.postings, cnt.employers,
       round(100.0 * cnt.postings / denom.n, 1) AS share_pct,
       round(100.0 * seg.seg_share, 1) AS segment_share_pct,
       round(cnt.postings / denom.n / seg.seg_share, 2) AS lift,
       row_number() OVER (PARTITION BY cnt.ord ORDER BY cnt.postings DESC, cnt.skill) AS rnk
FROM cnt JOIN denom USING (ord) JOIN seg USING (skill)
ORDER BY cnt.ord, rnk;

-- Топ-5 навыков, пар и троек в каждом регионе (без самого go: он почти в каждой вакансии).
CREATE OR REPLACE TABLE region_top_combos AS
WITH sk AS (
    SELECT r.ord, r.area, g.vacancy_id, g.skill
    FROM go_skills g JOIN go_vacancies v USING (vacancy_id) JOIN region_list r USING (area)
    WHERE g.skill <> 'go'
), combos AS (
    SELECT ord, area, 1 AS k, skill AS combo, count(*) AS vacancies FROM sk GROUP BY ALL
    UNION ALL
    SELECT a.ord, a.area, 2, a.skill || ' + ' || b.skill, count(*)
    FROM sk a JOIN sk b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill GROUP BY ALL
    UNION ALL
    SELECT a.ord, a.area, 3, a.skill || ' + ' || b.skill || ' + ' || c.skill, count(*)
    FROM sk a
    JOIN sk b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill
    JOIN sk c ON c.vacancy_id = a.vacancy_id AND b.skill < c.skill
    GROUP BY ALL
)
SELECT *, row_number() OVER (PARTITION BY ord, k ORDER BY vacancies DESC, combo) AS rnk
FROM combos
QUALIFY rnk <= 5
ORDER BY ord, k, rnk;

CREATE OR REPLACE TABLE region_salary_by_experience AS
SELECT r.ord, r.area, o.experience, count(*) AS offers, median(o.salary) AS median
FROM region_offers o JOIN region_list r USING (area)
GROUP BY r.ord, r.area, o.experience
ORDER BY r.ord, CASE o.experience WHEN 'Нет опыта' THEN 0 WHEN 'От 1 года до 3 лет' THEN 1
                                  WHEN 'От 3 до 6 лет' THEN 2 ELSE 3 END;
