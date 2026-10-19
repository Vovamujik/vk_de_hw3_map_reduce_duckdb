-- Сегмент Go: те же правила, что в hw/normalize.py.
-- ${SOURCE} — табличная функция над объектом в RustFS (CSV или Parquet), ${ALIASES} — словарь синонимов.

CREATE OR REPLACE TABLE skill_aliases AS
SELECT alias, canonical
FROM read_csv('${ALIASES}', header = true, all_varchar = true);

-- Одно чтение объекта: в название входит отдельное слово Go или Golang (ASCII-границы, регистр не важен).
CREATE OR REPLACE TABLE go_vacancies AS
SELECT vacancy_id, employer_id, name, role, area, experience, schedule, employment,
       salary_from, salary_to, currency, gross, key_skills
FROM ${SOURCE}
WHERE regexp_matches(name, '(^|[^A-Za-z0-9_])(?i:go|golang)([^A-Za-z0-9_]|$)');

-- Одна строка на пару (вакансия, навык): края очищены, нижний регистр, синонимы склеены, без повторов.
CREATE OR REPLACE TABLE go_skills AS
WITH split AS (
    SELECT vacancy_id,
           lower(trim(raw_skill, ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13)
                                 || chr(160) || chr(8203) || chr(65279))) AS skill
    FROM go_vacancies, unnest(string_split(key_skills, ';')) AS t(raw_skill)
)
SELECT DISTINCT s.vacancy_id, coalesce(a.canonical, s.skill) AS skill
FROM split s
LEFT JOIN skill_aliases a ON a.alias = s.skill
WHERE s.skill <> '';
