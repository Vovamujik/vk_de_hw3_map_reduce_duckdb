-- Наивный вариант для бонусного задания: три независимых запроса, каждый сам читает весь объект
-- из RustFS с автоопределением схемы и заново строит сегмент Go. Правила те же, что в go_segment.sql.

COPY (
    WITH go AS (
        SELECT * FROM ${SOURCE}
        WHERE regexp_matches(name, '(^|[^A-Za-z0-9_])(?i:go|golang)([^A-Za-z0-9_]|$)')
    ), split AS (
        SELECT vacancy_id, lower(trim(raw_skill, ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13)
                                          || chr(160) || chr(8203) || chr(65279))) AS skill
        FROM go, unnest(string_split(key_skills, ';')) AS t(raw_skill)
    ), sk AS (
        SELECT DISTINCT s.vacancy_id, coalesce(a.canonical, s.skill) AS skill
        FROM split s LEFT JOIN read_csv('${ALIASES}', header = true, all_varchar = true) a ON a.alias = s.skill
        WHERE s.skill <> ''
    )
    SELECT skill, count(*) AS vacancies FROM sk GROUP BY skill ORDER BY vacancies DESC, skill
) TO '${OUT}/skills.csv' (HEADER);

COPY (
    WITH go AS (
        SELECT * FROM ${SOURCE}
        WHERE regexp_matches(name, '(^|[^A-Za-z0-9_])(?i:go|golang)([^A-Za-z0-9_]|$)')
    ), split AS (
        SELECT vacancy_id, lower(trim(raw_skill, ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13)
                                          || chr(160) || chr(8203) || chr(65279))) AS skill
        FROM go, unnest(string_split(key_skills, ';')) AS t(raw_skill)
    ), sk AS (
        SELECT DISTINCT s.vacancy_id, coalesce(a.canonical, s.skill) AS skill
        FROM split s LEFT JOIN read_csv('${ALIASES}', header = true, all_varchar = true) a ON a.alias = s.skill
        WHERE s.skill <> ''
    )
    SELECT a.skill AS skill_1, b.skill AS skill_2, count(*) AS vacancies
    FROM sk a JOIN sk b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill
    GROUP BY ALL ORDER BY vacancies DESC, skill_1, skill_2
) TO '${OUT}/pairs.csv' (HEADER);

COPY (
    WITH go AS (
        SELECT * FROM ${SOURCE}
        WHERE regexp_matches(name, '(^|[^A-Za-z0-9_])(?i:go|golang)([^A-Za-z0-9_]|$)')
    ), split AS (
        SELECT vacancy_id, lower(trim(raw_skill, ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13)
                                          || chr(160) || chr(8203) || chr(65279))) AS skill
        FROM go, unnest(string_split(key_skills, ';')) AS t(raw_skill)
    ), sk AS (
        SELECT DISTINCT s.vacancy_id, coalesce(a.canonical, s.skill) AS skill
        FROM split s LEFT JOIN read_csv('${ALIASES}', header = true, all_varchar = true) a ON a.alias = s.skill
        WHERE s.skill <> ''
    )
    SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3, count(*) AS vacancies
    FROM sk a
    JOIN sk b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill
    JOIN sk c ON c.vacancy_id = a.vacancy_id AND b.skill < c.skill
    GROUP BY ALL ORDER BY vacancies DESC, skill_1, skill_2, skill_3
) TO '${OUT}/triples.csv' (HEADER);
