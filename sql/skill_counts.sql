-- Частоты навыков, пар и троек по готовой таблице go_skills (sql/go_segment.sql).
-- Частота = число Go-вакансий, где встречается навык или комбинация; навыки в комбинации по алфавиту.

COPY (
    SELECT skill, count(*) AS vacancies
    FROM go_skills
    GROUP BY skill
    ORDER BY vacancies DESC, skill
) TO '${OUT}/skills.csv' (HEADER);

COPY (
    SELECT a.skill AS skill_1, b.skill AS skill_2, count(*) AS vacancies
    FROM go_skills a
    JOIN go_skills b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill
    GROUP BY ALL
    ORDER BY vacancies DESC, skill_1, skill_2
) TO '${OUT}/pairs.csv' (HEADER);

COPY (
    SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3, count(*) AS vacancies
    FROM go_skills a
    JOIN go_skills b ON b.vacancy_id = a.vacancy_id AND a.skill < b.skill
    JOIN go_skills c ON c.vacancy_id = a.vacancy_id AND b.skill < c.skill
    GROUP BY ALL
    ORDER BY vacancies DESC, skill_1, skill_2, skill_3
) TO '${OUT}/triples.csv' (HEADER);

-- Контрольные числа для сверки с MapReduce.
COPY (
    SELECT 'go_vacancies' AS metric, count(*) AS value FROM go_vacancies
    UNION ALL SELECT 'skill_mentions', count(*) FROM go_skills
    UNION ALL SELECT 'with_skills', count(DISTINCT vacancy_id) FROM go_skills
    ORDER BY metric
) TO '${OUT}/control.csv' (HEADER);
