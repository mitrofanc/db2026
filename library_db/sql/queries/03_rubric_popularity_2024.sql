-- начало месяца, название
WITH month_list AS (
    SELECT gs::date AS month_start,
           (ARRAY[
               'Январь', 'Февраль', 'Март', 'Апрель',
               'Май', 'Июнь', 'Июль', 'Август',
               'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
           ])[EXTRACT(MONTH FROM gs)::INT] AS month_name
    FROM generate_series(
        DATE '2024-01-01',
        DATE '2024-12-01',
        INTERVAL '1 month'
    ) AS gs
),
-- одна книга в одной рубрике в одном месяце
request_facts AS (
    SELECT DATE_TRUNC('month', br.request_date)::date AS month_start,
           e.rubric_id,
           r.rubric_name,
           e.edition_id,
           e.title,
           SUM(ri.qty) AS request_qty,
           MAX(br.request_date) AS last_request_date
    FROM book_request br
    JOIN request_item ri ON ri.request_id = br.request_id
    JOIN edition e ON e.edition_id = ri.edition_id
    JOIN rubric r ON r.rubric_id = e.rubric_id
    WHERE br.request_date >= DATE '2024-01-01'
      AND br.request_date < DATE '2025-01-01'
    GROUP BY DATE_TRUNC('month', br.request_date)::date,
             e.rubric_id,
             r.rubric_name,
             e.edition_id,
             e.title
),
-- запросы по рубрике за месяц
rubric_month_stats AS (
    SELECT rf.month_start,
           rf.rubric_id,
           rf.rubric_name,
           SUM(rf.request_qty) AS rubric_request_count_in_month
    FROM request_facts rf
    GROUP BY rf.month_start, rf.rubric_id, rf.rubric_name
),
-- запросы по рубрике за год
rubric_year_stats AS (
    SELECT rf.rubric_id,
           SUM(rf.request_qty) AS rubric_request_count_in_2024
    FROM request_facts rf
    GROUP BY rf.rubric_id
),
-- самая популярная рубрика за месяц
top_rubric_per_month AS (
    SELECT ranked.month_start,
           ranked.rubric_id,
           ranked.rubric_name,
           ranked.rubric_request_count_in_month
    FROM (
        SELECT rms.month_start,
               rms.rubric_id,
               rms.rubric_name,
               rms.rubric_request_count_in_month,
               ROW_NUMBER() OVER (
                   PARTITION BY rms.month_start
                   ORDER BY rms.rubric_request_count_in_month DESC,
                            rms.rubric_name,
                            rms.rubric_id
               ) AS rn
        FROM rubric_month_stats rms
    ) ranked
    WHERE ranked.rn = 1
),
-- самая популярная книга рубрики в конкретном месяце
top_book_per_rubric_month AS (
    SELECT ranked.month_start,
           ranked.rubric_id,
           ranked.title,
           ranked.book_request_count_in_month,
           ranked.last_request_date_in_month
    FROM (
--      одна книга в одной рубрике в одном месяце
        SELECT rf.month_start,
               rf.rubric_id,
               rf.edition_id,
               rf.title,
               SUM(rf.request_qty) AS book_request_count_in_month,
               MAX(rf.last_request_date) AS last_request_date_in_month,
               ROW_NUMBER() OVER (
                   PARTITION BY rf.month_start, rf.rubric_id
                   ORDER BY SUM(rf.request_qty) DESC,
                            MAX(rf.last_request_date) DESC,
                            rf.title,
                            rf.edition_id
               ) AS rn
        FROM request_facts rf
        GROUP BY rf.month_start, rf.rubric_id, rf.edition_id, rf.title
    ) ranked
    WHERE ranked.rn = 1
)
SELECT ml.month_name,
       tr.rubric_name AS most_popular_rubric,
       tb.title AS most_popular_book_in_rubric,
       COALESCE(tr.rubric_request_count_in_month, 0) AS rubric_request_count_in_month,
       COALESCE(rys.rubric_request_count_in_2024, 0) AS rubric_request_count_in_2024,
       COALESCE(tb.book_request_count_in_month, 0) AS book_request_count_in_month,
       tb.last_request_date_in_month
FROM month_list ml
LEFT JOIN top_rubric_per_month tr
       ON tr.month_start = ml.month_start
LEFT JOIN rubric_year_stats rys
       ON rys.rubric_id = tr.rubric_id
LEFT JOIN top_book_per_rubric_month tb
       ON tb.month_start = ml.month_start
      AND tb.rubric_id = tr.rubric_id
ORDER BY ml.month_start;
