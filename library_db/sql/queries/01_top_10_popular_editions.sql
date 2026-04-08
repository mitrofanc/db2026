-- id издания, автор
WITH edition_authors AS (
    SELECT ea.edition_id,
           STRING_AGG(
               CONCAT_WS(' ', a.last_name, a.first_name, a.middle_name),
               ', '
               ORDER BY ea.author_order, a.author_id
           ) AS author
    FROM edition_author ea
    JOIN author a ON a.author_id = ea.author_id
    GROUP BY ea.edition_id
),
-- статистика по каждому изданию
edition_issue_stats AS (
    SELECT ii.edition_id,
           COUNT(*) AS total_issue_count,
           COUNT(*) FILTER (
               WHERE id.issue_date >= CURRENT_DATE - INTERVAL '14 days'
           ) AS issue_count_last_2_weeks,
           COUNT(DISTINCT t.owner_user_id) FILTER (
               WHERE ii.return_date IS NULL
           ) AS active_reader_count
    FROM issue_item ii
    JOIN issue_doc id ON id.issue_doc_id = ii.issue_doc_id
    JOIN ticket t ON t.ticket_id = id.ticket_id
    GROUP BY ii.edition_id
),
-- количество отказов у издательства
publisher_refusal_stats AS (
    SELECT e.publisher_id,
           COUNT(*) AS publisher_refusal_count
    FROM refusal_item ri
    JOIN edition e ON e.edition_id = ri.edition_id
    GROUP BY e.publisher_id
),
-- наиболее частая причина отказа у издательства
publisher_top_reason AS (
    SELECT ranked.publisher_id,
           ranked.reason_name
    FROM (
        SELECT e.publisher_id,
               COALESCE(rr.reason_name, ri.reason_text, 'Не указана') AS reason_name,
               COUNT(*) AS reason_count,
               ROW_NUMBER() OVER (
                   PARTITION BY e.publisher_id
                   ORDER BY COUNT(*) DESC,
                            COALESCE(rr.reason_name, ri.reason_text, 'Не указана')
               ) AS rn
        FROM refusal_item ri
        JOIN edition e ON e.edition_id = ri.edition_id
        LEFT JOIN refusal_reason rr ON rr.reason_id = ri.reason_id
        GROUP BY e.publisher_id, COALESCE(rr.reason_name, ri.reason_text, 'Не указана')
    ) ranked
    WHERE ranked.rn = 1
),
-- последний читатель для издания
last_reader AS (
    SELECT ranked.edition_id,
           ranked.ticket_number
    FROM (
        SELECT ii.edition_id,
               t.ticket_number,
               ROW_NUMBER() OVER (
                   PARTITION BY ii.edition_id
                   ORDER BY id.issue_date DESC, id.issue_doc_id DESC
               ) AS rn
        FROM issue_item ii
        JOIN issue_doc id ON id.issue_doc_id = ii.issue_doc_id
        JOIN ticket t ON t.ticket_id = id.ticket_id
    ) ranked
    WHERE ranked.rn = 1
)
SELECT e.title,
       COALESCE(ea.author, 'Автор не указан') AS author,
       r.rubric_name AS rubric,
       COALESCE(eis.total_issue_count, 0) AS total_issue_count,
       COALESCE(eis.active_reader_count, 0) AS active_reader_count,
       COALESCE(prs.publisher_refusal_count, 0) AS publisher_refusal_count,
       COALESCE(ptr.reason_name, 'Нет отказов') AS top_refusal_reason,
       COALESCE(eis.issue_count_last_2_weeks, 0) AS issue_count_last_2_weeks,
       lr.ticket_number AS last_reader_ticket_number
FROM edition e
LEFT JOIN edition_authors ea ON ea.edition_id = e.edition_id
LEFT JOIN rubric r ON r.rubric_id = e.rubric_id
LEFT JOIN edition_issue_stats eis ON eis.edition_id = e.edition_id
LEFT JOIN publisher_refusal_stats prs ON prs.publisher_id = e.publisher_id
LEFT JOIN publisher_top_reason ptr ON ptr.publisher_id = e.publisher_id
LEFT JOIN last_reader lr ON lr.edition_id = e.edition_id
ORDER BY total_issue_count DESC, e.title, e.edition_id
LIMIT 10;
