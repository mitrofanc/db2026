-- список читателей
WITH reader_base AS (
    SELECT u.user_id AS reader_id,
           CONCAT_WS(' ', u.last_name, u.first_name, u.middle_name) AS reader_name
    FROM library_user u
    JOIN role r ON r.role_id = u.role_id
    WHERE r.role_code = 4
),
-- читатели с 2 и более билетами
ticket_stats AS (
    SELECT t.owner_user_id AS reader_id,
           COUNT(*) AS total_ticket_count,
           MIN(t.issue_date) AS first_ticket_issue_date,
           MAX(t.expire_date) AS last_ticket_expire_date,
           BOOL_OR(t.is_active AND t.expire_date >= CURRENT_DATE) AS has_active_ticket
    FROM ticket t
    GROUP BY t.owner_user_id
    HAVING COUNT(*) > 1
),
-- количество изданий, которые заказал читатель по всем билетам
order_stats AS (
    SELECT t.owner_user_id AS reader_id,
           COALESCE(SUM(ri.qty), 0) AS total_ordered_edition_count
    FROM ticket t
    LEFT JOIN book_request br ON br.ticket_id = t.ticket_id
    LEFT JOIN request_item ri ON ri.request_id = br.request_id
    GROUP BY t.owner_user_id
),
-- количество просроченных изданий по всем билетам читателя
overdue_stats AS (
    SELECT t.owner_user_id AS reader_id,
           COUNT(*) FILTER (
               WHERE (ii.return_date IS NULL AND ii.due_date < CURRENT_DATE)
                  OR (ii.return_date IS NOT NULL AND ii.return_date > ii.due_date)
           ) AS overdue_edition_count
    FROM ticket t
    LEFT JOIN issue_doc id ON id.ticket_id = t.ticket_id
    LEFT JOIN issue_item ii ON ii.issue_doc_id = id.issue_doc_id
    GROUP BY t.owner_user_id
),
-- самое популярное издание у читателя
top_edition AS (
    SELECT ranked.reader_id,
           ranked.title
    FROM (
        SELECT t.owner_user_id AS reader_id,
               e.edition_id,
               e.title,
               SUM(ri.qty) AS ordered_qty,
               ROW_NUMBER() OVER (
                   PARTITION BY t.owner_user_id
                   ORDER BY SUM(ri.qty) DESC, e.title, e.edition_id
               ) AS rn
        FROM ticket t
        JOIN book_request br ON br.ticket_id = t.ticket_id
        JOIN request_item ri ON ri.request_id = br.request_id
        JOIN edition e ON e.edition_id = ri.edition_id
        GROUP BY t.owner_user_id, e.edition_id, e.title
    ) ranked
    WHERE ranked.rn = 1
)
SELECT rb.reader_name,
       ts.total_ticket_count,
       ts.first_ticket_issue_date,
       ts.last_ticket_expire_date,
       COALESCE(os.total_ordered_edition_count, 0) AS total_ordered_edition_count,
       COALESCE(ovs.overdue_edition_count, 0) AS overdue_edition_count,
       te.title AS most_popular_edition,
       ts.has_active_ticket
FROM ticket_stats ts
JOIN reader_base rb ON rb.reader_id = ts.reader_id
LEFT JOIN order_stats os ON os.reader_id = ts.reader_id
LEFT JOIN overdue_stats ovs ON ovs.reader_id = ts.reader_id
LEFT JOIN top_edition te ON te.reader_id = ts.reader_id
ORDER BY ts.total_ticket_count DESC, rb.reader_name;
