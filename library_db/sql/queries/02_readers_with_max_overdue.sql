-- читатели
WITH reader_base AS (
    SELECT u.user_id AS reader_id,
           CONCAT_WS(' ', u.last_name, u.first_name, u.middle_name) AS reader_name
    FROM library_user u
    JOIN role r ON r.role_id = u.role_id
    WHERE r.role_code = 4
),
ticket_stats AS (
    SELECT t.owner_user_id AS reader_id,
           COUNT(*) AS total_ticket_count,
           BOOL_OR(t.is_active AND t.expire_date >= CURRENT_DATE) AS has_active_ticket
    FROM ticket t
    GROUP BY t.owner_user_id
),
-- оставляет только текущий билет пользователя
current_ticket AS (
    SELECT ranked.reader_id,
           ranked.ticket_number,
           ranked.expire_date
    FROM (
        SELECT t.owner_user_id AS reader_id,
               t.ticket_number,
               t.expire_date,
               ROW_NUMBER() OVER (
                   PARTITION BY t.owner_user_id
                   ORDER BY t.issue_date DESC, t.ticket_id DESC
               ) AS rn
        FROM ticket t
        WHERE t.is_active
          AND t.expire_date >= CURRENT_DATE
    ) ranked
    WHERE ranked.rn = 1
),
-- был ли запрос с нарушением
request_flags AS (
    SELECT rd.request_id,
           BOOL_OR(rr.reason_code = 'RULES_VIOLATION') AS has_rule_violation
    FROM refusal_doc rd
    JOIN refusal_item ri ON ri.refusal_doc_id = rd.refusal_doc_id
    LEFT JOIN refusal_reason rr ON rr.reason_id = ri.reason_id
    GROUP BY rd.request_id
),
-- сколько обращений с нарушениями у читателя
request_stats AS (
    SELECT t.owner_user_id AS reader_id,
           COUNT(br.request_id) AS total_request_count,
           COUNT(br.request_id) FILTER (
               WHERE COALESCE(rf.has_rule_violation, FALSE)
           ) AS violation_request_count
    FROM ticket t
    LEFT JOIN book_request br ON br.ticket_id = t.ticket_id
    LEFT JOIN request_flags rf ON rf.request_id = br.request_id
    GROUP BY t.owner_user_id
),
-- сколько просроченный и текущая суммарная просрочка
overdue_stats AS (
    SELECT t.owner_user_id AS reader_id,
           COUNT(*) FILTER (
               WHERE ii.return_date IS NULL
                 AND ii.due_date < CURRENT_DATE
           ) AS current_overdue_book_count,
           COALESCE(
               SUM((CURRENT_DATE - ii.due_date)) FILTER (
                   WHERE ii.return_date IS NULL
                     AND ii.due_date < CURRENT_DATE
               ),
               0
           ) AS total_overdue_days
    FROM ticket t
    LEFT JOIN issue_doc id ON id.ticket_id = t.ticket_id
    LEFT JOIN issue_item ii ON ii.issue_doc_id = id.issue_doc_id
    GROUP BY t.owner_user_id
),
reader_report AS (
    SELECT rb.reader_name,
           COALESCE(ts.total_ticket_count, 0) AS total_ticket_count,
           COALESCE(ts.has_active_ticket, FALSE) AS has_active_ticket,
           ct.ticket_number AS current_ticket_number,
           ct.expire_date AS current_ticket_expire_date,
           COALESCE(rs.total_request_count, 0) AS total_request_count,
           COALESCE(rs.violation_request_count, 0) AS violation_request_count,
           COALESCE(os.current_overdue_book_count, 0) AS current_overdue_book_count,
           COALESCE(os.total_overdue_days, 0) AS total_overdue_days
    FROM reader_base rb
    LEFT JOIN ticket_stats ts ON ts.reader_id = rb.reader_id
    LEFT JOIN current_ticket ct ON ct.reader_id = rb.reader_id
    LEFT JOIN request_stats rs ON rs.reader_id = rb.reader_id
    LEFT JOIN overdue_stats os ON os.reader_id = rb.reader_id
)
SELECT rr.reader_name,
       rr.total_ticket_count,
       rr.has_active_ticket,
       rr.current_ticket_number,
       rr.current_ticket_expire_date,
       rr.total_request_count,
       rr.violation_request_count,
       rr.current_overdue_book_count
FROM reader_report rr
WHERE rr.total_overdue_days = (
          SELECT MAX(total_overdue_days)
          FROM reader_report
      )
  AND rr.total_overdue_days > 0
ORDER BY rr.current_overdue_book_count DESC, rr.reader_name;
