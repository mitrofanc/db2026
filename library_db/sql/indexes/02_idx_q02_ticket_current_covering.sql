CREATE INDEX IF NOT EXISTS idx_q02_ticket_current_covering
ON ticket (owner_user_id, issue_date DESC, ticket_id DESC)
INCLUDE (ticket_number, expire_date)
WHERE is_active;
