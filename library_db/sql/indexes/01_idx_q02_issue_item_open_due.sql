CREATE INDEX IF NOT EXISTS idx_q02_issue_item_open_due
ON issue_item (due_date, issue_doc_id)
WHERE return_date IS NULL;
