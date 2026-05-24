CREATE INDEX IF NOT EXISTS idx_q01_edition_author_order_author
ON edition_author (edition_id, author_order, author_id);
