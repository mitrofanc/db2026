BEGIN;

DO $$
    DECLARE
        cols text;
        sql  text;
    BEGIN
        SELECT
            STRING_AGG(
                    FORMAT(
                            'COALESCE(MAX(CASE WHEN ap.publisher = %L THEN ap.titles END), ''-'') AS %I',
                            p.publisher_name || ' (' || p.city || ')',
                            p.publisher_name || ' (' || p.city || ')'
                    ),
                    ', ' ORDER BY p.publisher_name, p.city, p.publisher_id
            )
        INTO cols
        FROM publisher p;

        sql := FORMAT($q$
        WITH ap AS (
            SELECT ea.author_id,
                   p.publisher_name || ' (' || p.city || ')' AS publisher,
                   STRING_AGG(DISTINCT e.title, ', ' ORDER BY e.title) AS titles
            FROM edition_author ea
            JOIN edition e ON e.edition_id = ea.edition_id
            JOIN publisher p ON p.publisher_id = e.publisher_id
            GROUP BY ea.author_id, p.publisher_name, p.city, p.publisher_id
        )
        SELECT CONCAT_WS(' ', a.last_name, a.first_name, a.middle_name) AS author_name,
               %s
        FROM author a
        LEFT JOIN ap ON ap.author_id = a.author_id
        GROUP BY a.author_id, a.last_name, a.first_name, a.middle_name
        ORDER BY author_name
    $q$, cols);

        EXECUTE 'DECLARE cur CURSOR FOR ' || sql;
    END $$;

FETCH ALL FROM cur;

COMMIT;
