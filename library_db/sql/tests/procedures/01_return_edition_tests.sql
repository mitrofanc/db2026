BEGIN;

-- возврат просроченного издания
SAVEPOINT sp_return_pos_1;
DO
$$
DECLARE
    v_issue_doc_id BIGINT;
    v_edition_id BIGINT;
    v_ticket_number VARCHAR(20);
    v_current_before INT;
    v_current_after INT;
    v_open_before INT;
    v_return_date DATE;
    v_all_items_returned BOOLEAN;
    v_expected_all_items_returned BOOLEAN;
BEGIN
    SELECT ii.issue_doc_id,
           ii.edition_id,
           t.ticket_number,
           e.current_count
    INTO v_issue_doc_id,
         v_edition_id,
         v_ticket_number,
         v_current_before
    FROM issue_item ii
             JOIN issue_doc id ON id.issue_doc_id = ii.issue_doc_id
             JOIN ticket t ON t.ticket_id = id.ticket_id
             JOIN edition e ON e.edition_id = ii.edition_id
    WHERE ii.return_date IS NULL
      AND ii.due_date < CURRENT_DATE
      AND e.current_count < e.total_count
    ORDER BY id.issue_date DESC,
             id.issue_doc_id DESC,
             ii.edition_id
    LIMIT 1;

    IF v_issue_doc_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для positive #1 return_edition';
    END IF;

    SELECT COUNT(*)
    INTO v_open_before
    FROM issue_item i
    WHERE i.issue_doc_id = v_issue_doc_id
      AND i.return_date IS NULL;

    CALL return_edition(v_edition_id, v_ticket_number);

    SELECT i.return_date
    INTO v_return_date
    FROM issue_item i
    WHERE i.issue_doc_id = v_issue_doc_id
      AND i.edition_id = v_edition_id;

    IF v_return_date IS DISTINCT FROM CURRENT_DATE THEN
        RAISE EXCEPTION 'Positive #1 failed: return_date=%, expected=%', v_return_date, CURRENT_DATE;
    END IF;

    SELECT e.current_count
    INTO v_current_after
    FROM edition e
    WHERE e.edition_id = v_edition_id;

    IF v_current_after <> v_current_before + 1 THEN
        RAISE EXCEPTION 'Positive #1 failed: current_count_after=%, expected=%',
            v_current_after,
            v_current_before + 1;
    END IF;

    SELECT d.all_items_returned
    INTO v_all_items_returned
    FROM issue_doc d
    WHERE d.issue_doc_id = v_issue_doc_id;

    v_expected_all_items_returned := (v_open_before = 1);

    IF v_all_items_returned IS DISTINCT FROM v_expected_all_items_returned THEN
        RAISE EXCEPTION 'Positive #1 failed: all_items_returned=%, expected=%',
            v_all_items_returned,
            v_expected_all_items_returned;
    END IF;

    RAISE NOTICE 'PASS: Positive #1 (успешный возврат просроченного издания)';
END;
$$;
ROLLBACK TO SAVEPOINT sp_return_pos_1;

-- возврат без просрочки
SAVEPOINT sp_return_pos_2;
DO
$$
DECLARE
    v_issue_doc_id BIGINT;
    v_edition_id BIGINT;
    v_ticket_number VARCHAR(20);
    v_current_before INT;
    v_current_after INT;
    v_return_date DATE;
BEGIN
    SELECT ii.issue_doc_id,
           ii.edition_id,
           t.ticket_number,
           e.current_count
    INTO v_issue_doc_id,
         v_edition_id,
         v_ticket_number,
         v_current_before
    FROM issue_item ii
             JOIN issue_doc id ON id.issue_doc_id = ii.issue_doc_id
             JOIN ticket t ON t.ticket_id = id.ticket_id
             JOIN edition e ON e.edition_id = ii.edition_id
    WHERE ii.return_date IS NULL
      AND e.current_count < e.total_count
    ORDER BY id.issue_date DESC,
             id.issue_doc_id DESC,
             ii.edition_id
    LIMIT 1;

    IF v_issue_doc_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для positive #2 return_edition';
    END IF;

    UPDATE issue_item ii
    SET due_date = CURRENT_DATE + 5
    FROM issue_doc id
             JOIN ticket t ON t.ticket_id = id.ticket_id
    WHERE ii.issue_doc_id = id.issue_doc_id
      AND ii.edition_id = v_edition_id
      AND ii.return_date IS NULL
      AND t.ticket_number = v_ticket_number;

    CALL return_edition(v_edition_id, v_ticket_number);

    SELECT i.return_date
    INTO v_return_date
    FROM issue_item i
    WHERE i.issue_doc_id = v_issue_doc_id
      AND i.edition_id = v_edition_id;

    IF v_return_date IS DISTINCT FROM CURRENT_DATE THEN
        RAISE EXCEPTION 'Positive #2 failed: return_date=%, expected=%', v_return_date, CURRENT_DATE;
    END IF;

    SELECT e.current_count
    INTO v_current_after
    FROM edition e
    WHERE e.edition_id = v_edition_id;

    IF v_current_after <> v_current_before + 1 THEN
        RAISE EXCEPTION 'Positive #2 failed: current_count_after=%, expected=%',
            v_current_after,
            v_current_before + 1;
    END IF;

    RAISE NOTICE 'PASS: Positive #2 (успешный возврат без просрочки)';
END;
$$;
ROLLBACK TO SAVEPOINT sp_return_pos_2;

-- несуществующий билет
SAVEPOINT sp_return_neg_1;
DO
$$
DECLARE
    v_issue_doc_id BIGINT;
    v_edition_id BIGINT;
    v_current_before INT;
    v_current_after INT;
    v_return_date DATE;
    v_failed BOOLEAN;
BEGIN
    SELECT ii.issue_doc_id,
           ii.edition_id,
           e.current_count
    INTO v_issue_doc_id,
         v_edition_id,
         v_current_before
    FROM issue_item ii
             JOIN edition e ON e.edition_id = ii.edition_id
    WHERE ii.return_date IS NULL
      AND e.current_count < e.total_count
    ORDER BY ii.issue_doc_id DESC,
             ii.edition_id
    LIMIT 1;

    IF v_issue_doc_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для negative #1 return_edition';
    END IF;

    v_failed := FALSE;
    BEGIN
        CALL return_edition(v_edition_id, '123456789098765432134567890-098765434567890');
    EXCEPTION
        WHEN OTHERS THEN
            v_failed := TRUE;
            RAISE NOTICE 'PASS: Negative #1 (несуществующий билет): %', SQLERRM;
    END;

    IF NOT v_failed THEN
        RAISE EXCEPTION 'Negative #1 failed: ожидалась ошибка для несуществующего билета';
    END IF;

    SELECT i.return_date
    INTO v_return_date
    FROM issue_item i
    WHERE i.issue_doc_id = v_issue_doc_id
      AND i.edition_id = v_edition_id;

    IF v_return_date IS NOT NULL THEN
        RAISE EXCEPTION 'Negative #1 failed: return_date unexpectedly changed to %', v_return_date;
    END IF;

    SELECT e.current_count
    INTO v_current_after
    FROM edition e
    WHERE e.edition_id = v_edition_id;

    IF v_current_after <> v_current_before THEN
        RAISE EXCEPTION 'Negative #1 failed: current_count changed from % to %',
            v_current_before,
            v_current_after;
    END IF;
END;
$$;
ROLLBACK TO SAVEPOINT sp_return_neg_1;


-- повторный возврат уже возвращенного издания
SAVEPOINT sp_return_neg_2;
DO
$$
DECLARE
    v_issue_doc_id BIGINT;
    v_edition_id BIGINT;
    v_ticket_number VARCHAR(20);
    v_old_return_date DATE;
    v_new_return_date DATE;
    v_current_before INT;
    v_current_after INT;
    v_failed BOOLEAN;
BEGIN
    SELECT ii.issue_doc_id,
           ii.edition_id,
           t.ticket_number,
           ii.return_date,
           e.current_count
    INTO v_issue_doc_id,
         v_edition_id,
         v_ticket_number,
         v_old_return_date,
         v_current_before
    FROM issue_item ii
             JOIN issue_doc id ON id.issue_doc_id = ii.issue_doc_id
             JOIN ticket t ON t.ticket_id = id.ticket_id
             JOIN edition e ON e.edition_id = ii.edition_id
    WHERE ii.return_date IS NOT NULL
    ORDER BY ii.return_date DESC,
             ii.issue_doc_id DESC,
             ii.edition_id
    LIMIT 1;

    IF v_issue_doc_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для negative #2 return_edition';
    END IF;

    v_failed := FALSE;
    BEGIN
        CALL return_edition(v_edition_id, v_ticket_number);
    EXCEPTION
        WHEN OTHERS THEN
            v_failed := TRUE;
            RAISE NOTICE 'PASS: Negative #2 (повторный возврат): %', SQLERRM;
    END;

    IF NOT v_failed THEN
        RAISE EXCEPTION 'Negative #2 failed: ожидалась ошибка для уже возвращенного издания';
    END IF;

    SELECT ii.return_date
    INTO v_new_return_date
    FROM issue_item ii
    WHERE ii.issue_doc_id = v_issue_doc_id
      AND ii.edition_id = v_edition_id;

    IF v_new_return_date IS DISTINCT FROM v_old_return_date THEN
        RAISE EXCEPTION 'Negative #2 failed: return_date changed from % to %',
            v_old_return_date,
            v_new_return_date;
    END IF;

    SELECT e.current_count
    INTO v_current_after
    FROM edition e
    WHERE e.edition_id = v_edition_id;

    IF v_current_after <> v_current_before THEN
        RAISE EXCEPTION 'Negative #2 failed: current_count changed from % to %',
            v_current_before,
            v_current_after;
    END IF;
END;
$$;
ROLLBACK TO SAVEPOINT sp_return_neg_2;

ROLLBACK;
\echo 'OK: return_edition tests passed'
