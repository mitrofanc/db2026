BEGIN;

DO
$$
DECLARE
    v_ticket_valid   BIGINT;
    v_ticket_expired BIGINT;
    v_edition_ok_1   BIGINT;
    v_edition_ok_2   BIGINT;
    v_edition_zero   BIGINT;

    v_request_id     BIGINT;
    v_cnt            INT;
BEGIN
    SELECT t.ticket_id
    INTO v_ticket_valid
    FROM ticket t
    WHERE t.is_active = TRUE
      AND t.expire_date >= CURRENT_DATE
      AND (SELECT COUNT(*)
           FROM issue_doc d
                    JOIN issue_item i ON i.issue_doc_id = d.issue_doc_id
           WHERE d.ticket_id = t.ticket_id
             AND i.return_date IS NULL) < 10
      AND NOT EXISTS (SELECT 1
                      FROM issue_doc d
                               JOIN issue_item i ON i.issue_doc_id = d.issue_doc_id
                      WHERE d.ticket_id = t.ticket_id
                        AND i.return_date IS NULL
                        AND i.due_date < CURRENT_DATE)
    ORDER BY t.ticket_id
    LIMIT 1;

    SELECT t.ticket_id
    INTO v_ticket_expired
    FROM ticket t
    WHERE (t.is_active = FALSE OR t.expire_date < CURRENT_DATE)
    ORDER BY t.ticket_id
    LIMIT 1;

    SELECT e.edition_id
    INTO v_edition_ok_1
    FROM edition e
    WHERE e.current_count > 0
    ORDER BY e.current_count DESC, e.edition_id
    LIMIT 1;

    SELECT e.edition_id
    INTO v_edition_ok_2
    FROM edition e
    WHERE e.current_count > 0
      AND e.edition_id <> v_edition_ok_1
    ORDER BY e.current_count DESC, e.edition_id
    LIMIT 1;

    SELECT e.edition_id
    INTO v_edition_zero
    FROM edition e
    WHERE e.edition_id <> v_edition_ok_1
      AND e.edition_id <> v_edition_ok_2
    ORDER BY e.current_count, e.edition_id
    LIMIT 1;

    IF v_ticket_valid IS NULL OR v_ticket_expired IS NULL OR v_edition_ok_1 IS NULL OR v_edition_ok_2 IS NULL OR
       v_edition_zero IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для теста';
    END IF;

    -- Для негативного кейса гарантируем отсутствие экземпляров
    UPDATE edition
    SET current_count = 0
    WHERE edition_id = v_edition_zero;

    -- Positive #1: валидный билет + одна позиция
    INSERT INTO book_request (ticket_id, request_date, status)
    VALUES (v_ticket_valid, CURRENT_DATE, 'NEW')
    RETURNING request_id INTO v_request_id;

    INSERT INTO request_item (request_id, edition_id, qty)
    VALUES (v_request_id, v_edition_ok_1, 1);

    SELECT COUNT(*) INTO v_cnt FROM request_item WHERE request_id = v_request_id;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'Positive #1 failed: request_item_count=%, expected=1', v_cnt;
    END IF;

    SELECT COUNT(*)
    INTO v_cnt
    FROM refusal_doc rd
             JOIN refusal_item ri ON ri.refusal_doc_id = rd.refusal_doc_id
    WHERE rd.request_id = v_request_id;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'Positive #1 failed: refusal_item_count=%, expected=0', v_cnt;
    END IF;
    RAISE NOTICE 'PASS: Positive #1 (валидная одиночная вставка)';

    -- Positive #2: валидный билет + multi-row из двух валидных позиций
    INSERT INTO book_request (ticket_id, request_date, status)
    VALUES (v_ticket_valid, CURRENT_DATE, 'NEW')
    RETURNING request_id INTO v_request_id;

    INSERT INTO request_item (request_id, edition_id, qty)
    VALUES (v_request_id, v_edition_ok_1, 1),
           (v_request_id, v_edition_ok_2, 1);

    SELECT COUNT(*) INTO v_cnt FROM request_item WHERE request_id = v_request_id;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'Positive #2 failed: request_item_count=%, expected=2', v_cnt;
    END IF;

    SELECT COUNT(*)
    INTO v_cnt
    FROM refusal_doc rd
             JOIN refusal_item ri ON ri.refusal_doc_id = rd.refusal_doc_id
    WHERE rd.request_id = v_request_id;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'Positive #2 failed: refusal_item_count=%, expected=0', v_cnt;
    END IF;
    RAISE NOTICE 'PASS: Positive #2 (валидная multi-row вставка)';

    -- Negative #1: просроченный билет, обе позиции отклоняются
    INSERT INTO book_request (ticket_id, request_date, status)
    VALUES (v_ticket_expired, CURRENT_DATE, 'NEW')
    RETURNING request_id INTO v_request_id;

    INSERT INTO request_item (request_id, edition_id, qty)
    VALUES (v_request_id, v_edition_ok_1, 1),
           (v_request_id, v_edition_ok_2, 1);

    SELECT COUNT(*) INTO v_cnt FROM request_item WHERE request_id = v_request_id;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'Negative #1 failed: request_item_count=%, expected=0', v_cnt;
    END IF;

    SELECT COUNT(*)
    INTO v_cnt
    FROM refusal_doc rd
             JOIN refusal_item ri ON ri.refusal_doc_id = rd.refusal_doc_id
             JOIN refusal_reason rr ON rr.reason_id = ri.reason_id
    WHERE rd.request_id = v_request_id
      AND rr.reason_code = 'TICKET_EXPIRED';
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'Negative #1 failed: TICKET_EXPIRED refusals=%, expected=2', v_cnt;
    END IF;
    RAISE NOTICE 'PASS: Negative #1 (просроченный билет)';

    -- Negative #2: нет свободных экземпляров
    INSERT INTO book_request (ticket_id, request_date, status)
    VALUES (v_ticket_valid, CURRENT_DATE, 'NEW')
    RETURNING request_id INTO v_request_id;

    INSERT INTO request_item (request_id, edition_id, qty)
    VALUES (v_request_id, v_edition_zero, 1);

    SELECT COUNT(*) INTO v_cnt FROM request_item WHERE request_id = v_request_id;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'Negative #2 failed: request_item_count=%, expected=0', v_cnt;
    END IF;

    SELECT COUNT(*)
    INTO v_cnt
    FROM refusal_doc rd
             JOIN refusal_item ri ON ri.refusal_doc_id = rd.refusal_doc_id
             JOIN refusal_reason rr ON rr.reason_id = ri.reason_id
    WHERE rd.request_id = v_request_id
      AND rr.reason_code = 'NO_COPIES';
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'Negative #2 failed: NO_COPIES refusals=%, expected=1', v_cnt;
    END IF;
    RAISE NOTICE 'PASS: Negative #2 (нет свободных экземпляров)';
END;
$$;

ROLLBACK;