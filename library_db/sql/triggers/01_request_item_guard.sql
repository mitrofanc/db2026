CREATE OR REPLACE FUNCTION trg_request_item_guard_fn()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_ticket_id        BIGINT;
    v_operator_user_id BIGINT;
    v_expire_date      DATE;
    v_is_active        BOOLEAN;
    v_active_loans     BIGINT;
    v_has_overdue      BOOLEAN;
    v_current_count    INT;
    v_reason_code      VARCHAR(20);
    v_reason_text      VARCHAR(500);
    v_reason_id        BIGINT;
    v_refusal_doc_id   BIGINT;
BEGIN
    SELECT br.ticket_id, t.operator_user_id, t.expire_date, t.is_active
    INTO v_ticket_id, v_operator_user_id, v_expire_date, v_is_active
    FROM book_request br
             JOIN ticket t ON t.ticket_id = br.ticket_id
    WHERE br.request_id = NEW.request_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Не найден request/ticket для request_id=%', NEW.request_id;
    END IF;

--     подсчет невозвразенных
    SELECT COUNT(*)
    INTO v_active_loans
    FROM issue_doc d
             JOIN issue_item i ON i.issue_doc_id = d.issue_doc_id
    WHERE d.ticket_id = v_ticket_id
      AND i.return_date IS NULL;

--     просроченные книги
    SELECT EXISTS (SELECT 1
                   FROM issue_doc d
                            JOIN issue_item i ON i.issue_doc_id = d.issue_doc_id
                   WHERE d.ticket_id = v_ticket_id
                     AND i.return_date IS NULL
                     AND i.due_date < CURRENT_DATE)
    INTO v_has_overdue;

--     количество свободных экземпляров
    SELECT e.current_count
    INTO v_current_count
    FROM edition e
    WHERE e.edition_id = NEW.edition_id;

    v_reason_code := NULL;
    v_reason_text := NULL;

    IF (NOT v_is_active) OR v_expire_date < CURRENT_DATE THEN
        v_reason_code := 'TICKET_EXPIRED';
        v_reason_text := 'Истек срок действия читательского билета';
    ELSIF v_active_loans >= 10 THEN
        v_reason_code := 'RULES_VIOLATION';
        v_reason_text := 'Превышен лимит активных выдач (10)';
    ELSIF COALESCE(v_current_count, 0) <= 0 THEN
        v_reason_code := 'NO_COPIES';
        v_reason_text := 'Нет свободных экземпляров';
    ELSIF v_has_overdue THEN
        v_reason_code := 'RULES_VIOLATION';
        v_reason_text := 'У читателя есть просроченные книги';
    END IF;

    IF v_reason_code IS NULL THEN
        RETURN NEW;
    END IF;

--     поиск причины
    SELECT rr.reason_id
    INTO v_reason_id
    FROM refusal_reason rr
    WHERE rr.reason_code = v_reason_code
    LIMIT 1;

    INSERT INTO refusal_doc (request_id, operator_user_id, refusal_date)
    VALUES (NEW.request_id, v_operator_user_id, CURRENT_DATE)
    RETURNING refusal_doc_id INTO v_refusal_doc_id;

    INSERT INTO refusal_item (refusal_doc_id, edition_id, reason_id, reason_text)
    VALUES (v_refusal_doc_id, NEW.edition_id, v_reason_id, v_reason_text);

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_request_item_guard ON request_item;
CREATE TRIGGER trg_request_item_guard
    BEFORE INSERT
    ON request_item
    FOR EACH ROW
EXECUTE FUNCTION trg_request_item_guard_fn();

