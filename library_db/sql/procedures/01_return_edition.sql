ALTER TABLE issue_doc
    ADD COLUMN IF NOT EXISTS all_items_returned BOOLEAN NOT NULL DEFAULT FALSE;

CREATE OR REPLACE PROCEDURE return_edition(
    IN p_edition_id BIGINT,
    IN p_ticket_number VARCHAR(20)
)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_issue_doc_id BIGINT;
    v_due_date DATE;
    v_overdue_days INT;
    v_all_items_returned BOOLEAN;
BEGIN
    SELECT ii.issue_doc_id,
           ii.due_date
    INTO v_issue_doc_id,
         v_due_date
    FROM issue_item ii
             JOIN issue_doc id ON id.issue_doc_id = ii.issue_doc_id
             JOIN ticket t ON t.ticket_id = id.ticket_id
    WHERE ii.edition_id = p_edition_id
      AND t.ticket_number = p_ticket_number
      AND ii.return_date IS NULL
    ORDER BY id.issue_date DESC,
             id.issue_doc_id DESC
    LIMIT 1
    FOR UPDATE OF ii;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Активная выдача не найдена (edition_id=%, ticket_number=%)',
            p_edition_id, p_ticket_number;
    END IF;

    UPDATE issue_item
    SET return_date = CURRENT_DATE
    WHERE issue_doc_id = v_issue_doc_id
      AND edition_id = p_edition_id
      AND return_date IS NULL;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Возврат не оформлен: запись выдачи уже закрыта (issue_doc_id=%, edition_id=%)',
            v_issue_doc_id,
            p_edition_id;
    END IF;

    UPDATE edition
    SET current_count = current_count + 1
    WHERE edition_id = p_edition_id
      AND current_count < total_count;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Возврат не оформлен: остаток по изданию уже максимальный (edition_id=%)',
            p_edition_id;
    END IF;

    IF v_due_date < CURRENT_DATE THEN
        v_overdue_days := CURRENT_DATE - v_due_date;
        RAISE NOTICE 'Возврат оформлен. Просрочка: % полн. дн.', v_overdue_days;
    ELSE
        RAISE NOTICE 'Возврат оформлен без просрочки.';
    END IF;

    UPDATE issue_doc d
    SET all_items_returned = NOT EXISTS (
        SELECT 1
        FROM issue_item i
        WHERE i.issue_doc_id = d.issue_doc_id
          AND i.return_date IS NULL
    )
    WHERE d.issue_doc_id = v_issue_doc_id
    RETURNING d.all_items_returned INTO v_all_items_returned;

    IF v_all_items_returned THEN
        RAISE NOTICE 'По документу выдачи % возвращены все издания.', v_issue_doc_id;
    END IF;
END;
$$;
