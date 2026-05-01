BEGIN;

DO
$$
DECLARE
    v_parent_id         BIGINT;
    v_child_id          BIGINT;
    v_parent_old_parent BIGINT;

    v_parent_after      BIGINT;
    v_failed            BOOLEAN;
BEGIN
    SELECT p.rubric_id,
           c.rubric_id,
           p.parent_rubric_id
    INTO v_parent_id,
         v_child_id,
         v_parent_old_parent
    FROM rubric p
             JOIN rubric c ON c.parent_rubric_id = p.rubric_id
    ORDER BY p.rubric_id, c.rubric_id
    LIMIT 1;

    IF v_parent_id IS NULL OR v_child_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для теста. Нужна хотя бы одна пара parent-child в rubric';
    END IF;

    -- Positive #1: перенос дочерней рубрики в корень.
    UPDATE rubric
    SET parent_rubric_id = NULL
    WHERE rubric_id = v_child_id;

    SELECT parent_rubric_id INTO v_parent_after FROM rubric WHERE rubric_id = v_child_id;
    IF v_parent_after IS NOT NULL THEN
        RAISE EXCEPTION 'Positive #1 failed: expected NULL parent for child %, got %', v_child_id, v_parent_after;
    END IF;
    RAISE NOTICE 'PASS: Positive #1 (перенос в корень)';

    -- Positive #2: корректное возвращение дочерней рубрики к прежнему родителю.
    UPDATE rubric
    SET parent_rubric_id = v_parent_id
    WHERE rubric_id = v_child_id;

    SELECT parent_rubric_id INTO v_parent_after FROM rubric WHERE rubric_id = v_child_id;
    IF v_parent_after IS DISTINCT FROM v_parent_id THEN
        RAISE EXCEPTION 'Positive #2 failed: expected parent % for child %, got %',
            v_parent_id, v_child_id, v_parent_after;
    END IF;
    RAISE NOTICE 'PASS: Positive #2 (корректная смена родителя)';

    -- Negative #1: self-parent должен блокироваться.
    v_failed := FALSE;
    BEGIN
        UPDATE rubric
        SET parent_rubric_id = rubric_id
        WHERE rubric_id = v_child_id;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed := TRUE;
            RAISE NOTICE 'PASS: Negative #1 (self-parent blocked): %', SQLERRM;
    END;

    IF NOT v_failed THEN
        RAISE EXCEPTION 'Negative #1 failed: self-parent update was not blocked';
    END IF;

    SELECT parent_rubric_id INTO v_parent_after FROM rubric WHERE rubric_id = v_child_id;
    IF v_parent_after IS DISTINCT FROM v_parent_id THEN
        RAISE EXCEPTION 'Negative #1 failed: child parent changed to %, expected %', v_parent_after, v_parent_id;
    END IF;

    -- Negative #2: попытка создать цикл parent -> child должна блокироваться.
    v_failed := FALSE;
    BEGIN
        UPDATE rubric
        SET parent_rubric_id = v_child_id
        WHERE rubric_id = v_parent_id;
    EXCEPTION
        WHEN OTHERS THEN
            v_failed := TRUE;
            RAISE NOTICE 'PASS: Negative #2 (cycle blocked): %', SQLERRM;
    END;

    IF NOT v_failed THEN
        RAISE EXCEPTION 'Negative #2 failed: cycle update was not blocked';
    END IF;

    SELECT parent_rubric_id INTO v_parent_after FROM rubric WHERE rubric_id = v_parent_id;
    IF v_parent_after IS DISTINCT FROM v_parent_old_parent THEN
        RAISE EXCEPTION 'Negative #2 failed: parent old parent changed to %, expected %',
            v_parent_after, v_parent_old_parent;
    END IF;
END;
$$;

ROLLBACK;
