CREATE OR REPLACE FUNCTION trg_rubric_no_cycle_fn()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_cycle_found BOOLEAN;
BEGIN
--     корневая рубрика
    IF NEW.parent_rubric_id IS NULL THEN
        RETURN NEW;
    END IF;

    IF NEW.parent_rubric_id = NEW.rubric_id THEN
        RAISE EXCEPTION 'Рубрика % не может быть родителем самой себе', NEW.rubric_id;
    END IF;

    WITH RECURSIVE ancestors AS (SELECT r.rubric_id, r.parent_rubric_id
                                 FROM rubric r
                                 WHERE r.rubric_id = NEW.parent_rubric_id
                                 UNION
                                 SELECT r.rubric_id, r.parent_rubric_id
                                 FROM rubric r
                                          JOIN ancestors a ON r.rubric_id = a.parent_rubric_id)
    SELECT EXISTS (SELECT 1 FROM ancestors WHERE rubric_id = NEW.rubric_id)
    INTO v_cycle_found;

    IF v_cycle_found THEN
        RAISE EXCEPTION 'Нельзя создать цикл в иерархии рубрик (rubric_id=%, parent_rubric_id=%)',
            NEW.rubric_id, NEW.parent_rubric_id;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_rubric_no_cycle ON rubric;
CREATE TRIGGER trg_rubric_no_cycle
    BEFORE UPDATE OF parent_rubric_id
    ON rubric
    FOR EACH ROW
    WHEN (NEW.parent_rubric_id IS DISTINCT FROM OLD.parent_rubric_id)
EXECUTE FUNCTION trg_rubric_no_cycle_fn();

