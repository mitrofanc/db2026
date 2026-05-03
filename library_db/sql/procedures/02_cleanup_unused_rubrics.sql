CREATE OR REPLACE PROCEDURE cleanup_unused_rubrics()
LANGUAGE plpgsql
AS
$$
DECLARE
    v_deleted_step_count INT;
    v_deleted_total_count INT := 0;
    v_deleted_names TEXT[] := ARRAY[]::TEXT[];
    v_step_names TEXT[];
    v_deleted_list TEXT;
BEGIN
    LOOP
        -- нет изданий в этой рубрике и нет дочерних рубрик
        WITH empty_leaf AS (
            SELECT r.rubric_id,
                   r.rubric_name
            FROM rubric r
            WHERE NOT EXISTS (
                SELECT 1
                FROM edition e
                WHERE e.rubric_id = r.rubric_id
            )
              AND NOT EXISTS (
                SELECT 1
                FROM rubric c
                WHERE c.parent_rubric_id = r.rubric_id
            )
        ),
        -- удаление рубрик, найденных в empty_leaf
            deleted AS (
                DELETE
                FROM rubric r
                    USING empty_leaf el
                WHERE r.rubric_id = el.rubric_id
                RETURNING r.rubric_id,
                    r.rubric_name
             )
        SELECT COALESCE(ARRAY_AGG(d.rubric_name ORDER BY d.rubric_name, d.rubric_id), ARRAY[]::TEXT[]),
               COUNT(*)
        INTO v_step_names,
             v_deleted_step_count
        FROM deleted d;

        -- пустых листьев нет, выходим
        EXIT WHEN v_deleted_step_count = 0;

        v_deleted_names := v_deleted_names || v_step_names;
        v_deleted_total_count := v_deleted_total_count + v_deleted_step_count;
    END LOOP;

    IF v_deleted_total_count = 0 THEN
        RAISE NOTICE 'Неиспользуемые рубрики не найдены.';
        RETURN;
    END IF;

    SELECT string_agg(name, ', ' ORDER BY name)
    INTO v_deleted_list
    FROM UNNEST(v_deleted_names) AS name;

    RAISE NOTICE 'Удалены рубрики (%): %', v_deleted_total_count, v_deleted_list;
END;
$$;
