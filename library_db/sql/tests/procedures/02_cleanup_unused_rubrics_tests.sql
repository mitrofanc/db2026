BEGIN;

-- удаление пустой рубрики листа
SAVEPOINT sp_cleanup_pos_1;
DO
$$
DECLARE
    v_leaf_id BIGINT;
    v_parent_id BIGINT;
    v_target_id BIGINT;
    v_exists_leaf BOOLEAN;
    v_exists_parent BOOLEAN;
BEGIN
    WITH leaf_candidates AS (
        SELECT leaf.rubric_id AS leaf_id,
               leaf.parent_rubric_id AS parent_id
        FROM rubric leaf
        WHERE leaf.parent_rubric_id IS NOT NULL
          AND NOT EXISTS (SELECT 1
                          FROM rubric c
                          WHERE c.parent_rubric_id = leaf.rubric_id)
          AND EXISTS (SELECT 1
                      FROM edition e
                      WHERE e.rubric_id = leaf.rubric_id)
    )
    SELECT lc.leaf_id,
           lc.parent_id
    INTO v_leaf_id,
         v_parent_id
    FROM leaf_candidates lc
    WHERE EXISTS (
        SELECT 1
        FROM edition e
        WHERE e.rubric_id IN (
            SELECT r2.rubric_id
            FROM rubric r2
            WHERE r2.rubric_id = lc.parent_id
               OR r2.parent_rubric_id = lc.parent_id
        )
          AND e.rubric_id <> lc.leaf_id
    )
    ORDER BY lc.leaf_id
    LIMIT 1;

    IF v_leaf_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для positive #1 cleanup_unused_rubrics';
    END IF;

    SELECT r.rubric_id
    INTO v_target_id
    FROM rubric r
    WHERE r.rubric_id <> v_leaf_id
    ORDER BY r.rubric_id
    LIMIT 1;

    UPDATE edition
    SET rubric_id = v_target_id
    WHERE rubric_id = v_leaf_id;

    CALL cleanup_unused_rubrics();

    SELECT EXISTS (SELECT 1 FROM rubric r WHERE r.rubric_id = v_leaf_id)
    INTO v_exists_leaf;

    IF v_exists_leaf THEN
        RAISE EXCEPTION 'Positive #1 failed: leaf rubric % was not deleted', v_leaf_id;
    END IF;

    SELECT EXISTS (SELECT 1 FROM rubric r WHERE r.rubric_id = v_parent_id)
    INTO v_exists_parent;

    IF NOT v_exists_parent THEN
        RAISE EXCEPTION 'Positive #1 failed: parent rubric % should stay', v_parent_id;
    END IF;

    RAISE NOTICE 'PASS: Positive #1 (удаление неиспользуемой листовой рубрики)';
END;
$$;
ROLLBACK TO SAVEPOINT sp_cleanup_pos_1;

-- удаление пустой подветки
SAVEPOINT sp_cleanup_pos_2;
DO
$$
DECLARE
    v_root_id BIGINT;
    v_target_id BIGINT;
    v_subtree_ids BIGINT[];
    v_left_count INT;
BEGIN
    SELECT r.rubric_id
    INTO v_root_id
    FROM rubric r
    WHERE r.parent_rubric_id IS NULL
      AND EXISTS (SELECT 1 FROM rubric c WHERE c.parent_rubric_id = r.rubric_id)
    ORDER BY r.rubric_id
    LIMIT 1;

    IF v_root_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для positive #2 cleanup_unused_rubrics';
    END IF;

    WITH RECURSIVE subtree AS (
        SELECT r.rubric_id
        FROM rubric r
        WHERE r.rubric_id = v_root_id
        UNION ALL
        SELECT c.rubric_id
        FROM subtree s
                 JOIN rubric c ON c.parent_rubric_id = s.rubric_id
    )
    SELECT ARRAY_AGG(s.rubric_id ORDER BY s.rubric_id)
    INTO v_subtree_ids
    FROM subtree s;

    IF v_subtree_ids IS NULL OR CARDINALITY(v_subtree_ids) = 0 THEN
        RAISE EXCEPTION 'Positive #2 failed: пустое поддерево для root %', v_root_id;
    END IF;

    SELECT r.rubric_id
    INTO v_target_id
    FROM rubric r
    WHERE r.rubric_id <> ALL (v_subtree_ids)
    ORDER BY r.rubric_id
    LIMIT 1;

    IF v_target_id IS NULL THEN
        RAISE EXCEPTION 'Positive #2 failed: не найдена рубрика вне удаляемого поддерева';
    END IF;

    UPDATE edition e
    SET rubric_id = v_target_id
    WHERE e.rubric_id = ANY (v_subtree_ids);

    CALL cleanup_unused_rubrics();

    SELECT COUNT(*)
    INTO v_left_count
    FROM rubric r
    WHERE r.rubric_id = ANY (v_subtree_ids);

    IF v_left_count <> 0 THEN
        RAISE EXCEPTION 'Positive #2 failed: expected deleted subtree size %, left %',
            CARDINALITY(v_subtree_ids),
            v_left_count;
    END IF;

    RAISE NOTICE 'PASS: Positive #2 (удаление неиспользуемого поддерева)';
END;
$$;
ROLLBACK TO SAVEPOINT sp_cleanup_pos_2;

-- ветка с одним изданием не удаляется
SAVEPOINT sp_cleanup_neg_1;
DO
$$
DECLARE
    v_root_id BIGINT;
    v_target_id BIGINT;
    v_keep_edition_id BIGINT;
    v_keep_rubric_id BIGINT;
    v_subtree_ids BIGINT[];
    v_root_exists BOOLEAN;
    v_keep_rubric_exists BOOLEAN;
    v_keep_edition_rubric BIGINT;
BEGIN
    SELECT r.rubric_id
    INTO v_root_id
    FROM rubric r
    WHERE r.parent_rubric_id IS NULL
      AND EXISTS (SELECT 1 FROM rubric c WHERE c.parent_rubric_id = r.rubric_id)
    ORDER BY r.rubric_id
    LIMIT 1;

    IF v_root_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для negative #1 cleanup_unused_rubrics';
    END IF;

    WITH RECURSIVE subtree AS (
        SELECT r.rubric_id
        FROM rubric r
        WHERE r.rubric_id = v_root_id
        UNION ALL
        SELECT c.rubric_id
        FROM subtree s
                 JOIN rubric c ON c.parent_rubric_id = s.rubric_id
    )
    SELECT ARRAY_AGG(s.rubric_id ORDER BY s.rubric_id)
    INTO v_subtree_ids
    FROM subtree s;

    IF v_subtree_ids IS NULL OR CARDINALITY(v_subtree_ids) = 0 THEN
        RAISE EXCEPTION 'Negative #1 failed: пустое поддерево для root %', v_root_id;
    END IF;

    SELECT e.edition_id
    INTO v_keep_edition_id
    FROM edition e
    WHERE e.rubric_id = ANY (v_subtree_ids)
    ORDER BY e.edition_id
    LIMIT 1;

    IF v_keep_edition_id IS NULL THEN
        RAISE EXCEPTION 'Negative #1 failed: в поддереве % нет изданий для сохранения', v_root_id;
    END IF;

    SELECT e.rubric_id
    INTO v_keep_rubric_id
    FROM edition e
    WHERE e.edition_id = v_keep_edition_id;

    SELECT r.rubric_id
    INTO v_target_id
    FROM rubric r
    WHERE r.rubric_id <> ALL (v_subtree_ids)
    ORDER BY r.rubric_id
    LIMIT 1;

    IF v_target_id IS NULL THEN
        RAISE EXCEPTION 'Negative #1 failed: не найдена рубрика вне поддерева';
    END IF;

    UPDATE edition e
    SET rubric_id = v_target_id
    WHERE e.rubric_id = ANY (v_subtree_ids)
      AND e.edition_id <> v_keep_edition_id;

    CALL cleanup_unused_rubrics();

    SELECT EXISTS (SELECT 1 FROM rubric r WHERE r.rubric_id = v_root_id)
    INTO v_root_exists;

    IF NOT v_root_exists THEN
        RAISE EXCEPTION 'Negative #1 failed: root rubric % was deleted', v_root_id;
    END IF;

    SELECT EXISTS (SELECT 1 FROM rubric r WHERE r.rubric_id = v_keep_rubric_id)
    INTO v_keep_rubric_exists;

    IF NOT v_keep_rubric_exists THEN
        RAISE EXCEPTION 'Negative #1 failed: rubric with kept edition % was deleted', v_keep_rubric_id;
    END IF;

    SELECT e.rubric_id
    INTO v_keep_edition_rubric
    FROM edition e
    WHERE e.edition_id = v_keep_edition_id;

    IF v_keep_edition_rubric IS DISTINCT FROM v_keep_rubric_id THEN
        RAISE EXCEPTION 'Negative #1 failed: kept edition moved from rubric % to %',
            v_keep_rubric_id,
            v_keep_edition_rubric;
    END IF;

    RAISE NOTICE 'PASS: Negative #1 (рубрика с используемой веткой не удаляется)';
END;
$$;
ROLLBACK TO SAVEPOINT sp_cleanup_neg_1;

-- не пустая листовая рубрика не удаляется
SAVEPOINT sp_cleanup_neg_2;
DO
$$
DECLARE
    v_leaf_id BIGINT;
    v_exists_leaf BOOLEAN;
BEGIN
    SELECT r.rubric_id
    INTO v_leaf_id
    FROM rubric r
    WHERE NOT EXISTS (SELECT 1 FROM rubric c WHERE c.parent_rubric_id = r.rubric_id)
      AND EXISTS (SELECT 1 FROM edition e WHERE e.rubric_id = r.rubric_id)
    ORDER BY r.rubric_id
    LIMIT 1;

    IF v_leaf_id IS NULL THEN
        RAISE EXCEPTION 'Недостаточно данных для negative #2 cleanup_unused_rubrics';
    END IF;

    CALL cleanup_unused_rubrics();

    SELECT EXISTS (SELECT 1 FROM rubric r WHERE r.rubric_id = v_leaf_id)
    INTO v_exists_leaf;

    IF NOT v_exists_leaf THEN
        RAISE EXCEPTION 'Negative #2 failed: used leaf rubric % must stay', v_leaf_id;
    END IF;

    RAISE NOTICE 'PASS: Negative #2 (используемая листовая рубрика не удаляется)';
END;
$$;
ROLLBACK TO SAVEPOINT sp_cleanup_neg_2;

ROLLBACK;
\echo 'OK: cleanup_unused_rubrics tests passed'
