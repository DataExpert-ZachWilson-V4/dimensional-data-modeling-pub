INSERT INTO jlcharbneau.actors_history_scd
WITH current_data AS (
    SELECT *
    FROM jlcharbneau.actors
    WHERE current_year = 1920  -- This should be parameterized for dynamic year handling
),
     previous_data AS (
         SELECT *
         FROM jlcharbneau.actors_history_scd
         WHERE end_date = DATE '9999-12-31'  -- Current active records
     ),
     detected_changes AS (
         SELECT
             cd.actor_id,
             cd.actor,
             cd.quality_class,
             cd.is_active,
             CASE
                 WHEN pd.actor_id IS NULL OR cd.quality_class <> pd.quality_class OR cd.is_active <> pd.is_active THEN DATE(CONCAT(CAST(cd.current_year AS VARCHAR), '-01-01'))
    ELSE pd.start_date
END AS start_date,
        CASE
            WHEN pd.actor_id IS NULL OR cd.quality_class <> pd.quality_class OR cd.is_active <> pd.is_active THEN DATE '9999-12-31'
            ELSE pd.end_date
END AS end_date,
        cd.current_year as current_year
    FROM current_data cd
    LEFT JOIN previous_data pd ON cd.actor_id = pd.actor_id
)
SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
FROM detected_changes
WHERE start_date = DATE(CONCAT(CAST(current_year AS VARCHAR), '-01-01')) OR end_date = DATE '9999-12-31'