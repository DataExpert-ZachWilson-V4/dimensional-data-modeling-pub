-- Backfill query that populates entire actors_history_scd table at once


INSERT INTO shruthishridhar.actors_history_scd
WITH
    lagged AS ( -- calculate previous year's quality_class and is_active for each actor
        SELECT
            actor,
            quality_class,
            LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY quality_class) AS quality_class_last_year,  -- quality_class of the previous year for the same actor
            is_active,
            LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS is_active_last_year,   -- is_active status of the previous year for the same actor
            current_year
        FROM shruthishridhar.actors
        WHERE current_year <= 2015
    ),
    track_combined_changed AS ( -- identify changes in quality_class and is_active for each actor
        SELECT
            *,
            SUM (   -- cumulative sum that increments when there is a change in either quality_class or is_active
                CASE
                    WHEN is_active <> is_active_last_year AND quality_class <> quality_class_last_year THEN 1
                    ELSE 0
                END
            ) OVER ( PARTITION by actor ORDER BY current_year ) AS combined_change_identifier
        FROM lagged
    )
SELECT
    actor,
    MAX(quality_class) as quality_class,
    MAX(is_active) as is_active,
    MIN(current_year) AS start_year,    -- earliest year of the period for each group
    MAX(current_year) AS end_year,  -- latest year of the period for each group
    2015 AS current_year
FROM track_combined_changed
GROUP BY actor, combined_change_identifier