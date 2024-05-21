    -- CTEs:
    --     lagged: Selects actors' data from alia.actors, adding columns for previous year's is_active and quality_class values using the LAG function.
    --     streaked: Adds a streak_identifier that increments when there's a change in is_active or quality_class compared to the previous year.

    -- Main logic:
    --     Selects from streaked and groups by actor, actor_id, quality_class, is_active, and streak_identifier.
    --     For each group, calculates the start_date (minimum current_year) and end_date (maximum current_year).
    --     Adds the current year as current_year.


INSERT INTO
    alia.actors_history_scd
WITH
    lagged AS (
        SELECT
            actor,
            actor_id,
            is_active,
            LAG (is_active, 1) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) is_active_last_year,
            quality_class,
            LAG (quality_class, 1) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) quality_class_last_year,
            current_year
        FROM
            alia.actors
    ),
    streaked AS (
        SELECT
            *,
            SUM(
                CASE
                    WHEN is_active <> is_active_last_year
                    or quality_class <> quality_class_last_year THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) AS streak_identifier
        FROM
            lagged
    )
SELECT
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    EXTRACT(YEAR FROM CURRENT_DATE) AS current_year
FROM
    streaked
GROUP BY
    actor,
    actor_id,
    quality_class,
    is_active,
    streak_identifier