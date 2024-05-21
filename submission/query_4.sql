INSERT INTO
    barrocaeric.actors_history_scd
WITH
    lagged AS (
        SELECT
            actor_id,
            actor,
            quality_class,
            is_active,
            -- Window function: to identify the last active state
            LAG(is_active, 1) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) as is_active_last_year,
            -- Window function: to identify the last quality_class state
            LAG(quality_class, 1) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) as quality_class_last_year,
            current_year
        FROM
            barrocaeric.actors
    ),
    -- I chose to have two separated fields to identify streaks at is_active and quality_class
    -- would it be possible to do it with just one?
    streaked AS (
        SELECT
            *,
            SUM(
                CASE
                    WHEN is_active <> is_active_last_year THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) AS is_active_streak_identifier,
            SUM(
                CASE
                    WHEN quality_class <> quality_class_last_year THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) AS quality_class_streak_identifier
        FROM
            lagged
    )

SELECT
    actor_id,
    actor,
    any_value (quality_class) as quality_class,
    MAX(is_active) AS is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    -- Not really necessary here since I am not using it to filter data in the first query, 
    -- but just to have it equal the other assigments I am fixing it to the year 2000
    2000 AS current_year
FROM
    streaked
GROUP BY
    actor_id,
    actor,
    is_active_streak_identifier,
    quality_class_streak_identifier