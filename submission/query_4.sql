INSERT INTO
    actors_history_scd WITH lagged AS (
        SELECT
            actor,
            quality_class,
            LAG(quality_class, 1) OVER (
                PARTITION BY actor
                ORDER BY
                    current_year
            ) AS quality_class_last_year, -- Using LAG to determine the quality class of an actor last year.
            is_active,
            LAG(is_active, 1) OVER (
                PARTITION BY actor
                ORDER BY
                    current_year
            ) as is_active_last_year, -- Using LAG to determine the activity of an actor last year
            current_year
        FROM
            actors
    ),
    streaked AS (
        SELECT
            *,
            -- Sum the number of times that either the activity or quality class of an actor
            -- has changed from the prior year to the current year. This indicates that
            -- one of the slowly changing dimensions has changed. 
            SUM(
                CASE
                    WHEN is_active <> is_active_last_year
                    OR quality_class <> quality_class_last_year THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY actor
                ORDER BY
                    current_year
            ) AS tracked_changes
        FROM
            lagged
    )
SELECT
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_date, -- Determine the start date of every interval which dimensions did not change.
    MAX(current_year) AS end_date, -- Determine the end date of every interval which dimensions did not change.
    2021 as current_year
FROM
    streaked
GROUP BY
    actor,
    is_active,
    quality_class,
    tracked_changes

