

INSERT INTO
    phabrahao.actors_history_scd
WITH
    lagged AS (
        SELECT
            *,
            lag(quality_class) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) AS quality_class_last_year
        FROM
            phabrahao.actors
    ),
    changed AS (
        SELECT
            *,
            CASE
                WHEN quality_class <> quality_class_last_year THEN 1
                ELSE 0
            END AS changed
        FROM
            lagged
    ),
    streak AS (
        SELECT
            *,
            SUM(changed) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) AS streak
        FROM
            changed
    ),
    grouped AS (
        SELECT
            actor,
            actor_id,
            quality_class,
            max(is_active) AS is_active,
            min(current_year) AS start_date,
            max(current_year) AS end_date
        FROM
            streak
        GROUP BY
            actor,
            actor_id,
            quality_class,
            streak
    ),
    max_current_year AS (
        SELECT
            max(current_year) AS max_current_year
        FROM
            phabrahao.actors
    )
SELECT
    g.*,
    max_current_year AS current_year
FROM
    grouped g
    LEFT JOIN max_current_year mc ON 1 = 1