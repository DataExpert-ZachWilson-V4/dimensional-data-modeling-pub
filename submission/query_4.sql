INSERT INTO
    jb19881.actors_history_scd
WITH
    lagged AS (
        SELECT
            actor,
            actor_id,
            is_active,
            CASE
                WHEN LAG(is_active, 1) OVER (
                    PARTITION BY
                        actor_id
                    ORDER BY
                        current_year
                ) THEN True
                ELSE False
            END AS is_active_last_year,
            quality_class,
            COALESCE(
                LAG(quality_class, 1) OVER (
                    PARTITION BY
                        actor_id
                    ORDER BY
                        current_year
                ),
                'first year'
            ) as quality_class_last_year,
            current_year
        FROM
            jb19881.actors
        WHERE
            current_year <= 1916
    ),
    streaked AS (
        SELECT
            *,
            SUM(
                CASE
                    WHEN is_active <> is_active_last_year
                    OR quality_class <> quality_class_last_year THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) AS streak_identifier
        FROM
            lagged
    )
    --  select * from streaked  order by actor, current_year
SELECT
    actor,
    actor_id,
    quality_class,
    MAX(is_active) = True AS is_active,
    DATE(CAST(MIN(current_year) as varchar) || '-01-01') AS start_date,
    DATE(CAST(MAX(current_year) as varchar) || '-12-31') AS end_date,
    1916 AS current_year
FROM
    streaked
GROUP BY
    actor,
    actor_id,
    quality_class,
    streak_identifier