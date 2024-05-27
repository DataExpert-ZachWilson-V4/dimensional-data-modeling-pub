-- select *  from xeno.actors
-- select actor, quality_class, is_active  from xeno.actors
INSERT INTO
    actors_history_scd
with
    lagged as (
        SELECT
            actor,
            actor_id,
            CASE
                WHEN is_active THEN 1
                ELSE 0
            END AS is_active,
            CASE
                WHEN LAG(is_active, 1) OVER (
                    PARTITION BY
                        actor
                    ORDER BY
                        current_year
                ) THEN 1
                ELSE 0
            END AS has_activity,
            quality_class,
            LAG(quality_class, 1) OVER (
                PARTITION BY
                    actor
                ORDER BY
                    current_year
            ) as has_quality_class,
            current_year
        FROM
            xeno.actors
    ),
    streaked as (
        SELECT
            *,
            SUM(
                CASE
                    WHEN is_active <> has_activity
                    OR quality_class <> has_quality_class THEN 1
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
    actor_id,
    MAX(quality_class) as quality_class,
    CASE
        WHEN MAX(is_active) = 1 THEN true
        ELSE false
    END AS is_active,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year
FROM
    streaked
GROUP BY
    actor,
    actor_id,
    streak_identifier