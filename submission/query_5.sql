

INSERT INTO
    phabrahao.actors_history_scd
WITH
    last_year AS (
        SELECT
            *
        FROM
            phabrahao.actors_history_scd
        WHERE
            current_year = 1920
    ),
    this_year AS (
        SELECT
            actor,
            actor_id,
            quality_class,
            is_active,
            current_year
        FROM
            phabrahao.actors
        WHERE
            current_year = 1921
    )
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    COALESCE(ly.quality_class, ty.quality_class) AS quality_class,
    COALESCE(ly.is_active, ty.is_active) AS is_active,
    COALESCE(ly.start_date, ty.current_year) AS start_date,
    COALESCE(ty.current_year, ly.end_date) AS end_date,
    COALESCE(ly.current_year + 1, ty.current_year) AS current_year
FROM
    last_year ly
    FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
    -- if quality class didn't change, it will be on the same row. If it did, it will create another row
    AND COALESCE(ly.quality_class, 'null') = COALESCE(ty.quality_class, 'null')
    AND ly.end_date + 1 = ty.current_year
ORDER BY
    actor,
    start_date