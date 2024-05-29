INSERT INTO actors
--incremental data loading with previous data from 1914
WITH
    last_year_scd AS (
        SELECT
            *
        FROM
            actors
        WHERE
            current_year = 1914
    ),
    --incremental data loading with new data from 1915
    this_year_scd AS (
        SELECT
            actor,
            actor_id,
            --aggregate by actor the films they were in for the year 1915
            ARRAY_AGG(
                CAST(
                    ROW(film, votes, rating, film_id) AS ROW(
                        film VARCHAR,
                        votes INTEGER,
                        rating DOUBLE,
                        film_id VARCHAR
                    )
                )
            ) AS films,
            --categorical buckets for ratings from this year's films
            CASE
                WHEN AVG(rating) > 8 THEN 'star'
                WHEN AVG(rating) > 7 THEN 'good'
                WHEN AVG(rating) > 6 THEN 'average'
                ELSE 'bad'
            END AS quality_class
        FROM
            bootcamp.actor_films
        WHERE
            YEAR = 1915
        GROUP BY
            actor,
            actor_id
    )
SELECT
    COALESCE(l.actor, t.actor) AS actor,
    COALESCE(l.actor_id, t.actor_id) AS actor_id,
    --concatenate the two arrays of rows unless one is empty
    CASE
        WHEN t.actor IS NULL THEN l.films
        WHEN l.actor IS NULL THEN t.films
        ELSE CONCAT(t.films, l.films)
    END AS films,
    --most recent quality class is used if it exists, else most recent is used
    COALESCE(t.quality_class, l.quality_class) AS quality_class,
    --if actor appears in this_year data then they are active
    CASE
        WHEN t.actor IS NULL THEN FALSE
        ELSE TRUE
    END AS is_active,
    1915 AS year
FROM
    last_year_scd AS l
    FULL OUTER JOIN this_year_scd AS t ON t.actor_id = l.actor_id
    AND t.actor = l.actor