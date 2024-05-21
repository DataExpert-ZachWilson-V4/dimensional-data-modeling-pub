INSERT INTO
    jb19881.actors
WITH
    current_year_actor_films AS (
        SELECT
            actor,
            actor_id,
            ARRAY_AGG(
                CAST(
                    row(film, votes, rating, film_id) AS row(
                        film varchar,
                        votes integer,
                        rating double,
                        film_id varchar
                    )
                )
            ) AS films,
            -- Calculating the average rating per year based on the LLM feedback to remove a CTE for readability
            AVG(rating) AS avg_rating,
            YEAR AS current_year
        FROM
            bootcamp.actor_films
        WHERE
            YEAR = 1917
        GROUP BY
            actor,
            actor_id,
            YEAR
    ),
    prev_year_actors AS (
        SELECT
            *,
            REDUCE(
                TRANSFORM(films, x -> x.rating),
                CAST(ROW(0.0, 0) AS ROW(SUM DOUBLE, COUNT INTEGER)),
                (s, x) -> CAST(
                    ROW(x + s.sum, s.count + 1) AS ROW(SUM DOUBLE, COUNT INTEGER)
                ),
                s -> IF(s.count = 0, NULL, s.sum / s.count)
            ) AS avg_rating
        FROM
            jb19881.actors
        WHERE
            current_year = 1917 - 1
    ),
    full_join AS (
        SELECT
            COALESCE(
                current_year_actor_films.actor,
                prev_year_actors.actor
            ) AS actor,
            COALESCE(
                current_year_actor_films.actor_id,
                prev_year_actors.actor_id
            ) AS actor_id,
            COALESCE(prev_year_actors.films, ARRAY[]) || COALESCE(current_year_actor_films.films, ARRAY[]) AS films,
            -- TRANSFORM(
            --     COALESCE(prev_year_actors.films, array[]) || coalesce(current_year_actor_films.films, array[]),
            --     x -> x.rating
            -- ) as ratings,
            COALESCE(
                current_year_actor_films.avg_rating,
                prev_year_actors.avg_rating
            ) AS avg_rating,
            current_year_actor_films.actor_id IS NOT NULL AS is_active
        FROM
            current_year_actor_films
            FULL OUTER JOIN prev_year_actors ON prev_year_actors.actor_id = current_year_actor_films.actor_id
    )
    -- avg_rating as (
    --     select
    --         actor,
    --         actor_id,
    --         films,
    --         is_active,
    --         REDUCE(
    --             ratings,
    --             CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
    --             (s, x) -> CAST(
    --                 ROW(x + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)
    --             ),
    --             s -> IF(s.count = 0, NULL, s.sum / s.count)
    --         ) as avg_rating
    --     FROM
    --         full_join
    -- )
SELECT
    actor,
    actor_id,
    films,
    CASE
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7
        AND avg_rating <= 8 THEN 'good'
        WHEN avg_rating > 6
        AND avg_rating <= 7 THEN 'average'
        WHEN avg_rating <= 6 THEN 'bad'
        WHEN avg_rating IS NULL THEN 'no_ratings'
    END AS quality_class,
    is_active,
    1917 AS current_year
FROM
    full_join