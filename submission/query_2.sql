INSERT INTO
    jb19881.actors
WITH
    current_year_actor_films as (
        select
            actor,
            actor_id,
            array_agg(
                cast(
                    row(film, votes, rating, film_id) as row(
                        film varchar,
                        votes integer,
                        rating double,
                        film_id varchar
                    )
                )
            ) as films,
            -- Calculating the average rating per year based on the LLM feedback to remove a CTE for readability
            avg(rating) as avg_rating,
            year as current_year
        FROM
            bootcamp.actor_films
        WHERE
            year = 1917
        GROUP BY
            actor,
            actor_id,
            year
    ),
    prev_year_actors as (
        SELECT
            *
        FROM
            jb19881.actors
        WHERE
            current_year = 1917 - 1
    ),
    full_join as (
        SELECT
            COALESCE(
                current_year_actor_films.actor,
                prev_year_actors.actor
            ) as actor,
            COALESCE(
                current_year_actor_films.actor_id,
                prev_year_actors.actor_id
            ) as actor_id,
            COALESCE(prev_year_actors.films, array[]) || COALESCE(current_year_actor_films.films, array[]) as films,
            -- TRANSFORM(
            --     COALESCE(prev_year_actors.films, array[]) || coalesce(current_year_actor_films.films, array[]),
            --     x -> x.rating
            -- ) as ratings,
            COALESCE(current_year_actor_films.avg_rating, prev_year_actors.avg_rating) as avg_rating,
            current_year_actor_films.actor_id IS NOT NULL as is_active
        FROM
            current_year_actor_films
            FULL OUTER JOIN prev_year_actors ON prev_year_actors.actor_id = current_year_actor_films.actor_id
    ),
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
select
    actor,
    actor_id,
    films,
    CASE 
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 and avg_rating <= 8 THEN 'good'
        WHEN avg_rating > 6 and avg_rating <= 7 THEN 'average'
        WHEN avg_rating <= 6 THEN 'bad'
        WHEN avg_rating is NULL THEN 'no_ratings'
    END as quality_class,
    is_active,
    1917 as current_year
from
    full_join 