INSERT INTO
    barrocaeric.actors
WITH
    last_year AS (
        SELECT
            *
        FROM
            barrocaeric.actors
        WHERE
            current_year = 1999
    ),
    -- Moved aggregations for this CTE for simplicity and readability,
    -- it was necessary to aggregate data since one actor can have many films in 1 year.
    -- I think it is possible to achieve the same results using reduce or map instead of group by.
    this_year AS (
        SELECT
            actor_id,
            actor,
            array_agg((film, votes, rating, film_id, year)) FILTER(
                WHERE
                    film IS NOT NULL
            ) as film,
            AVG(rating) as rating,
            MAX(year) as year
        FROM
            bootcamp.actor_films
        WHERE
            year = 2000
        group by
            actor_id,
            actor
    )
SELECT
    COALESCE(ls.actor, ts.actor) as actor,
    COALESCE(ls.actor_id, ts.actor_id) as actor_id,
    CASE
        WHEN ts.film IS NULL THEN ls.films
        WHEN ts.film IS NOT NULL
        AND ls.films IS NULL THEN ts.film
        WHEN ts.film IS NOT NULL
        AND ls.films IS NOT NULL THEN ts.film || ls.films
    END as films,
    -- If there is no record for the actor in this year we should use last year quality class
    -- else we just have to compute the average of the current year ratings to classify the actor quality
    CASE
        WHEN ts.actor_id IS NULL THEN ls.quality_class
        WHEN ts.rating > 8 THEN 'star'
        WHEN ts.rating > 7 THEN 'good'
        WHEN ts.rating > 6 THEN 'average'
        ELSE 'bad'
    END as quality_class,
    ts.actor_id IS NOT NULL AS is_active,
    COALESCE(ts.year, ls.current_year + 1) as current_year
FROM
    last_year ls
    FULL OUTER JOIN this_year ts ON ls.actor_id = ts.actor_id
