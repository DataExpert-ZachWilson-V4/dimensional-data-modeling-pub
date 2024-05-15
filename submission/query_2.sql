-- Query that populates the actors table one year at a time
INSERT INTO
    actors
WITH
    -- CTE to hold data from the 'actors' table for specific year
    last_year AS (
        SELECT
            *
        FROM
            actors
        WHERE
            current_year = 1999
    ),
    -- CTE to hold aggregated and computed data from the 'bootcamp.actor_films' table for the next year
    this_year AS (
        SELECT
            actor,
            actor_id,
            -- Aggregating all film details into an array for each actor
            array_agg (ROW (year, film, votes, rating, film_id)) AS films,
            -- Determining the 'quality_class' based on the average film rating
            CASE
                WHEN AVG(rating) > 8 THEN 'star'
                WHEN AVG(rating) > 7 THEN 'good'
                WHEN AVG(rating) > 6 THEN 'average'
                ELSE 'bad'
            END AS quality_class,
            year
        FROM
            actor_films
        WHERE
            year = 2000
            -- Grouping the results by actor, actor_id, and year for aggregation
        GROUP BY
            actor,
            actor_id,
            year
    )
    -- Final SELECT statement to merge and insert the data from 'last_year' 
    -- and 'this_year' into the 'actors' table
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
        WHEN ly.films IS NULL THEN ty.films
        WHEN ty.films IS NOT NULL THEN ly.films || ty.films
        ELSE ly.films
    END AS films,
    COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
    COALESCE(ty.actor_id, ly.actor_id) IS NOT NULL AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
    last_year ly
    -- Joining on actor_id and ensuring the years are sequential
    FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
    AND ly.current_year = ty.year - 1
