INSERT INTO bhakti.actors
WITH
    last_year_actors AS (
        -- Select all actors from the previous year
        SELECT
            *
        FROM
            bhakti.actors
        WHERE
            current_year =  1926-- Replace with the last year
    ),
    current_year_actors AS (
        -- Select actors from the current year and calculate film-related information
        SELECT
            actor_id,
            actor,
            ARRAY_AGG(ROW(film, votes, rating, film_id)) OVER (PARTITION BY actor_id, actor, year) AS films,
            quality_class,
            CASE 
                WHEN COUNT(film) OVER (PARTITION BY actor_id, actor, year) > 0 THEN TRUE
                ELSE FALSE
            END AS is_active,
            year
        FROM (
            -- Select films for each actor in the current year and calculate film-related information
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY actor_id, actor, year) AS rn,
                CASE 
                    WHEN AVG(rating) OVER (PARTITION BY actor_id, actor, year) > 8 THEN 'star'
                    WHEN AVG(rating) OVER (PARTITION BY actor_id, actor, year) > 7 THEN 'good'
                    WHEN AVG(rating) OVER (PARTITION BY actor_id, actor, year) > 6 THEN 'average'
                    ELSE 'bad'
                END AS quality_class
            FROM bootcamp.actor_films
            WHERE year = 1927 -- Replace with the current year
        ) t
        WHERE rn = 1
    )
SELECT
    COALESCE(ly.actor_id, ty.actor_id),
    COALESCE(ly.actor, ty.actor),
    COALESCE(ly.films, ty.films),
    COALESCE(ly.quality_class, ty.quality_class),
    COALESCE(ly.is_active, ty.is_active),
    COALESCE(ly.current_year, ty.year)
FROM
    last_year_actors ly
    FULL OUTER JOIN current_year_actors ty ON ly.actor = ty.actor