INSERT INTO
jesusr93.actors
WITH last_year AS (
    SELECT
        actor,
        actor_id,
        films,
        quality_class,
        is_active,
        current_year
    FROM jesusr93.actors
    WHERE current_year = 1923
),

this_year AS (
    -- I decided to group as one actor can have more than one film in a year
    -- Added the year to the array will let us to explode it preserving the year
    SELECT
        actor,
        actor_id,
        year,
        ARRAY_AGG(
            CAST(
                ROW(film, votes, rating, film_id, year)
                AS ROW (
                    film VARCHAR,
                    votes BIGINT,
                    rating DOUBLE,
                    film_id VARCHAR,
                    year INTEGER
                )
            )
            ORDER BY film_id
        ) AS films,
        AVG(rating) AS avg_rating
    FROM bootcamp.actor_films
    WHERE year = 1924
    GROUP BY actor, actor_id, year
)

SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
        WHEN ty.films IS NULL THEN ly.films
        WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL
            THEN ty.films || ly.films
    END AS films,
    CASE
        WHEN ty.avg_rating IS NULL THEN ly.quality_class
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
        WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
        WHEN ty.avg_rating <= 6 THEN 'bad'
    END AS quality_class,
    ty.year IS NOT NULL AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year AS ly
FULL OUTER JOIN this_year AS ty ON ly.actor_id = ty.actor_id
