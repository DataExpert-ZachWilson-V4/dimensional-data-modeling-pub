INSERT INTO actors

-- get cumulative data from last year
WITH last_year AS (
    SELECT * FROM actors
    WHERE current_year = 1914
),

-- get current year data
this_year AS (
    SELECT 
        actor,
        actor_id,
        year,
        ARRAY_AGG (ROW(year, film, votes, rating, film_id)) AS films,
        AVG(rating) AS avg_rating
        -- we can also take the avg with a REDUCE function 
    FROM bootcamp.actor_films
    WHERE year = 1915
    GROUP BY actor, actor_id, year
)

-- FULL JOIN them together
SELECT 
    COALESCE(ty.actor, ly.actor) AS actor,
    COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
    CASE
        WHEN ty.films IS NULL THEN ly.films
        WHEN ly.films IS NULL THEN ty.films
        ELSE ty.films || ly.films
    END AS films,
    CASE 
        WHEN ty.avg_rating IS NULL THEN ly.quality_class
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
        WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
        ELSE 'bad'
    END AS quality_class, -- we can also do this in current_year but I think it makes more sense here
    CASE 
        WHEN ty.actor_id IS NOT NULL THEN TRUE 
        ELSE FALSE
    END AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty 
ON ly.actor_id = ty.actor_id