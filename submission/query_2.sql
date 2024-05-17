
--INSERT INTO faraanakmirzaei15025.actors
--data for last year
WITH last_year AS (
    SELECT
        actor,
        actor_id,
        films,
        quality_class,
        is_active,
        current_year
    FROM faraanakmirzaei15025.actors
    WHERE current_year = 2000
),
--data for current year
this_year AS (
    SELECT
        actor,
        actor_id,
        ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
        CASE 
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 THEN 'good'
            WHEN AVG(rating) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        true AS is_active,
        year AS current_year        
    FROM bootcamp.actor_films
    WHERE year = 2001
    GROUP BY actor, actor_id, year
)
    SELECT
        COALESCE(ty.actor, ly.actor) AS actor,
        COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
        CASE
            WHEN ty.is_active AND ly.is_active THEN ly.films || ty.films
            WHEN ty.is_active AND NOT ly.is_active THEN ty.films
            WHEN NOT ty.is_active AND ly.is_active THEN ly.films
            WHEN ty.is_active AND ly.is_active IS NULL THEN ty.films
        END AS films,
        COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
        COALESCE(ty.actor_id IS NOT NULL, false) AS is_active,
        ty.current_year
    FROM
        this_year ty
        FULL OUTER JOIN last_year ly ON ty.actor_id = ly.actor_id
