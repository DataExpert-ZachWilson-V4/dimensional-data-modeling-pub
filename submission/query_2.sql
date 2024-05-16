-- The actor_films dataset contains data for year 1914 to 2021, inclusive.
-- The following code initializes the actors table with data from 2000 and
-- incrementally updates the table with data for 2001.

-- 2000:2001
INSERT INTO erich.actors
WITH last_year AS (
    SELECT *
    FROM erich.actors
    WHERE current_year = 2000
),
this_year AS (
    SELECT
        actor,
        actor_id,
        -- Aggregating actor films for this year
        ARRAY_AGG(
            ROW(
                film,
                votes,
                rating,
                film_id,
                year
            )
        ) AS films,
        AVG(rating) AS avg_rating,
        year
    FROM bootcamp.actor_films
    WHERE year = 2001
    GROUP BY actor, actor_id, year
)
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    -- Concatenating films from last year and this year
    CASE
        WHEN ty.year IS NULL THEN ly.films
        WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ty.films
        WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
    END AS films,
    -- Assigning quality class based on average rating
    CASE
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 THEN 'good'
        WHEN ty.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    CASE
        WHEN ty.year IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
  FROM last_year ly
  FULL OUTER JOIN
    this_year ty
    ON ly.actor_id = ty.actor_id