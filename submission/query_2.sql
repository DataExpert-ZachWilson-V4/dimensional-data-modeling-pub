INSERT INTO datademonslayer.actors
-- Create the CTE for actors from last year (1913)
WITH last_year AS (
    SELECT actor,
           actor_id,
           films,
           quality_class,
           is_active,
           current_year
    FROM datademonslayer.actors
    WHERE current_year = 1913
),

-- Define a temporary table for actor films from the current year (1914)
current_year_temp AS (
    SELECT actor,
           actor_id,
           year,
           ARRAY_AGG(
               ROW(
                   film,
                   votes,
                   rating,
                   film_id,
                   year -- Include the film's release year in the film details
               )
           ) AS films,
           AVG(rating) AS average_rating
    FROM bootcamp.actor_films
    WHERE year = 1914
    GROUP BY actor, actor_id, year
),

-- Define the current year CTE, incorporating results from the temp table and assigning quality classes based on average ratings
current_year AS (
    SELECT actor,
           actor_id,
           year,
           films,
           CASE
               WHEN average_rating > 8 THEN 'star'
               WHEN average_rating > 7 THEN 'good'
               WHEN average_rating > 6 THEN 'average'
               ELSE 'bad'
           END AS quality_class
    FROM current_year_temp
)

-- Select and coalesce data from last year and this year, effectively merging records while updating film listings and other attributes
SELECT
    COALESCE(ls.actor, ts.actor) AS actor,
    COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
    CASE
        WHEN ls.films IS NOT NULL AND ts.films IS NOT NULL THEN ls.films || ts.films
        WHEN ls.films IS NULL THEN ts.films
        WHEN ts.films IS NULL THEN ls.films
    END AS films,
    COALESCE(ts.quality_class, ls.quality_class) AS quality_class,
    (ts.actor_id IS NOT NULL) AS is_active,
    ts.year AS current_year -- Set the current year from the current_year CTE
FROM
    last_year ls
        FULL OUTER JOIN current_year ts
                        ON ls.actor_id = ts.actor_id