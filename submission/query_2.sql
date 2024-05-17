INSERT INTO luiscoelho37431.actors
-- Inserting data into the 'actors' table in the 'luiscoelho37431' schema

WITH last_year_cte AS (
    -- Common Table Expression (CTE) to select data from the 'actors' table where the current_year is 1913
    SELECT *
    FROM luiscoelho37431.actors
    -- We start with year 1913, as the first year for which we have data is 1914
    WHERE current_year = 1913
),
this_year_agg_cte AS (
    -- Another CTE to aggregate data from the 'actor_films' table for the year 1914
    SELECT
        ty.actor,
        ty.actor_id,
        -- Since there can be more than one film per actor per year, we use ARRAY_AGG to store them in an array
        -- Here I wasn't sure whether there was another magic way to do it without using ARRAY_AGG
        ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
        CASE
            WHEN SUM(rating) / COUNT(*) > 8 THEN 'star'
            WHEN (SUM(rating) / COUNT(*) > 7 AND SUM(rating) / COUNT(*) <= 8) THEN 'good'
            WHEN (SUM(rating) / COUNT(*) > 6 AND SUM(rating) / COUNT(*) <= 7) THEN 'average'
            WHEN (SUM(rating) / COUNT(*) <= 6) THEN 'bad'
        END AS quality_class,
        ty.year AS year
    FROM bootcamp.actor_films AS ty
    WHERE year = 1914
    GROUP BY ty.actor, ty.actor_id, ty.year
)
-- Selecting data from the last_year_cte and this_year_agg_cte CTEs
SELECT
    -- Use COALESCE to select the actor and actor_id from either last_year_cte or this_year_agg_cte in case of NULL values
    COALESCE(ly.actor, tya.actor) AS actor, 
    COALESCE(ly.actor_id, tya.actor_id) AS actor_id, 
    -- Cases for handling different scenarios of films data 
    CASE
        WHEN ly.films IS NULL AND tya.films IS NOT NULL THEN tya.films -- If films in last_year_cte is null and films in this_year_agg_cte is not null, select films from this_year_agg_cte
        WHEN ly.films IS NOT NULL AND tya.films IS NOT NULL THEN tya.films || ly.films -- If films in both last_year_cte and this_year_agg_cte are not null, concatenate them
        WHEN tya.films IS NULL THEN ly.films -- If films in this_year_agg_cte is null, select films from last_year_cte
    END AS films,
    COALESCE(tya.quality_class, ly.quality_class) AS quality_class, -- Using COALESCE to select the quality_class from either last_year_cte or this_year_agg_cte
    tya.year IS NOT NULL AS is_active, -- Checking if year in this_year_agg_cte is not null and assigning the result to is_active
    COALESCE(tya.year, ly.current_year + 1) AS current_year -- Using COALESCE to select the year from either this_year_agg_cte or incrementing the current_year from last_year_cte by 1
FROM last_year_cte AS ly
-- Use FULL OUTER JOIN to combine data from last_year_cte and this_year_agg_cte based on actor_id
FULL OUTER JOIN this_year_agg_cte AS tya
ON ly.actor_id = tya.actor_id
