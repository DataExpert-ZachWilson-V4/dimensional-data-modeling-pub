-- Populating shruthishridhar.actors table defined before according to description

INSERT INTO shruthishridhar.actors
WITH
    last_year AS (
        SELECT * FROM shruthishridhar.actors
        WHERE current_year = 1913   -- Oldest year - 1 in the actor_films dataset
    ),
    this_year AS (  -- aggregate film data for this year for this actor
        SELECT
            actor,
            actor_id, 
            ARRAY_AGG(
                DISTINCT ROW(   -- ignoring duplicate
                    film, 
                    votes, 
                    rating, 
                    film_id,
                    year
                )) as films,
            AVG(rating) as avg_rating,  -- average rating amongst all films this year
            year as current_year   -- setting year field as current year
        FROM bootcamp.actor_films
        WHERE year = 1914   -- Oldest year in the actor_films dataset
        GROUP BY actor, actor_id, year  -- group by non-aggregate fields
    )
SELECT 
    coalesce(ly.actor, ty.actor) AS actor,    -- coalesce actor name
    coalesce(ly.actor_id, ty.actor_id) AS actor_id,   -- coalesce actor id
    CASE
        WHEN ty.films IS NULL THEN ly.films     -- if this_year film data is null, use last year films data
        WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films    -- if this_year films data is not null and last_year films data is null,
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films    -- if this_year films data is not null and last_year films data is not null, use both -> this_year films data at the front followed by last_year films data
    END AS films,
    CASE
        WHEN ty.avg_rating is NOT NULL THEN
            CASE
                WHEN ty.avg_rating > 8 THEN 'star' -- star if this year's average rating > 8
                WHEN ty.avg_rating > 7 THEN 'good' -- good if this year's average rating > 7 <= 8
                WHEN ty.avg_rating > 6 THEN 'average'  -- average if this year's average rating > 6 <= 7
                ELSE 'bad'  -- bad if average rating <= 6
            END
        ELSE ly.quality_class
    END AS quality_class,
    ty.current_year IS NOT NULL as is_active,   -- is_active based on this_year data
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year    -- coalesce year
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actor_id