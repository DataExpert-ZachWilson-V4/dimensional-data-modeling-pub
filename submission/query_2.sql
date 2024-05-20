-- Query that populates the actors table one year at a time
INSERT INTO positivelyamber.actors
-- Get last year's data from actors table
WITH last_year AS (
    SELECT * FROM positivelyamber.actors
    WHERE current_year = 1991
),
-- Get this year's data from the bootcamp table and format
this_year AS (
    SELECT 
        actor, 
        actor_id,
        ARRAY_AGG(ROW(
            film, 
            votes,
            rating,
            film_id
        )) AS films,
        AVG(rating) as avg_rating,
        year
    FROM bootcamp.actor_films
    WHERE year = 1992
    GROUP BY actor, actor_id, year
)
SELECT 
    -- Prevent nulls if the data from last year doesn't exist yet
    COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actor_id, ty.actor_id) as actor_id,
    CASE
        -- If tha actor is not active this year then bring in previous films
        WHEN ty.films IS NULL THEN ly.films
        -- First time we are seeing this actor, so create the films arrray
        WHEN ty.films IS NOT NULL and ly.films IS NULL THEN ty.films
        -- Adding new films to the array, so concat new with previous film array
        WHEN ty.films iS NOT NULL and ly.films IS NULL THEN ty.films || ly.films
    END as films, 
    -- Categorize average rating for actor's films in the current year
    CASE
        WHEN ty.films IS NULL THEN NULL
        WHEN ty.films IS NOT NULL THEN
            CASE
                WHEN ty.avg_rating > 8 THEN 'star'
                WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
                WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
                WHEN ty.avg_rating <= 6 THEN 'bad'
            END
    END AS quality_class,
    -- If current year is present then the actor is active
    ty.year IS NOT NULL AS is_active,  
    -- Prevent null if no data from this year
    COALESCE(ty.year, ly.current_year+1) as current_year
FROM last_year ly 
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id