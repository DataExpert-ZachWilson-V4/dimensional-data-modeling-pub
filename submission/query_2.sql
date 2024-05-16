 -- Query loads and inserts data on actors grouped by the film year
INSERT INTO amaliah21315.actors  -- Inserts the results of the CTE query into the actors table
WITH last_year_films AS (
    -- Gets the data of actors from the previous years stored in local actors table
    SELECT *
    FROM amaliah21315.actors
    WHERE current_year = 2011 -- previous reporting year
),
this_year_films AS (
    -- Selects actors, their IDs, film year, average ratings, and aggregates films into arrays
    SELECT
        actor,
        actor_id,
        year,
        AVG(rating) AS average_rating, -- generates the average rating across all films for the specific actor that year
        ARRAY_AGG ( -- agregates an array of film details for each actor each year
            ROW (
                ts.film,
                ts.year,
                ts.votes,
                ts.rating,
                ts.film_id
            )
        ) AS films
    FROM bootcamp.actor_films ts
    WHERE year = 2012 -- current reporting year
    GROUP BY actor, actor_id, year -- groups by the actor, its id and the year of the film
)
SELECT
    COALESCE(ls.actor, ts.actor) AS actor,  -- Coalesce actor to handle NULL values
    COALESCE(ls.actor_id, ts.actor_id) AS actor_id,  -- Coalesce actor to handle NULL values
    CASE
        WHEN ts.year IS NULL THEN ls.films  -- Use last year's films if no films for the current year
        WHEN ts.year IS NOT NULL AND ls.films IS NULL THEN ts.films  -- Use current year's films if no last year's films
        WHEN ts.year IS NOT NULL AND ls.films IS NOT NULL THEN ts.films || ls.films  -- Concatenate films if both years have films
    END AS films,
    ts.average_rating,
    CASE
        WHEN ts.average_rating > 8 THEN 'star'
        WHEN ts.average_rating > 7 AND ts.average_rating <= 8 THEN 'good'
        WHEN ts.average_rating > 6 AND ts.average_rating <= 7 THEN 'average'
        WHEN ts.average_rating <= 6 THEN 'bad'
    END AS quality_class, -- defines the quality class or grading based on the average rating
    ts.year IS NOT NULL AS is_active,  -- Indicates if the actor is active in the current year
    COALESCE(ts.year, ls.current_year + 1) AS current_year  -- Use current year or next year if current year is NULL
FROM
    last_year_films ls
    FULL OUTER JOIN this_year_films ts ON ls.actor_id = ts.actor_id  -- Full outer join to combine data from both years
