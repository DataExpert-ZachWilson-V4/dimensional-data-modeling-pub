-- Insert or update records in the videet.actors table
INSERT INTO videet.actors (
    actor,
    actor_id,
    films,
    quality_class,
    is_active,
    current_year
)
-- Define a CTE to fetch last year's data from the videet.actors table
WITH last_year AS (
    SELECT
        *
    FROM
        videet.actors
    WHERE
        current_year = 2020  -- Filtering data for the previous year
),
-- Define a CTE to calculate this year's film data and average ratings
this_year AS (
    SELECT
        actor,
        actor_id,
        year,
        -- Aggregate films into an array of structs, encapsulating details about each film
        ARRAY_AGG(ROW(film, votes, rating, film_id,year)) AS films,
        AVG(rating) AS avg_rating  -- Calculate the average film rating for the year
    FROM
        bootcamp.actor_films
    WHERE
        year = 2021  -- Consider only this year's film data
    GROUP BY
        actor,
        actor_id,
        year
)
-- Select and transform data to insert into the actors table
SELECT
    -- Use COALESCE to handle potential NULLs between this year's and last year's actor names and IDs
    COALESCE(ls.actor, ts.actor) AS actor,
    COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
    -- Decide which films data to carry forward or merge based on availability
    CASE
        WHEN ts.films IS NULL THEN ls.films
        WHEN ls.films IS NULL THEN ts.films
        WHEN ts.films IS NOT NULL AND ls.films IS NOT NULL THEN (ls.films || ts.films)
    END AS films,
    -- Categorize the quality class based on this year's average rating
    CASE
        WHEN ts.avg_rating > 8 THEN 'star'
        WHEN ts.avg_rating > 7 AND ts.avg_rating <= 8 THEN 'good'
        WHEN ts.avg_rating > 6 AND ts.avg_rating <= 7 THEN 'average'
        WHEN ts.avg_rating <= 6 THEN 'bad'
        ELSE null
    END AS quality_class,
    -- Set is_active to true if there is data for this year, indicating active status
    CASE
        WHEN ts.actor_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,
    -- Set current year to this year if available, otherwise increment last year
    COALESCE(ts.year, ls.current_year + 1) AS current_year
FROM
    last_year ls
    FULL OUTER JOIN this_year ts ON ts.actor_id = ls.actor_id  -- Join on actor_id to merge data
