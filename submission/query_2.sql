/* Cumulative Table Computation Query (query_2)

Write a query that populates the actors table one year at a time. 
*/
INSERT INTO danieldavid.actors

-- 1) Start: pull actors from last year to use as a starting point for this year
WITH last_year AS (
    SELECT * FROM danieldavid.actors 
    -- first year in dataset is 1914
    WHERE current_year = 1913
),
-- 2) Stage: clean this year's data to match target actors schema
    -- a) films: set aggregate array for films 
    -- b) quality_class: feature creation of quality_class using this year's avg(rating)
this_year_stage AS (
    SELECT
        actor,
        actor_id,
        -- a) films
        ARRAY_AGG(ROW(
            film,
            votes,
            rating,
            film_id
        )) AS films,
        -- b) quality_class
        -- TO CONSIDER: simple avg or weighted avg? 
        -- if a film has 1k votes w/ 4 rating vs a film with 1M votes w/ 8 rating, is avg really 6?
        CASE 
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
            WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
            WHEN AVG(rating) <= 6 THEN 'bad'
            ELSE NULL
        END AS quality_class,
        year as current_year
    FROM bootcamp.actor_films
    WHERE year = 1914
    GROUP BY actor, actor_id, year
)
-- 3) Cumulate: last year's and this year's stage data to cumulative table
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
        WHEN ly.films IS NOT NULL AND ty.films IS NOT NULL THEN ly.films || ty.films
        WHEN ty.films IS NULL THEN ly.films
        WHEN ly.films IS NULL THEN ty.films
    END AS films,
    -- ASSUMPTION: if actor not active this year, then keep last year's quality_class
    COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
    (ty.actor_id IS NOT NULL) AS is_active,
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM
    last_year ly
    FULL OUTER JOIN this_year_stage ty
    ON ly.actor_id = ty.actor_id
-- Go go chatgpt feedback! :)