-- insert results into our actors table
insert into jlcharbneau.actors

-- create the "last year" cte
--  note we need to define our "current year" in the where clause
--  this should be one year *before* the year defined in current_year_temp
--  where clause
WITH last_year AS (
    SELECT actor,
           actor_id,
           films,
           quality_class,
           is_active,
           current_year
    FROM jlcharbneau.actors
    WHERE current_year = 1923
),
-- define a temporary table
current_year_temp AS (
    SELECT actor,
           actor_id,
           year,
           ARRAY_AGG(
            ROW(
                film,
                votes,
                rating,
                film_id
            )
           ) AS films,
           AVG(rating) AS average_rating
    FROM bootcamp.actor_films
    WHERE year = 1924
    GROUP BY actor, actor_id, year
),
-- define our current year cte pulling in our results from temp,
--   but making sure to build out our quality check
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

-- finally, get our results so they can be inserted into jlcharbneau.actors
--   coalesce the values, defaulting to those from last year
SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
    CASE
        WHEN ly.films IS NOT NULL
            AND cy.films IS NOT NULL
            THEN cy.films || ly.films
        WHEN ly.films IS NULL THEN cy.films
        WHEN cy.films IS NULL THEN ly.films
    END AS films,
    COALESCE(cy.quality_class, ly.quality_class) AS quality_class,
    (cy.actor_id IS NOT NULL) AS is_active,
    COALESCE(
            cy.year,
            CAST(ly.current_year AS INTEGER) + 1
        ) AS current_year
FROM
    last_year ly
    FULL OUTER JOIN current_year cy
    ON ly.actor_id = cy.actor_id