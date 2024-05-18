-- Replace 'current_year_value' with the year being processed, such as 1923 for last_year and 1924 for current_year_temp

-- Clear records for the current year to ensure idempotence
-- DELETE FROM jlcharbneau.actors WHERE current_year = 1916

-- Insert results into our actors table
INSERT INTO jlcharbneau.actors

-- Create the "last year" CTE
WITH last_year AS (
    SELECT actor,
           actor_id,
           films,
           quality_class,
           is_active,
           current_year
    FROM jlcharbneau.actors
    WHERE current_year = 1921 - 1
),
-- Define a temporary table for the current year
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
WHERE year = 1921
GROUP BY actor, actor_id, year
    ),
-- Define our current year CTE pulling in our results from temp,
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

-- Finally, get our results so they can be inserted into jlcharbneau.actors
--   coalesce the values, defaulting to those from last year
SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
    CASE
        WHEN ly.films IS NOT NULL AND cy.films IS NOT NULL THEN cy.films || ly.films
        WHEN ly.films IS NULL THEN cy.films
        WHEN cy.films IS NULL THEN ly.films
        END AS films,
    COALESCE(cy.quality_class, ly.quality_class) AS quality_class,
    (cy.actor_id IS NOT NULL) AS is_active,
    cy.year AS current_year -- Ensure that the current year is appropriately set from the current_year CTE
FROM
    last_year ly
        FULL OUTER JOIN current_year cy
                        ON ly.actor_id = cy.actor_id