-- Write a query that populates the actors table one year at a time.
INSERT INTO actors
-- CTE to hold last years data
WITH last_year AS (
    SELECT
        *
    FROM
        actors
    WHERE
        current_year = 1913
),
-- CTE to hold this years data
current_year AS (
    SELECT
        actor,
        actor_id,
        ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
        CASE 
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 THEN 'good'
            WHEN AVG(rating) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        actor_id IS NOT NULL AS is_active,
        year as current_year
    FROM
        bootcamp.actor_films
    WHERE
        year = 1914
    GROUP BY 
        actor,
        actor_id,
        year
)
-- Generate final data
SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
    CASE
        -- Last years' films IS NULL, choose current year films only
        WHEN ly.films IS NULL THEN cy.films
        -- Current years' films IS NOT NULL, load the films in reverse chronological order
        WHEN cy.films IS NOT NULL THEN cy.films || ly.films
        -- Current years' films IS NULL
        ELSE ly.films
    END AS films,
    COALESCE(ly.quality_class, cy.quality_class) AS quality_class,
    cy.is_active IS NOT NULL AS is_active,
    COALESCE(cy.current_year, ly.current_year+1) AS current_year
FROM
    last_year ly 
FULL OUTER JOIN current_year cy 
ON (ly.actor = cy.actor)