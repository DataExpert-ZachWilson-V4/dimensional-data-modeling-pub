-- QUERY 2 ASSIGNMENT

-- Cumulative Table Computation Query (query_2)
-- Write a query that populates the actors table one year at a time.
INSERT INTO vzucher.actors
SELECT
    actor_id,  
    actor,
    ARRAY_AGG(
        ROW(
            film_id,
            film, 
            votes, 
            rating
        )
    ) AS films,
    CASE 
        WHEN AVG(rating) > 8 THEN 'star'
        WHEN AVG(rating) > 7 THEN 'good'
        WHEN AVG(rating) > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    MAX(year) = EXTRACT(YEAR FROM CURRENT_DATE) AS is_active,
    year AS current_year
FROM
    bootcamp.actor_films
GROUP BY
    actor_id, actor, year
-- ORDER BY current_year DESC

-- The following query is designed to populate the actors table one year at a time.
-- It groups the actors by year and calculates the average rating for each actor based on the films they appeared in that year.
-- The quality_class field is then assigned based on the average rating, and the is_active field is determined based on whether the actor's latest year matches the current year.
-- The current_year field is also included to track the year for each record.

