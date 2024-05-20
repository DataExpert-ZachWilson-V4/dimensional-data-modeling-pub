INSERT INTO ovoxo.actors
WITH 
    -- select records for each actor from previous year
    previous_year AS (
        SELECT * 
        FROM ovoxo.actors
        WHERE current_year = 2013
    ),

    -- select records for each actor from current year
    -- sub query creates a row/array of film details for each actor and film id 
    -- group by actor and year to rollup grain from actor_id, film_id to actor_id, this combines multiple film details for a single actor
    ----- find average film rating by actor and year
    current_year AS (
        SELECT actor, 
            actor_id, 
            year, 
            ARRAY_AGG(film_details) AS films,
            AVG(rating) AS avg_rating
        FROM (
            SELECT *,
                ROW(year, film, votes, rating, film_id) as film_details
            FROM bootcamp.actor_films
            WHERE year = 2014
            )
        GROUP BY actor, actor_id, year
    )


SELECT COALESCE(py.actor, cy.actor) AS actor,
    COALESCE(py.actor_id, cy.actor_id) AS actor_id,
    CASE
        WHEN cy.year IS NULL THEN py.films -- handle cases where there are no records for current year but records exists in previous year - use the array from py
        WHEN cy.year IS NOT NULL -- handle cases where there are records for current year 
            AND py.films IS NULL -- but no records exists in previous year 
            THEN cy.films -- use the array from cy as actor's record
        WHEN cy.year IS NOT NULL -- handle cases where there are records for current year
            AND py.films IS NOT NULL -- and records for py
            THEN cy.films || py.films -- concat both arrays from cy and py, extend existing actor records
    END AS films,
    CASE 
        WHEN cy.avg_rating IS NULL THEN py.quality_class  -- if no rating in current year, use quality_class from previous year. Makes sense to carry over the quality class as it neither improved or diminished
        WHEN cy.avg_rating > 8 THEN 'star'
        WHEN cy.avg_rating > 7 AND cy.avg_rating <= 8 THEN 'good'
        WHEN cy.avg_rating > 6 AND cy.avg_rating <= 7 THEN 'average'
        WHEN cy.avg_rating <= 6 THEN 'bad'
    END AS quality_class,
    cy.year IS NOT NULL AS is_active,
    COALESCE(cy.year, py.current_year + 1) as current_year
FROM previous_year py
FULL OUTER JOIN current_year cy 
    ON py.actor_id = cy.actor_id
