INSERT INTO srik1981.actors 
WITH last_year AS (
    -- CTE for previous year data, this will be empty for the very first time
    SELECT
        *
    FROM srik1981.actors
    WHERE current_year = 1913
),
current_year AS (
    -- CTE for current year data
    SELECT
        actor,
        actor_id,
        array_agg(
            CAST(
                ROW(year, film, votes, rating, film_id) AS ROW(
                    year INTEGER,
                    film VARCHAR,
                    votes INTEGER,
                    rating DOUBLE,
                    film_id VARCHAR
                )
            )
        ) AS films,
        year AS current_year,
        AVG(rating) AS avg_rating
    FROM bootcamp.actor_films
    WHERE year = 1914 -- Update this everytime a new year's worth of data has to be loaded
    GROUP BY
        actor,
        actor_id,
        year
),
result_set AS ( -- Calculate the result set to be loaded into the cummulative dimension table
    SELECT
        COALESCE(ly.actor, cy.actor) AS actor,
        COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
        CASE
            WHEN cy.films IS NULL THEN ly.films
            WHEN ly.films IS NULL THEN cy.films
            WHEN cy.films IS NOT NULL
            AND ly.films IS NOT NULL THEN cy.films || ly.films
        END AS films,
        CASE
            WHEN avg_rating > 8 THEN 'star'
            WHEN avg_rating > 7
            AND avg_rating <= 8 THEN 'good'
            WHEN avg_rating > 6
            AND avg_rating <= 7 THEN 'average'
            WHEN avg_rating <= 6 THEN 'bad'
            ELSE ly.quality_class
        END AS quality_class,
        CASE
            WHEN cy.actor_id IS NOT NULL THEN true
            ELSE false
        END AS is_active,
        COALESCE(cy.current_year, ly.current_year + 1) AS current_year
    FROM last_year ly FULL
    OUTER JOIN current_year cy ON ly.actor = cy.actor
)
-- Select the columns for clarifty
SELECT
    rs.actor,
    rs.actor_id,
    rs.films,
    rs.quality_class,
    rs.is_active,
    rs.current_year
FROM result_set rs