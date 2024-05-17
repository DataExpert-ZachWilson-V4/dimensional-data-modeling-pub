-- =============================================
-- 1) Do I need to run this query one year at the time?
-- No, you can have one query for all of the actors and the years
-- 2) What is the minimum year?
-- 1914
-- 3) How to validate the population query is working as expected?
-- Grab 1 actor that has intermitent work and validate manually
-- 4) Which actor did you choose?
--  Tom Holland. Validation Went well


-- Define a series of years for which we need to generate data
INSERT INTO andreskammerath.actors
WITH years AS (
    SELECT year
    FROM UNNEST(sequence(1914, 2021)) AS t(year)
),
-- Aggregate film data for actors, considering films up to and including each given year
ActorAggregates AS (
    SELECT
        y.year AS current_year,
        f.actor,
        f.actor_id,
        array_agg(ROW(f.film, CAST(f.votes AS bigint), CAST(f.rating as DOUBLE),f.film_id) ORDER BY f.year) AS films,
        AVG(f.rating) AS avg_rating,
        MAX(f.year) AS last_active_year
    FROM
        years y
    CROSS JOIN
        bootcamp.actor_films f
    WHERE
        f.year <= y.year
    GROUP BY
        y.year,
        f.actor,
        f.actor_id
),
final_q as (SELECT
    actor,
    actor_id,
    films,
    CASE
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 THEN 'good'
        WHEN avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    (last_active_year = current_year) AS is_active,
    CAST(current_year as INT)
FROM
    ActorAggregates)
SELECT * FROM final_q