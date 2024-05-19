INSERT INTO raviks90.actors
    -- select previous year data, would return empty for the first year
WITH
    prev_year as (
        SELECT
            *
        FROM
            raviks90.actors
        WHERE
            current_year = 1913
    ),
    -- select current year data
    curr_year as (
        SELECT
            actor,
            actor_id,
            max(year) as year,
            -- aggregates rows into array. One array element represent attributes of one film of that actor
            array_agg(ROW(film_id, film, votes, rating, year)) as films,
            -- assign quality class for the actor based on avergae rating of his/her films
            CASE
                WHEN AVG(rating) > 8 then 'star'
                WHEN AVG(rating) > 7
                AND AVG(rating) <= 8 THEN 'good'
                WHEN AVG(rating) > 6
                AND AVG(rating) <= 7 THEN 'average'
                WHEN AVG(rating) <= 6 then 'bad'
            END AS quality_class
        FROM
            bootcamp.actor_films
        WHERE
            year = 1914
        GROUP BY
            actor,
            actor_id
    )
    -- compares previous year with current year to derive the latest info for the current year
SELECT
    COALESCE(p.actor, c.actor) as actor,
    COALESCE(p.actor_id, c.actor_id) as actor_id,
    CASE
        WHEN c.actor_id IS NOT NULL
        AND p.actor_id is NULL THEN c.films
        WHEN c.actor_id IS NOT NULL
        AND p.actor_id is NOT NULL THEN p.films || c.films
        WHEN c.actor_id IS NULL THEN p.films
    END AS films,
    CASE
        WHEN c.actor_id IS NOT NULL THEN c.quality_class
        WHEN c.actor_id IS NULL THEN p.quality_class
    END AS quality_class,
    c.actor_id IS NOT NULL as is_active,
    -- When actor does not have a film reased in the current year, derives current year by(previous year + 1)
    COALESCE(c.year, p.current_year + 1) as current_year
FROM
    curr_year c
    FULL OUTER JOIN prev_year p ON c.actor_id = p.actor_id
