INSERT INTO actors WITH cy as ( 
        -- current year data coming from 'snapshot' table
        SELECT actor,
            actor_id,
            year,
            ARRAY_AGG(ROW(film, film_id, year, votes, rating)) AS films, 
            -- arrays with all the films actor took part in during given year
            case
                when AVG(rating) > 8 then 'star'
                when AVG(rating) > 7 then 'good'
                when AVG(rating) > 6 then 'average'
                else 'bad'
            END as quality_class 
            -- text representation of average rating of films actor took part in
        FROM bootcamp.actor_films
        WHERE year = 1914
        GROUP BY actor_id,
            actor,
            year
    ),
    ly as ( 
    -- previous year data coming from already data already inserted in cumulative table
        SELECT actor,
            actor_id,
            films,
            quality_class,
            is_active,
            current_year
        FROM actors
        WHERE current_year = 1913
    )
SELECT COALESCE(cy.actor, ly.actor) as actor, 
    -- coalesce covers all cases - when current year data does not exist it takes value from previous year (actor did not take part in any films during current year)
    -- when previous year data does not exist ('new' actor) it takes it's value from previous year
    COALESCE(cy.actor_id, ly.actor_id) as actor_id,
    CASE
        WHEN cy.films IS NULL THEN ly.films
        -- no new films, we can keep already exising films table
        WHEN cy.films IS NOT NULL
        AND ly.films IS NULL THEN cy.films
        -- 'new' actor, create a new table with current year films
        WHEN cy.films IS NOT NULL
        AND ly.films IS NOT NULL THEN cy.films || ly.films
        -- actor already has some films he/she took part of, add current year films
    END AS films,
    COALESCE(cy.quality_class, ly.quality_class) as quality_class,
    CASE
        WHEN cy.films IS NULL THEN False
        -- actor goes inactive if there are no films for current year
        ELSE True
    END as is_active,
    COALESCE(cy.year, ly.current_year + 1) as current_year
    -- if there is no data for current year we still want to increment year for this partition
FROM cy
    FULL OUTER JOIN ly ON cy.actor_id = ly.actor_id
    -- full outer join to cover cases when
    -- 1. actor is 'new' - not present in accumulated table
    -- 2. actor goes inactive - not present in current year table