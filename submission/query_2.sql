INSERT INTO
    anjanashivangi.actors WITH last_year AS (
        -- This CTE grabs existing records from target table. 
        -- 'currenty_year' is set to 1913 for the first run. This will be empty in the first run.
        -- 'currenty_year' should be increased sequentially for each incremental run. 
        SELECT
            *
        FROM
            anjanashivangi.actors
        WHERE
            current_year = 1913
    ),
    this_year AS (
        -- This CTE emulates the incoming data for the current year. 
        -- It groups the data to bring it to the same grain as target table (actors)
        -- 'year' value is harcoded to 1914 for the first run. It should be increased sequentially for each incremental run.
        SELECT
            actor,
            actor_id,
            array_agg(ROW(film, votes, rating, film_id)) as films,
            -- array_agg because an actor can have multiple films in a year
            AVG(rating) as average_rating,
            -- calculate average rating of all the movies for an actor in current year
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
SELECT
    COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actor_id, ty.actor_id) as actor_id,
    CASE
        WHEN ty.films is NULL THEN ly.films -- if actor not in current year then pull last year's data
        WHEN ty.films is NOT NULL and ly.films is NULL THEN ty.films -- Use this year data if it's brand new actor
        WHEN ty.films is NOT NULL and ly.films is NOT NULL THEN ty.films || ly.films -- if this year actor already existed then concat films
    END as films,
    IF(
        -- if current year average_rating is not NULL then compute quality_class else use last year's quality_class
        ty.average_rating is not NULL,
        CASE
            WHEN ty.average_rating > 8 THEN 'star'
            WHEN ty.average_rating > 7
            and ty.average_rating <= 8 THEN 'good'
            WHEN ty.average_rating > 6
            and ty.average_rating <= 7 THEN 'average'
            ELSE 'bad'
        END,
        ly.quality_class
    ) as quality_class,
    ty.films is not NULL as is_active,
    -- If actor has films in this year's data then he is active
    coalesce(ty.current_year, ly.current_year + 1) as current_year
FROM
    last_year ly FULL
    OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id