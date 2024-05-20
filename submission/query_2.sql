-- Insert data into the halloweex.actors table
INSERT INTO actors
WITH
    -- Step 1: Retrieve data from the last year (2020)
    last_year AS (
        SELECT
            actor,
            actor_id,
            films,
            quality_class,
            current_year
        FROM
            halloweex.actors
        WHERE
            current_year = 2020
    ),

    -- Step 2: Aggregate data for the current year (2021) from the actor_films table
    this_year AS (
        SELECT
            actor,
            actor_id,
            -- Aggregate films details into an array for each actor
            array_agg(ROW(year, film, votes, rating, film_id)) AS films,
            -- Determine quality_class based on the average film rating
            CASE
                WHEN AVG(rating) > 8 THEN 'star'
                WHEN AVG(rating) > 7 THEN 'good'
                WHEN AVG(rating) > 6 THEN 'average'
                ELSE 'bad'
            END AS quality_class,
            year
        FROM
            bootcamp.actor_films
        WHERE
            year = 2021
        GROUP BY
            actor,
            actor_id,
            year
    )

-- Step 3: Combine last year's data with this year's data and insert into the halloweex.actors table
SELECT
    -- Use actor from either last year or this year
    COALESCE(ly.actor, ty.actor) AS actor,
    -- Use actor_id from either last year or this year
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    -- Combine films from last year and this year
    CASE
        WHEN ly.films IS NULL THEN ty.films
        WHEN ty.films IS NOT NULL THEN ly.films || ty.films
        ELSE ly.films
    END AS films,
    -- Use quality_class from this year if available, otherwise use last year's
    COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
    -- Determine if the actor is active
    COALESCE(ty.actor_id, ly.actor_id) IS NOT NULL AS is_active,
    -- Use year from this year or increment last year's current_year
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
    last_year ly
    -- Full outer join to combine data from last year and this year
    FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
    AND ly.current_year = ty.year - 1;
