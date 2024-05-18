INSERT INTO actors_history_scd
WITH last_year_actors_scd AS (  -- Gets the data of actors from the previous years stored in local actors table
        SELECT *
        FROM actors_history_scd
        WHERE current_year = 2011 -- previous reporting year
    ),
    this_year_actors AS (
        -- Selects actors, their IDs, film year, average ratings, and aggregates films into arrays
        SELECT *
        FROM actors
        WHERE current_year = 2012 -- Consider data up to the year 2011
    ),
    combined_scd AS (
        -- Combines data from the last year's actors SCD and current year's actors
        SELECT COALESCE(ls.actor, ts.actor) AS actor, -- handles null values in actor
            COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
            COALESCE(ls.start_date, ts.current_year) AS start_date,-- handles null start date
            COALESCE(ls.end_date, ts.current_year) AS end_date, -- handles null end date
            CASE
                WHEN ls.is_active <> ts.is_active
                OR ls.quality_class <> ts.quality_class
                OR ls.average_rating <> ts.average_rating THEN 1 -- Flag record as changed if any of these conditions are met such as a change in active, quality class or rating
                WHEN ls.is_active = ts.is_active
                OR ls.quality_class = ts.quality_class
                OR ls.average_rating = ts.average_rating THEN 0 -- Flag record as unchanged if these conditions are met such as a change in active, quality class or rating
            END AS record_changed,
            ls.is_active AS is_active_last_year,
            ts.is_active AS is_active_this_year,
            ls.average_rating AS average_rating_last_year, -- defining last years average rating as such
            ts.average_rating AS average_rating_this_year, -- defining this years average rating as such
            ls.quality_class AS quality_class_last_year, -- defining last years quality class as such
            ts.quality_class AS quality_class_this_year,-- defining this years quality class as such
            2012 as current_year -- Set the current year for the records being processed
        FROM last_year_actors_scd ls
            FULL OUTER JOIN this_year_actors ts ON ls.actor_id = ts.actor_id --get all records from both last year and this year queries
            AND ls.end_date + 1 = ts.current_year -- Join condition to match records between last year and current year
    ),
    changed AS (
        SELECT actor,
            actor_id,
            current_year,
            CASE
                WHEN record_changed = 0 THEN ARRAY [ROW(
                is_active_last_year, average_rating_last_year, quality_class_last_year, start_date, end_date + 1)] -- if record is unchanged then populate array
                WHEN record_changed = 1 THEN ARRAY [ROW(
                is_active_last_year, average_rating_last_year, quality_class_last_year, start_date, end_date),
                ROW(
                is_active_this_year, average_rating_this_year, quality_class_this_year, current_year, current_year)] -- if record is changed then keep the last and current years records
                WHEN record_changed IS NULL THEN ARRAY [ROW(
                COALESCE(is_active_last_year, is_active_this_year),
                COALESCE(average_rating_last_year, average_rating_this_year),
                COALESCE(quality_class_last_year, quality_class_this_year),
                start_date, end_date)] -- if the record change is null then coalesce data
            END AS record_changed_array
        FROM combined_scd
    )
SELECT actor,
    actor_id,
    arr.is_active, -- get the is_active element from the array
    arr.average_rating,  -- get the average_rating element from the array
    arr.quality_class,  -- get the quality_class element from the array
    arr.start_date,  -- get the start_date element from the array
    arr.end_date,  -- get the end_date element from the array
    current_year
FROM changed
    CROSS JOIN UNNEST(record_changed_array) AS arr (
        is_active,
        average_rating,
        quality_class,
        start_date,
        end_date
    ) -- unnest the array into the defined columns
