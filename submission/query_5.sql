INSERT INTO martinaandrulli.actors_history_scd 
WITH
    --- Last year data are taken from the table itself for performing a join with the new data at a later stage. 
    last_year_scd AS ( 
        SELECT *
        FROM martinaandrulli.actors_history_scd
        -- Last year to select from the past data
        WHERE current_year = 1918 
    ),
    -- "New" data are grabbed from the actors table
    current_year_scd AS ( 
        SELECT *
        FROM martinaandrulli.actors
        -- Year to add to the existing data
        WHERE current_year = 1919 
    ),
    -- To understand which rows will be affected by the addition of a new year, it is required to know the behaviour of each actor in the new year (active or not) 
    active_info_cte AS ( 
        SELECT
            -- If the actor is present as "old" record, we can use his actor_id, but if the actor is present only as "new" record, we still need to fetch his actor_id
            COALESCE(ly.actor_id, cy.actor_id) AS actor_id, 
            -- If the actor is present as "old" record, we can use his quality_class, but if the actor is present only as "new" record, we still need to fetch his quality_class
            COALESCE(ly.quality_class, cy.quality_class) as quality_class,
            -- If the actor is present as "old" record, we can use his start_date, but if the actor is present only as "new" record, we need to use the current_year as start_date of his timerange
            COALESCE(ly.start_date, cy.current_year) AS start_date,
            -- If the actor is present as "old" record, we can use his end_date, but if the actor is present only as "new" record, we need to use the current_year as end_date of his timerange
            COALESCE(ly.end_date, cy.current_year) AS end_date, 
            CASE
                -- When his old "is_active" does not match the new "is_active", it means that the actor has changed his status (from active to inactive or viceversa)
                WHEN ly.is_active <> cy.is_active THEN 1 
                -- When his old "is_active" matches the new "is_active", it means that his status is stable (as active or inactive)
                WHEN ly.is_active = cy.is_active THEN 0 
            END AS change_status,
            -- If the actor has an old record (belonging to last year), his status is assigned as "is_active_last_year"
            ly.is_active AS is_active_last_year,
            -- If the actor has a new record (belonging to this year), his status is assigned as "is_active_this_year"
            cy.is_active AS is_active_this_year,
            -- current year that has to match the current_year of the 'current_year_scd' filter
            1919 AS current_year 
        FROM
        last_year_scd AS ly
        -- A FULL outer join is required to include also the "new" actors that were not present in the old data
        FULL OUTER JOIN current_year_scd AS cy ON ly.actor_id = cy.actor_id 
        -- Only those rows that can be affected by the join are considered, so those that have a time range that ends with the previous year wrt the year to join. Indeed, "old" data (i.e. having a timerange that ends at least two years before the "new" year) will anyway not change.
        AND ly.end_date + 1 = cy.current_year 
    ),
    -- Final table that checks if changes have been happened or not on those rows affected by the additional year and updates their values accordingly. "Old" data are re-inserted with an updated current_year value. 
    change_info_cte AS ( 
        SELECT
            actor_id,
            quality_class,
            CASE
                -- This case cover if the new data didn't generate a change in the status of the actor (still active or still inactive). In this case, one row is added for the actor and the timerange is increased by having a end_date equal to the "old" year + 1.
                WHEN change_status = 0 THEN ARRAY[ 
                                                CAST(
                                                    ROW(is_active_last_year, start_date, end_date + 1 ) AS 
                                                        ROW (is_active BOOLEAN, start_date INTEGER, end_date INTEGER)
                                                    )
                                                ]
                -- This case cover if the new data has generated a change in the status of the actor (from active to inactive or viceversa). In this case, two rows are generated: One with the "last" year information and another one with the "current_year"
                WHEN change_status = 1 THEN ARRAY[ 
                                                CAST(
                                                    ROW(is_active_last_year, start_date, end_date) AS 
                                                    ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER)
                                                ),
                                                CAST(
                                                    ROW(is_active_this_year, current_year, current_year) AS 
                                                    ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER)
                                                )
                                                ]
                -- This case cover the "old" data that has not been affected by the addition of the new data - i.e. Those data where the end_date was at least two years before the current one.
                WHEN change_status IS NULL THEN ARRAY[ 
                                                CAST(
                                                    ROW( COALESCE(is_active_last_year, is_active_this_year), start_date, end_date) AS 
                                                    ROW(is_active BOOLEAN, start_date INTEGER, end_date INTEGER)
                                                )
                                                ]
            END AS change_status_array,
            current_year
        FROM
        active_info_cte
    )
SELECT
  actor_id,
  quality_class,
  cs_arr.is_active,
  cs_arr.start_date,
  cs_arr.end_date,
  current_year
FROM
  change_info_cte
  CROSS JOIN UNNEST (change_status_array) AS cs_arr -- Unpack each array value to get a column in the final table
  