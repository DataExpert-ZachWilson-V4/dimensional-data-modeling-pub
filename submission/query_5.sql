-- Insert data into SCD (type 2) table for actors data by tracking year over year changes; and inserting data for one year at a time
INSERT INTO
    raviks90.actors_history_scd
WITH
    -- select the previous year data; will be empty for the first insertion
    prev_year AS  (
        SELECT
            *
        FROM
            raviks90.actors_history_scd
        WHERE
            current_year = 1913
    ),
    -- select the current year data
    curr_year AS  (
        SELECT
            actor_id,
            quality_class,
            is_active,
            current_year
        FROM
            raviks90.actors
        WHERE
            current_year = 1914  
    ),
    -- Join previous year with current year to determine the changes to attribues quality_class and is_active
    joined AS  (
        SELECT
            COALESCE(p.actor_id, c.actor_id) AS  actor_id,
            p.quality_class AS  prev_quality_class,
            c.quality_class AS  curr_quality_class,
            p.is_active AS  prev_is_active,
            c.is_active AS  curr_is_active,
            -- create "changed" flag to identy if any change happened
            CASE
                WHEN p.quality_class != c.quality_class
                OR p.is_active != c.is_active THEN   1 
                WHEN p.quality_class = c.quality_class
                AND  p.is_active = c.is_active THEN  0 
            END AS  changed,
            p.start_date AS  prev_start_date,
            p.end_date AS  prev_end_date,
            COALESCE(p.current_year + 1, c.current_year) AS  current_year -- When actor does not have a film reased in the current year, derives current year by(previous year + 1)
        FROM
            prev_year p
            FULL OUTER JOIN curr_year c on p.actor_id = c.actor_id
            AND  p.end_date + 1 = c.current_year -- additional check to make sure we are comparing with only previous year
    ),
    scd_info AS(
        SELECT
            actor_id,
            current_year,
           -- create an array with attributes and start/end dates based on changes
           -- changed flag can be NULL since there wont be anything to compare for the first time 
            CASE
                WHEN changed IS NULL THEN ARRAY[
                    CAST(  
                        ROW(  
                            COALESCE(prev_quality_class, curr_quality_class),
                            COALESCE(prev_is_active, curr_is_active),
                            COALESCE(prev_start_date, current_year),
                            COALESCE(prev_end_date, current_year)
                        ) AS  ROW(  
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]

                -- when nothing changed take the previous values, this will extend the end date further
                WHEN changed = 0 THEN  ARRAY[
                    CAST(  
                        ROW(  
                            prev_quality_class,
                            prev_is_active,
                            prev_start_date,
                            current_year
                        ) AS  ROW(  
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]
                -- when changed, we need two array elements one with previous attributes & start/end dates AND one with new attributes with new start and end dates
                WHEN changed = 1 THEN  ARRAY[
                    CAST(  
                        ROW(  
                            prev_quality_class,
                            prev_is_active,
                            prev_start_date,
                            prev_end_date
                        ) AS  ROW(  
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    ),
                    CAST(  
                        ROW(  
                            curr_quality_class,
                            curr_is_active,
                            current_year,
                            current_year
                        ) AS  ROW(  
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]
            END AS  scd_arr
        FROM
            joined
    )
  -- select the scd structure by exploding array elements
SELECT
    actor_id,
    scd_arr.quality_class,
    scd_arr.is_active,
    scd_arr.start_date,
    scd_arr.end_date,
    current_year
FROM
    scd_info
    CROSS JOIN UNNEST (scd_arr) AS  scd_arr
