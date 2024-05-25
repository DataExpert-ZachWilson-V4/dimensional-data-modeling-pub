-- This query inserts data into the table "akp389.actors_history_scd" using a WITH clause for better readability and organization.

-- The first common table expression (CTE) "last_year_scd" selects records from "akp389.actors_history_scd" for the year 2001.
INSERT INTO akp389.actors_history_scd
WITH last_year_scd AS (
    SELECT
        *
    FROM
        akp389.actors_history_scd
    WHERE
        current_year = 2000
),

-- The second CTE "this_year_scd" calculates the quality class and sets the activity status for actors in the year 2002.
-- It uses a CASE statement to determine the quality class based on average ratings from the "bootcamp.actor_films" table.

this_year_scd AS (
    SELECT
        actor,
        actor_id,
        YEAR,
        CASE
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 THEN 'good'
            WHEN AVG(rating) > 6 THEN 'average'
            ELSE 'bad'
        END quality_class,
        TRUE AS is_active
    FROM
        bootcamp.actor_films
    WHERE
        YEAR = 2001
    GROUP BY
        actor,
        actor_id,
        YEAR
),

-- The third CTE "combined" joins data from the "last_year_scd" and "this_year_scd" CTEs to create a unified dataset.
-- It uses a FULL OUTER JOIN on actor ID and year, ensuring all records from both CTEs are included.

combined AS (
    SELECT
        l.actor l_actor,
        t.actor t_actor,
        l.actor_id l_actor_id,
        t.actor_id t_actor_id,
        l.is_active l_is_active,
        -- captures activity as false for actors who don't have movies in the new year
        COALESCE(t.is_active, FALSE) t_is_active,
        l.start_date p_start_date,
        l.end_date p_end_date,
        l.current_year l_current_year,
        l.quality_class l_quality_class,
        t.quality_class t_quality_class
    FROM
        last_year_scd l
    FULL OUTER JOIN
        this_year_scd t ON l.actor_id = t.actor_id
        AND l.end_date + 1 = t.year
),

-- The fourth CTE "changes" determines changes in quality class and activity status between the previous and current years.
-- It constructs an array of records representing these changes.

changes AS (
    SELECT
        COALESCE(t_actor, l_actor) AS actor,
        COALESCE(t_actor_id, l_actor_id) AS actor_id,
        CASE
          -- case for extending the same row: no change in status and quality class
            WHEN l_is_active = t_is_active
            AND l_quality_class = COALESCE(t_quality_class, l_quality_class)
            AND p_end_date = l_current_year THEN ARRAY[
                CAST(
                    ROW(
                        l_quality_class,
                        l_is_active,
                        p_start_date,
                        l_current_year + 1,
                        l_current_year + 1
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INT,
                        end_date INT,
                        current_year INT
                    )
                )
            ]
            -- case for getting additional row: change in either status or quality class
            WHEN NOT (
                l_is_active = t_is_active
                AND l_quality_class = COALESCE(t_quality_class, l_quality_class)
            )
            AND p_end_date = l_current_year THEN ARRAY[
                CAST(
                    ROW(
                        l_quality_class,
                        l_is_active,
                        p_start_date,
                        l_current_year,
                        l_current_year + 1
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INT,
                        end_date INT,
                        current_year INT
                    )
                ),
                CAST(
                    ROW(
                        COALESCE(t_quality_class, l_quality_class),
                        t_is_active,
                        l_current_year + 1,
                        l_current_year + 1,
                        l_current_year + 1
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INT,
                        end_date INT,
                        current_year INT
                    )
                )
            ]
            -- last condition : this leaves us with closed records
            ELSE ARRAY[
                CAST(
                    ROW(
                        l_quality_class,
                        l_is_active,
                        p_start_date,
                        p_end_date,
                        l_current_year + 1
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INT,
                        end_date INT,
                        current_year INT
                    )
                )
            ]
        END AS change_array
    FROM
        combined
)

-- The final SELECT statement extracts data from the "changes" CTE by unnesting the array of change records.

SELECT
    actor,
    actor_id,
    arr.quality_class,
    arr.is_active,
    arr.start_date,
    arr.end_date,
    arr.current_year
FROM
    changes
CROSS JOIN
    UNNEST (change_array) AS arr