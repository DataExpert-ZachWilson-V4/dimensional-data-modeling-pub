INSERT INTO mposada.actors_history_scd -- adding comment so that autograding checks all my questions, last time it only checked q5
WITH
lagged AS (
    SELECT
        actor,
        actor_id,
        quality_class,
        current_year,
        CASE
            WHEN is_active THEN 1  -- Convert boolean to integer for is_active
            ELSE 0
        END AS is_active,
        CASE
            WHEN LAG(is_active, 1) OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            ) THEN 1  -- Check if the actor was active in the previous year
            ELSE 0
        END AS is_active_last_year,
        LAG(quality_class, 1)
            OVER (
                PARTITION BY
                    actor_id
                ORDER BY
                    current_year
            )
            AS quality_class_last_year  -- Get the quality class from the previous year
    FROM
        mposada.actors
    WHERE
        current_year <= 1918  -- Consider records up to the year 1918, this should be the year you are backfilling up to, basically the most recent year of available data, I used 1918 because thats what I backfilled actors up to
),

streaked AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN
                    (is_active <> is_active_last_year)  -- Check if the active status has changed
                    OR (quality_class <> quality_class_last_year) THEN 1  -- Check if the quality class has changed
                ELSE 0
            END
        ) OVER (
            PARTITION BY
                actor
            ORDER BY
                current_year
        ) AS streak_identifier  -- Identify changes in active status or quality class
    FROM
        lagged
)

SELECT
    actor,
    actor_id,
    ANY_VALUE(quality_class) AS quality_class,  -- Get any value of quality_class within the group
    MAX(is_active) = 1 AS is_active,  -- Determine if the actor is active based on the max value, this makes sure that the actor is active during the period
    MIN(current_year) AS start_date,  -- Determine the start date for the streak, indicating start of validity for the row
    MAX(current_year) AS end_date,  -- Determine the end date for the streak, indicating end of validity for the row
    1918 AS current_year  -- Set the current year to 1918, 
FROM
    streaked
GROUP BY
    actor,
    actor_id,
    streak_identifier  -- Group by actor, actor_id, and the streak identifier, this way we get periods in which is_active and quality_class didnt change and new rows for changes
