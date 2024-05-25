-- Write an "incremental" query that populates a single year's worth of the actors_history_scd table 
-- by combining the previous year's SCD data with the new incoming data from the actors table for this year.

-- Insert into actors_history_scd table
Insert into actors_history_scd 
-- Common Table Expressions (CTEs) to fetch data from the previous year's SCD and the current year's actors table
WITH last_year as 
(
    -- Select data from actors_history_scd for the previous year
    select *
    from actors_history_scd
    where current_year = 2021
),
this_year AS 
(
    -- Select data from actors table for the current year
    SELECT actor,
        actor_id,
        quality_class,
        is_active,
        current_year
    FROM actors
    WHERE current_year = 2022
)
-- Combine data from the previous year and the current year
select COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actor_id, ty.actor_id) as actor_id,
    COALESCE(ly.quality_class, ty.quality_class) as quality_class,
    COALESCE(ly.is_active, ty.is_active) as is_active,
    COALESCE(ly.start_date, ty.current_year) as start_date,
    COALESCE(ty.current_year, ly.end_date) as end_date,
    COALESCE(ly.current_year + 1, ty.current_year) AS current_year
from last_year ly
    full outer join this_year ty on ly.actor_id = ty.actor_id  -- We use FULL OUTER JOIN to keep both records from SCD table and new including data from actors table
    -- Handle cases where quality class didn't change
    and COALESCE(ly.quality_class, 'null') = COALESCE(ty.quality_class, 'null')
    -- Ensure continuity of records by checking end date and current year
    and ly.end_date + 1 = ty.current_year
order by actor,
    start_date