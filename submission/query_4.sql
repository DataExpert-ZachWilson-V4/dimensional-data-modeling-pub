-- This query is used to create a slowly changing dimension (type 2) table for actors' data in a single query tracking changes in quality_class and is_active
insert into
    sarneski44638.actors_history_scd
with
    track_change as (
        select
            actor_id,
            quality_class,
            coalesce(
                LAG(quality_class) over (
                    partition by
                        actor_id
                    order by
                        current_year
                ) != quality_class,
                false
            ) as quality_class_change, -- boolean for if quality class changed; Compare lagged quality class (offset 1) with this year's. If they are different then there was a change.
            --  Note first lagged value will be null, but this is not not a change in quality class so filled with false via coalesce
            is_active,
            coalesce(
                LAG(is_active) over (
                    partition by
                        actor_id
                    order by
                        current_year
                ) != is_active,
                false
            ) as is_active_change, -- boolean for if `is_active` changed; Compare lagged by 1 `is_active` value with this year's. If they are different then there was a change.
            --  Note first lagged value will be null, but this is not not a change in `is_active` so filled with false via coalesce
            current_year
        from
            sarneski44638.actors
        where
            current_year <= 2021 -- max year in actor table 2021 (since based off `actor_films` table)
    ),
    streak as (
        select
            actor_id,
            quality_class,
            is_active,
            -- create a streak identifier using a running sum. Will create a new streak identifier every time either the quality class changes or `is_active` changes (or both)
            sum(
                case
                    when quality_class_change = true -- case where either quality class or `is_active` status changed => increment by 1 to start new streak
                    or is_active_change = true then 1
                    when quality_class_change = false
                    and is_active_change = false then 0
                end
            ) over (
                partition by
                    actor_id
                order by
                    current_year
            ) as streak_identifier,
            current_year
        from
            track_change
    )
select
    actor_id,
    max(quality_class) as quality_class, -- within a streak (same value of streak_identifier) will have same quality_class value
    max(is_active) as is_active, -- within a streak (same value of streak_identifier) will have same is_active value
    min(current_year) as start_date, -- year of start of streak
    max(current_year) as end_date, -- year of end of streak
    2021 as current_year -- since there isn't more current data in table stop here
from
    streak
group by
    actor_id,
    streak_identifier