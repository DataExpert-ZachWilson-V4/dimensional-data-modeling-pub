-- This query is used to incrementally backfill a slowly changing dimension (type 2) table for actors' data tracking changes in quality_class and is_active
-- vary line 13 (current_year) from 1913 to 2020 and line 24 (current_year) from 1914 to 2021 to fully populate scd table with available data
insert into
    sarneski44638.actors_history_scd
with
    -- start with data from scd table from previous year
    prev_year as (
        select
            *
        from
            sarneski44638.actors_history_scd
        where
            current_year = 2020
    ),
    curr_year as (
        select
            actor_id,
            quality_class,
            is_active,
            current_year
        from
            sarneski44638.actors
        where
            current_year = 2021 -- will be current_year + 1 where current_year from prev_year CTE
    ),
    -- join prev_year & curr_year data to determine if dimensions changed or not; if changed => new changes added to table; if unchanged => in some cases scd end_date extended (not extended if the prev_year.end_date + 1 != curr_year.current_year)
    past_curr_joined as (
        select
            coalesce(p.actor_id, c.actor_id) as actor_id,
            p.quality_class as prev_quality_class,
            c.quality_class as curr_quality_class,
            p.is_active as prev_is_active,
            c.is_active as curr_is_active,
            case
                when p.quality_class != c.quality_class
                or p.is_active != c.is_active then 1 -- case where either quality_class or is_active status changed
                when p.quality_class = c.quality_class
                and p.is_active = c.is_active then 0 -- case where neither quality_class nor is_active status changed
            end as changed,
            p.start_date as prev_start_date,
            p.end_date as prev_end_date,
            coalesce(p.current_year + 1, c.current_year) as current_year
        from
            prev_year p
            full outer join curr_year c on p.actor_id = c.actor_id
            and p.end_date + 1 = c.current_year
    ),
    scd_info as (
        select
            actor_id,
            current_year,
            case
            -- either wasn't in prev_year or wasn't in curr_year (but was in one of them!); coalesce values to keep values for whichever it was in (not null)
                when changed is null then array[
                    cast(
                        row(
                            coalesce(prev_quality_class, curr_quality_class),
                            coalesce(prev_is_active, curr_is_active),
                            coalesce(prev_start_date, current_year),
                            coalesce(prev_end_date, current_year)
                        ) as row(
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]
                -- neither quality_class nor in_active status changed => need to extend prev_end_date by 1 (same as current_year) as dimension didn't change
                when changed = 0 then array[
                    cast(
                        row(
                            prev_quality_class,
                            prev_is_active,
                            prev_start_date,
                            current_year
                        ) as row(
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]
                -- either quality_class or in_active status changed (or both!) => keep info from previous dimension change the same & add new row with new values from starting: current_year to ending: current_year 
                when changed = 1 then array[
                    cast(
                        row(
                            prev_quality_class,
                            prev_is_active,
                            prev_start_date,
                            prev_end_date
                        ) as row(
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    ),
                    cast(
                        row(
                            curr_quality_class,
                            curr_is_active,
                            current_year,
                            current_year
                        ) as row(
                            quality_class VARCHAR,
                            is_active BOOLEAN,
                            start_date INTEGER,
                            end_date INTEGER
                        )
                    )
                ]
            end as scd_arr
        from
            past_curr_joined
    )
select
    actor_id,
    scd_arr.quality_class,
    scd_arr.is_active,
    scd_arr.start_date,
    scd_arr.end_date,
    current_year
from
    scd_info
    cross join unnest (scd_arr) as scd_arr
    --comment for grader