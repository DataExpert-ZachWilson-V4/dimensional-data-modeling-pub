-- ### Actors History SCD Table Incremental Backfill Query (query_5)
--
-- Write an "incremental" query that can populate a single year's worth of the `actors_history_scd` table by combining the previous year's SCD data with the new incoming data from the `actors` table for this year.

insert into shababali.actors_history_scd
with
    last_year_scd as (
        select * from shababali.actors_history_scd where current_year = 1998
    ),
    this_year_scd as (
        select * from shababali.actors where current_year = 1999
    ),
    -- use two different columns to identify changes on quality_class and is_active
    -- merge last year's historical data with this year's data, assessing changes in 'is_active' and 'quality_class'
    combined as (
        select

        COALESCE(lys.actor, tys.actor) as actor,
        COALESCE(lys.actor_id, tys.actor_id) as actor_id,

        COALESCE(lys.start_date, tys.current_year) as start_date,
        COALESCE(lys.end_date, tys.current_year) as end_date,

        lys.quality_class as quality_class_last_year,
        tys.quality_class as quality_class_this_year,
        case
            when lys.quality_class <> tys.quality_class
                then TRUE
            when lys.quality_class = tys.quality_class
                then FALSE
        end as quality_class_did_change,

        lys.is_active as is_active_last_year,
        tys.is_active as is_active_this_year,
        case
            when lys.is_active <> tys.is_active
                then TRUE
            when lys.is_active = tys.is_active
                then FALSE
        end as is_active_did_change,

        1999 as current_year

        from
            last_year_scd as lys full outer join this_year_scd tys
            on lys.actor_id = tys.actor_id and lys.end_date + 1 = tys.current_year
    ),
    changes as (
        select
            actor,
            actor_id,
            current_year,
            -- construct an array of historical rows, based on whether there was a change or not
        case
            -- no changes
            when not quality_class_did_change and not is_active_did_change
                then
                    array[
                        CAST(row(is_active_last_year, quality_class_last_year, start_date, end_date)
                        as row(is_active boolean, quality_class varchar, start_date integer, end_date integer)
                        )
                        ]
            -- quality class changed
            when quality_class_did_change and not is_active_did_change
                then
                    array[
                        CAST(row(is_active_last_year, quality_class_last_year, start_date, end_date)
                        as row(is_active boolean, quality_class varchar, start_date integer, end_date integer)
                        ),
                        CAST(row(is_active_last_year, quality_class_this_year, current_year, current_year)
                        as row(is_active boolean, quality_class varchar, start_date integer, end_date integer)
                        )
                        ]
            -- is active changed
            when is_active_did_change and not quality_class_did_change
                then
                    array[
                        CAST(row(is_active_last_year, quality_class_last_year, start_date, end_date)
                        as row(is_active boolean, quality_class varchar, start_date integer, end_date integer)
                        ),
                        CAST(row(is_active_this_year, quality_class_last_year, current_year, current_year)
                        as row(is_active boolean, quality_class varchar, start_date integer, end_date integer)
                        )
                        ]
            -- both changed
            when is_active_did_change and quality_class_did_change
                then
                    array[
                        CAST(row(is_active_last_year, quality_class_last_year, start_date, end_date)
                        as row(is_active boolean, quality_class varchar, start_date integer, end_date integer)
                        ),
                        CAST(row(is_active_this_year, quality_class_this_year, current_year, current_year)
                            as row(is_active boolean, quality_class varchar, start_date integer, end_date integer)
                        )
                        ]
            -- new record ie changes are null
            when is_active_did_change is NULL and quality_class_did_change is NULL
                then
                    array[
                        CAST(
                            row(
                                COALESCE(is_active_last_year, is_active_this_year),
                                COALESCE(quality_class_last_year, quality_class_this_year),
                                start_date, end_date
                                ) as row (is_active boolean, quality_class varchar, start_date integer, end_date integer)
                            )
                        ]
        end as change_array
        from
            combined
    )
    select
        actor,
        actor_id,
        arr.quality_class,
        arr.is_active,
        arr.start_date,
        arr.end_date,
        current_year
    from changes cross join UNNEST (change_array) AS arr
