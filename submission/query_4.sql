
insert into fayiztk.actors_history_scd
with
    last_year_values as (
        select
            actor,
            quality_class,
            is_active,
            current_year,
            lag (quality_class) over (
                partition by
                    actor
                order by
                    current_year
            ) as quality_class_last_year,
            lag (is_active) over (
                partition by
                    actor
                order by
                    current_year
            ) as is_active_last_year
        from
            fayiztk.actors
    ),
    streaks as (
        select
            *,
            sum(
                case
                    when quality_class != quality_class_last_year
                    or is_active != is_active_last_year then 1
                    else 0
                end
            ) over (
                partition by
                    actor
                order by
                    current_year
            ) as streak
        from
            last_year_values
    )
    
select
    actor,
    max(quality_class) as quality_class,
    max(is_active) as is_active ,
    min(current_year) start_date,
    max(current_year) end_date,
    2021 as current_year
from
    streaks
group by
    actor,
    streak
order by
    actor