INSERT INTO emmaisemma.actors_history_scd
WITH lagged as(
    select 
        actor,
        quality_class,
        is_active,
        lag(is_active, 1) over(partition by actor order by current_year) as is_active_last_year,
        lag(quality_class,1) over(partition by actor order by current_year) as quality_class_last_year,
        current_year
    from emmaisemma.actors
    where current_year <= 2001 
),
streaked as(
    select *,
    sum(
        case when is_active <> is_active_last_year then 1
            when quality_class <> quality_class_last_year then 1
            else 0
        end
    )over(partition by actor order by current_year)as streak_identifier
    from lagged
)
select 
    actor,
    quality_class,
    max(is_active) as is_active,
    min(current_year) as start_date,
    max(current_year) as end_date,
    2001 as current_year
From streaked
group by actor, streak_identifier, is_active, quality_class