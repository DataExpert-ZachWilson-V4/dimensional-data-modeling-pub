INSERT INTO phabrahao.actors_history_scd WITH lagged as (
        select *,
            lag(quality_class) OVER (
                partition by actor_id
                order by current_year
            ) as quality_class_last_year
        from phabrahao.actors
    ),
    changed as (
        select *,
            CASE
                WHEN quality_class <> quality_class_last_year then 1
                else 0
            end as changed
        from lagged
    ),
    streak as (
        select *,
            SUM(changed) OVER(
                partition by actor_id
                order by current_year
            ) as streak
        from changed
    ),
    grouped as (
        select actor,
            actor_id,
            quality_class,
            max(is_active) as is_active,
            min(current_year) as start_date,
            max(current_year) as end_date
        from streak
        group by actor,
            actor_id,
            quality_class,
            streak
    ),
    max_current_year as (
        select max(current_year) as max_current_year
        from phabrahao.actors
    )
select g.*,
    max_current_year as current_year
from grouped g
    left join max_current_year mc on 1 = 1