insert into phabrahao.actors_history_scd WITH last_year as (
        select *
        from phabrahao.actors_history_scd
        where current_year = 1920
    ),
    this_year AS (
        SELECT actor,
            actor_id,
            quality_class,
            is_active,
            current_year
        FROM phabrahao.actors
        WHERE current_year = 1921
    )
select COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actor_id, ty.actor_id) as actor_id,
    COALESCE(ly.quality_class, ty.quality_class) as quality_class,
    COALESCE(ly.is_active, ty.is_active) as is_active,
    COALESCE(ly.start_date, ty.current_year) as start_date,
    COALESCE(ty.current_year, ly.end_date) as end_date,
    COALESCE(ly.current_year + 1, ty.current_year) AS current_year
from last_year ly
    full outer join this_year ty on ly.actor_id = ty.actor_id
    -- if quality class didn't change, it will be on the same row. If it did, it will create another row
    and COALESCE(ly.quality_class, 'null') = COALESCE(ty.quality_class, 'null')
    and ly.end_date + 1 = ty.current_year
order by actor,
    start_date