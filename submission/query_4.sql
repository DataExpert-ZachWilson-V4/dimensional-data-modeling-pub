-- ### Actors History SCD Table Batch Backfill Query (query_4)
--
-- Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
-- Note: We are attempting a Type 2 'slowly changing dimension' pipeline intended to be idempotent
-- ie reconcile back filled data vs data generated in production

-- Key takeaway: Batch backfill is effectively a single load query
-- Note: may not always be feasible...imagine batching in one go a table of terabytes of data, this may not be practical or take too long
insert into shababali.actors_history_scd
with
    -- 'lagged' CTE: scd backfill of cumulated table data, including 'is_active' and 'quality_class' for each actor
    --  uses LAG function to access data from the previous row partitioned by actor_id, ordered by current_year
    lagged as (
        select
            actor,
            actor_id,
            quality_class,
            lag(quality_class, 1) over (partition by actor_id order by current_year) as quality_class_last_year,
            is_active,
            lag(is_active, 1) over (partition by actor_id order by current_year) as is_active_last_year,
            current_year
        from shababali.actors
        -- may consider a current_year up to which batch can be segmented
    ),
    -- tracking changes as a rolling sum (streak identifier),
    -- which allows us to group by actors and review a checkered history
    streaked as (
        select *,
        -- rolling streak identifier; sum increments every time there is a change
        -- is_active
        SUM(
            case
                when is_active <> is_active_last_year
                    then 1 else 0
            end
        ) over (
            partition by actor_id order by current_year
            ) as is_active_streak,
        -- quality_class
        SUM(
            case
                when quality_class <> quality_class_last_year
                    then 1 else 0
            end
        ) over (
            partition by actor_id order by current_year
            ) as quality_class_streak
        from lagged
    )
select
    actor,
    actor_id,
    MAX(quality_class) as quality_class,
    MAX(is_active) as is_active,
    MIN(current_year) as start_year,
    MAX(current_year) as end_year,
    1999 as current_year
from streaked
-- group by actor and the identified streaks to segment history accurately
group by actor, actor_id, is_active_streak, quality_class_streak
