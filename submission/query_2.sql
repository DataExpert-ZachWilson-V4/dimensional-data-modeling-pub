-- min year in `actor_films` table 1914; max year in table 2021
-- This query is used to create a cummulative table for actors' data incrementally
-- vary line 12 (current_year) from 1913 to 2020 and line 29 (year) from 1914 to 2021 to fully populate cummulative table actors with available data from actor_films
insert into
    sarneski44638.actors
with
    prev_year as (
        select
            *
        from
            sarneski44638.actors
        where
            current_year = 2020
    ),
    curr_year as (
        select
            actor,
            actor_id,
            -- films aggregate of rows with each row containing more detailed info for each film released in the specified year
            ARRAY_AGG(row(film, year, votes, rating, film_id)) as films,
            -- determine the actors' quality class based on avg rating for films from specified year
            case
                when avg(rating) > 8 then 'star'
                when avg(rating) > 7 then 'good'
                when avg(rating) > 6 then 'average'
                when avg(rating) <= 6 then 'bad'
            end as quality_class,
            MAX(year) as current_year
        from
            bootcamp.actor_films
        where
            year = 2021 -- will be current_year + 1 where current_year from prev_year CTE
        group by
            actor,
            actor_id
    )
select
    coalesce(c.actor, p.actor) as actor,
    coalesce(c.actor_id, p.actor_id) as actor_id,
    -- combine films for actor in array
    case
        when p.films is null then c.films -- case when new actor & not yet in cummulative table
        when c.films is null then p.films -- case when actor didn't have films released in curr_year/they weren't active in curr_year, but did previously
        else c.films || p.films -- case when actor already in cummulative table & actor active in curr_year => concat new films with old films array
    end as films,
    -- if has films from current year then use current quality class otherwise use quality class from most recent past year
    coalesce(c.quality_class, p.quality_class) as quality_class,
    -- if actor has records in current year then considered active
    c.current_year is not null as is_active,
    coalesce(c.current_year, p.current_year + 1) as current_year
from
    prev_year p
    full outer join curr_year c on p.actor_id = c.actor_id
    --comment for grader