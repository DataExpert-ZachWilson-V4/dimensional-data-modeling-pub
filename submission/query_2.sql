insert into
    fayiztk.actors
with
    ty as (
        select
            actor,
            actor_id,
            year,
            array_agg(Row (film, votes, rating, film_id, year)) as films,
            avg(rating) as avg_rating
        from
            bootcamp.actor_films
        where
            year = 2000
        group by
            actor,
            actor_id,
            year
    ),
    ly as (
        select
            actor,
            actor_id,
            films,
            quality_class,
            is_active,
            current_year
        from
            fayiztk.actors
        where
            current_year = 1999
    )
select
    COALESCE(ty.actor, ly.actor) as actor,
    COALESCE(ty.actor_id, ly.actor_id) as actor,
    case
        when ty.actor is null then ly.films
        when ty.actor is not null
        and ly.actor is null then ty.films
        when ty.actor is not null
        and ly.actor is not null then ty.films || ly.films
    end as films,
    case
        when ty.actor is null then ly.quality_class
        when ty.actor is not null then case
            when ty.avg_rating > 8 then 'star'
            when ty.avg_rating > 7
            and ty.avg_rating <= 8 then 'good'
            when ty.avg_rating > 6
            and ty.avg_rating <= 7 then 'average'
            when ty.avg_rating <= 6 then 'bad'
        end
    end as quality_class,
    ty.actor is not null as is_active,
    COALESCE(ty.year, ly.current_year + 1) as current_year
from
    ty
    full outer join ly on ty.actor_id = ly.actor_id