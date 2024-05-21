-- ### Cumulative Table Computation Query (query_2)
--
-- Write a query that populates the `actors` table one year at a time.
-- SCHEMA:
-- - `actor`: Actor name
-- - `actor_id`: Actor's ID
-- - `films`: An array of `struct` with the following fields:
--   - `film`: The name of the film.
--   - `votes`: The number of votes the film received.
--   - `rating`: The rating of the film.
--   - `film_id`: A unique identifier for each film.
--   - `year`: The year of film release
-- - `quality_class`: A categorical bucketing of the average rating of the movies for this actor in their most recent year:
--   - `star`: Average rating > 8.
--   - `good`: Average rating > 7 and ≤ 8.
--   - `average`: Average rating > 6 and ≤ 7.
--   - `bad`: Average rating ≤ 6.
-- - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
-- - `current_year`: The year this row represents for the actor

insert into shababali.actors
with
    -- all data from last year; intended to be compared and incrementally loaded w/ respect to a current year ie next year; see this year in next CTE
    -- expect last year for the first iteration to yield null values
    last_year as (
        select * from shababali.actors where current_year = 1995
    ),
    -- data for this year; intended to be compared and incrementally loaded for the desired current year
    this_year as (
        select
            actor,
            actor_id,
            -- aggregating actor films for this year
            ARRAY_AGG(row(film, votes, rating, film_id, year)) as films,
            AVG(rating) as avg_rating,
            year
        from
            bootcamp.actor_films
        where
            year = 1996
        group by
            actor,
            actor_id,
            year
    )
-- insert collection of concatenated(last year and this year) data w/ respect to the current year increment
select
    COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actor_id, ty.actor_id) as actor_id,
    -- concatenate films from last year and this year (given incremental insert into table)
    case
        when ty.films is NULL
            then ly.films
        when ty.films is not NULL and ly.films is NULL
            then ty.films
        when ty.films is not NULL and ly.films is not NULL
            then ty.films || ly.films
    end as films,
    -- assign quality class based on average rating (given incremental insert into table)
    case
        when ty.avg_rating > 8
            then 'star'
        when ty.avg_rating > 7 and ty.avg_rating <= 8
            then 'good'
        when ty.avg_rating > 6 and ty.avg_rating <= 7
            then 'average'
        else
            'bad'
    end as quality_class,
    ty.year is not NULL as is_active,  -- mark as active if and only if actor has film record for this year
    COALESCE(ty.year, ly.current_year + 1) as current_year  -- convenience; set current year of increment ie this year (for incremental insert into table)
from
    last_year as ly full outer join this_year as ty
    on ly.actor_id = ty.actor_id
