-- ### Actors Table DDL (query_1)
--
-- Write a DDL query to create an `actors` table with the following fields:
--
-- - `actor`: Actor name
-- - `actor_id`: Actor's ID
-- - `films`: An array of `struct` with the following fields:
--   - `film`: The name of the film.
--   - `votes`: The number of votes the film received.
--   - `rating`: The rating of the film.
--   - `film_id`: A unique identifier for each film.
--   - `year`: year integer.
-- - `quality_class`: A categorical bucketing of the average rating of the movies for this actor in their most recent year:
--   - `star`: Average rating > 8.
--   - `good`: Average rating > 7 and ≤ 8.
--   - `average`: Average rating > 6 and ≤ 7.
--   - `bad`: Average rating ≤ 6.
-- - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
-- - `current_year`: The year this row represents for the actor

create or replace table shababali.actors (
    actor varchar,
    actor_id varchar,
    -- films array of struct ie array(row())
    -- intended to be array of rows for each film up to current_year
    films array(
        row(
            film varchar, votes integer, rating double, film_id varchar, year integer
        )
    ),
    quality_class varchar,
    is_active boolean,
    current_year integer
) with (
    format = 'PARQUET',
    partitioning = array['current_year']
)
