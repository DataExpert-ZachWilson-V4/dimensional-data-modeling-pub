create
or replace table sanchit.actors
(
    actor_id      varchar,
    actor         varchar,
    quality_class varchar,
    is_active     boolean,
    current_year  integer,
    films         array ( row (
        year integer,
        film_id varchar,
        film varchar,
        votes integer,
        rating double
        ))
)
    with
        ( format = 'parquet',
        partitioning = array ['current_year'])
