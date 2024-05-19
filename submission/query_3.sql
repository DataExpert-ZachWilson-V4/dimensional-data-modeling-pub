create
or replace table sanchit.actors_history_scd
( actor_id varchar,
    quality_class varchar,
    is_active boolean,
    start_date integer,
    end_date integer,
    current_year integer)
with
    ( format = 'parquet',
        partitioning = array ['current_year'])