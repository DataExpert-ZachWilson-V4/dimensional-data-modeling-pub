create or replace table fayiztk.actors (
    actor varchar,
    actor_id varchar,
    films ARRAY (
        ROW (
            film varchar,
            votes integer,
            rating double,
            film_id varchar
        )
    ),
    quality_class varchar,
    is_active boolean,
    current_year integer
)
with  
(  
format='parquet',  
partitioning=ARRAY['current_year']  
) 