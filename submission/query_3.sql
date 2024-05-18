-- creates table for tracking actor's changes in quality_class & is_active as a slowly changing dimension
create
or replace table sarneski44638.actors_history_scd (
    actor_id VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER, -- year where specified quality_class & is_active combo starts 
    end_date INTEGER, -- year where specified quality_class & is_active combo ends 
    current_year INTEGER
)
with
    (
        FORMAT = 'PARQUET',
        partitioning = ARRAY['current_year']
    )