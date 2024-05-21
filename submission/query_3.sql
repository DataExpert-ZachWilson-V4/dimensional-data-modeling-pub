-- Table for tracking changes to actors attribute year over year. This is a type 2 SCD table.
CREATE OR REPLACE TABLE raviks90.actors_history_scd (
    actor_id VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER, -- year when the dimensions(quality class & is_active) start
    end_date INTEGER, ---- year when the dimensions(quality class & is_active) end before changing either/both of them
    current_year INTEGER
)
with
    (
        FORMAT = 'PARQUET',
        partitioning = ARRAY['current_year']
    )
