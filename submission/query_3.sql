CREATE
OR REPLACE TABLE barrocaeric.actors_history_scd (
    -- Adding actor_id and actor as key fields that are used as anchors for the scd ones
    actor_id VARCHAR,
    actor VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER
)
WITH
    (
        format = 'PARQUET',
        partitioning = ARRAY['current_year']
    )