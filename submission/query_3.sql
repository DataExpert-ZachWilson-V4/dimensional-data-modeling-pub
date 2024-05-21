-- Table to store actors historical information as SCD type 2
CREATE OR REPLACE TABLE srik1981.actors_history_scd (
    -- Name of the actor
    actor VARCHAR,
    -- Unique identification for each actor
    actor_id VARCHAR,
    -- Categorical rating based on average rating in the most recent year
    quality_class VARCHAR,
    -- Indicates if the actor is active based on films made in current year
    is_active BOOLEAN,
    -- Start year when the combination of (quality_class + is_active) are unique
    -- for the given actor
    start_date INTEGER,
    -- End year when the combination of (quality_class + is_active) are unique
    -- for the given actor
    end_date INTEGER,
    -- The year this row represents for the actor
    current_year INTEGER
) WITH (
    -- Storage format
    format = 'PARQUET',
    -- Partition data by current_year which helps in optimizing query
    partitioning = ARRAY ['current_year']
)