-- Create table to store historical data for actors using Slowly Changing Dimensions (SCD) approach
CREATE TABLE rajkgupta091041107.actors_history_scd (
    -- Actor's ID
    actor_id VARCHAR,
    -- Quality class of the actor
    quality_class VARCHAR,
    -- Indicator for whether the actor is active
    is_active BOOLEAN, 
    -- Start date for the actor's status
    start_date DATE, 
    -- End date for the actor's status
    end_date DATE, 
    -- Date when the record was last modified
    modified_date DATE 
) WITH (
    -- Specify the data format as PARQUET
    format = 'PARQUET', 
    -- Partition the table by the modified date for efficient querying
    partitioning = ARRAY['modified_date'] 
)
