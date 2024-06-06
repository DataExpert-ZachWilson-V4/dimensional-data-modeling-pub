-- Create a table to store historical data for actors using Slowly Changing Dimension (SCD) approach
CREATE TABLE bhakti.actors_history_scd (
    actor VARCHAR, -- Name of the actor
    actor_id VARCHAR, -- Unique identifier for the actor
    quality_class VARCHAR, -- Quality class of the actor
    is_active BOOLEAN, -- Flag indicating if the actor is currently active
    start_date INTEGER, -- Start date of the actor's record
    end_date INTEGER -- End date of the actor's record
)
WITH (
    format = 'PARQUET', -- Store the data in Parquet format for efficient storage and processing
    partitioning = ARRAY['start_date'] -- Partition the data based on the start date for better query performance
)
