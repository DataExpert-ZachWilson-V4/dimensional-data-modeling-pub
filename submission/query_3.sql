-- Create table to track historical changes for each actor
CREATE TABLE alissabdeltoro.actors_history_scd (
    actor_id VARCHAR,  -- Unique identifier for the actor
    actor_name VARCHAR,  -- Name of the actor
    quality_class VARCHAR,  -- Categorical bucketing of the average rating of the movies
    is_active BOOLEAN,  -- Indicates whether an actor is currently active
    start_date DATE,  -- Start date of the historical record
    end_date DATE,  -- End date of the historical record
    current_year INTEGER  -- Year this row represents for the actor
)
WITH (
    FORMAT = 'PARQUET',  -- Data format
    partitioning = ARRAY['current_year']  -- Partitioned by current_year
)
