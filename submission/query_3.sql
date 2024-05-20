-- Create the actors_history_scd table in the videet schema
CREATE 
OR REPLACE TABLE videet.actors_history_scd (
    actor_id VARCHAR,        -- Unique identifier for the actor, referenced from videet.actors table
    quality_class VARCHAR,   -- Categorical quality based on average film rating: 'star', 'good', 'average', 'bad'
    is_active BOOLEAN,       -- Flag to indicate if the actor is currently active in the film industry
    start_date DATE,         -- Start date for the validity of the recorded data, typically when the record first applies
    end_date DATE,            -- End date for the validity of the recorded data; NULL indicates the record is currently valid
    current_year INTEGER     -- The year this record pertains to. Useful for partitioning and analyzing data by year.
)
WITH
  (
    FORMAT = 'PARQUET',     -- Specifies the storage format of the table, here set to PARQUET for efficient columnar storage
    partitioning =          -- Defines partitioning strategy for the table
    ARRAY['start_date']       -- Partitions the data by the actor_id to optimize query performance and data organization
  )
