-- Create a new table in the datademonslayer schema called 'actors_history_scd'
CREATE OR REPLACE TABLE datademonslayer.actors_history_scd (
    -- Column for storing the actor's name
    actor VARCHAR,

	-- Unique identifier for the actor, primary key
	actor_id VARCHAR, 

    -- Column for storing the quality classification of the actor
    quality_class VARCHAR,

    -- Column to indicate if the actor is currently active
    is_active BOOLEAN,

    -- Column for storing the start date of the actor's record (as an integer, possibly a date in YYYY-MM-DD format)
    start_date DATE,

    -- Column for storing the end date of the actor's record (as an integer, possibly a date in YYYY-MM-DD format)
    end_date DATE,

    -- Column for storing the current year (used for partitioning)
    current_year INTEGER
)
-- Specify the storage format for the table
WITH (
    format = 'PARQUET',

    -- Specify the column to partition the table by
    partitioning = ARRAY['current_year']
)