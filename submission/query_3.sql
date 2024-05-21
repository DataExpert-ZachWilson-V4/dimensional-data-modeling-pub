
-- Create the table to track the historical data of actors
CREATE OR REPLACE TABLE raniasalzahrani.actors_history_scd (
    actor_id VARCHAR,  -- Unique identifier for each actor
    quality_class VARCHAR(10),  -- Quality classification of the actor based on average rating
    is_active BOOLEAN,  -- Indicates if the actor is currently active
    start_date DATE,  -- The start date for the period this record is valid
    end_date DATE  -- The end date for the period this record is valid (9999-12-31 for current records)
)
WITH (
    FORMAT = 'PARQUET'  -- Store the table in Parquet format for efficient querying and storage
)
