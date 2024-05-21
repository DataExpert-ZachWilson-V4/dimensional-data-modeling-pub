-- Create a table to store historical data for actors using Slowly Changing Dimension (SCD) type 2
CREATE OR REPLACE TABLE saismail.actors_history_scd (
  actor_id VARCHAR,         -- Represents an Actor
  quality_class VARCHAR,    -- Represents the quality class of the actor (e.g., "star", "good", "average", "bad")
  is_active BOOLEAN,        -- Indicates whether the actor was active during the specified period
  start_date INTEGER,          -- Start date of the period when the actor had the specified quality class and activity status
  end_date INTEGER,            -- End date of the period when the actor had the specified quality class and activity status
  "current_date" INTEGER       -- Date when the record was added to the table (optional, if needed for auditing purposes)
)
-- Specify the format of the table data as Parquet
WITH (
  FORMAT = 'parquet',
  partitioning = ARRAY['current_date']
)
