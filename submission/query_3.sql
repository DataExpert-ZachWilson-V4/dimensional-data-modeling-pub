-- QUERY 3 ASSIGNMENT

-- Write a DDL statement to create an actors_history_scd table 
-- that tracks the following fields for each actor in the actors table:

-- quality_class
-- is_active
-- start_date
-- end_date
-- Note that this table should be appropriately modeled as a Type 2 
-- Slowly Changing Dimension Table (start_date and end_date).


CREATE TABLE IF NOT EXISTS vzucher.actors_history_scd (
    history_id VARCHAR,   -- Surrogate key, often treated logically as a primary key
    actor_id VARCHAR,     -- Natural key
    quality_class VARCHAR,  -- Quality classification of the actor
    is_active BOOLEAN,    -- Indicates if the actor is currently active
    start_date DATE,      -- Start date of the record's validity
    end_date DATE         -- End date of the record's validity
)

-- The following query is designed to populate a single year's worth of data in the actors_history_scd table.
-- It combines the previous year's data with the new incoming data from the actors table for the current year.
