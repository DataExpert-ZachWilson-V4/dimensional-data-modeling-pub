--Actors History SCD Table DDL (query_3)

CREATE TABLE  sanniepatron.actors_history_scd (
    actor varchar, -- The name of the actor.
    actor_id varchar, -- A unique identifier for the actor.
    quality_class varchar,  -- The classification of the actor's quality.
    is_active boolean, -- Indicates whether the actor is currently active.
    start_date integer, -- The start date of the record
    end_date integer, -- The end date of the record
    current_year INTEGER  -- The current year of the record (used for partitioning)
)
with (
    format = 'PARQUET', -- Specifies that the table format is PARQUET, which is efficient for storage and query performance.
    partitioning = ARRAY['current_year'] -- Partitions the table by `current_year` to improve query performance and manageability.
)