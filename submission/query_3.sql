-- Create a table named 'actors_history_scd' in the schema 'martinaandrulli' or replace it if already exists
CREATE OR REPLACE TABLE martinaandrulli.actors_history_scd ( 
  actor_id VARCHAR, -- Actor's ID
  quality_class VARCHAR, -- A categorical bucketing of the average rating of the movies for this actor in their most recent year
  is_active BOOLEAN, -- A BOOLEAN field that indicates whether an actor is currently active in the film industry
  start_date INTEGER, -- Year when the status (active/not active) and quality class described by the row has started.
  end_date INTEGER, -- Year when the status (active/not active) and quality class described by the row has ended.
  current_year INTEGER -- The year this row represents for the actor
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year'] -- Partitioning by the year to make easily discoverable the historical data
  )
  