-- Create actor_history_scd table that contains actor attributes and captures changes to those attributes

CREATE OR REPLACE TABLE ovoxo.actors_history_scd (
  actor VARCHAR,  -- actor name
  actor_id VARCHAR, -- actor identifier, I added this is this is a better identifier for an actor than name. Multiple actors can have same name but different ids
  quality_class VARCHAR, -- categorical rating of film linked to actor. A new record is generated for an actor if there is a change in their quality class
  is_active BOOLEAN, -- if actor is active in current_year. A new record is generated for an actor if there is a change in their active statis
  start_date INTEGER, -- start year of SCD record
  end_date INTEGER, -- end year of SCD record
  current_year INTEGER -- indicates the year the records pertains to. 
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )