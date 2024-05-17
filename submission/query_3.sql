/*
Actors History SCD Table DDL (query_3)
Prompt: 
Write a DDL statement to create an actors_history_scd table that tracks the following fields for each actor in the actors table:
  * quality_class
  * is_active
  * start_date
  * end_date
Note that this table should be appropriately modeled as a Type 2 Slowly Changing Dimension Table (start_date and end_date).
*/

DROP TABLE IF EXISTS general_schema.actors_history_scd;

CREATE TABLE general_schema.actors_history_scd (
  -- 'actor_id': Unique identifier for each actor.
  actor_id VARCHAR,
  -- 'actor': Stores the actor's name.
  actor VARCHAR,
  -- 'quality_class': Categorical rating based on average rating in the most recent year.
  quality_class VARCHAR,
  -- 'is_active': Indicates if the actor is active, based on making films this year.
  is_active BOOLEAN,
  -- 'start_date': Marks the beginning of a particular state (quality_class/is_active).
  start_date INTEGER,
  -- 'end_date': Signifies the end of a particular state.
  end_date INTEGER,
  -- 'current_year': The year this record pertains to.
  current_year INTEGER
) WITH (
  format = 'PARQUET',
  partitioning = ARRAY ['current_year']
)
