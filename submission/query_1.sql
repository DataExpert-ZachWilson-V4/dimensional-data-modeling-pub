-- A DDL query to create an `actors` table with the following fields:
-- Fields that wouldn't be empty marked as NON NULL i.e. an actor row only exists if there is an ID for an actor
CREATE OR REPLACE TABLE siawayforward.actors (
    -- based on actor_films dataset, id's are not cumulative numbers
    actor_id VARCHAR NOT NULL,
    actor VARCHAR NOT NULL,
    -- a record of the actor's films
    films ARRAY(ROW(
        -- based on actor_films dataset, id's are not cumulative numbers
        film_id VARCHAR,
        film VARCHAR,
        year INTEGER,
        votes INTEGER,
        rating DOUBLE
    )),
    -- a record of actor's movie ratings category
    quality_class VARCHAR,
    is_active BOOLEAN NOT NULL,
    current_year INTEGER NOT NULL
)
-- we want our table to be partitioned and store column optimal read data
WITH (
  format='PARQUET',
  partitioning= ARRAY['current_year']
)
