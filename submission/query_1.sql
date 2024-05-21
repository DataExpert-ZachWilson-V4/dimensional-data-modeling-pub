--query_1: actors table DDL 
CREATE
OR REPLACE TABLE aayushi.actors (
    actor varchar       -- actor name
  , actor_id varchar    -- actor’s ID
  , films ARRAY(        -- Array of Struct 
    ROW(
        film varchar    -- film name
      , votes integer   -- number of votes
      , rating double   -- film rating
      , film_id varchar -- unique identifier for each film
    )
  )
  , quality_class varchar  -- avg rating categorization of movies
  , is_active Boolean      -- current active actor in film industry i.e. making films this year
  , current_year integer   -- this year representation for actor
)
WITH
  (
      FORMAT = 'PARQUET'
    , partitioning = ARRAY['current_year']
  )
