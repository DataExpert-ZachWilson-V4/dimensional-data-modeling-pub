-- Create the actors table in the videet schema
CREATE 
OR REPLACE TABLE videet.actors (
    actor VARCHAR,           -- Name of the actor, providing a human-readable identifier
    actor_id VARCHAR,        -- Unique identifier for the actor, used for database references and joins
    films ARRAY(ROW(         -- Array of structs, with each struct representing details of a film
        film VARCHAR,        -- Name of the film, allowing for easy identification of the film within the array
        votes BIGINT,        -- Number of votes the film received, indicating popularity or viewer engagement
        rating DOUBLE,       -- Film rating on a scale typically from 1 to 10, used for quality assessment
        film_id VARCHAR,      -- Unique identifier for each film, necessary for database operations and detailed tracking
        year INTEGER
    )),
    quality_class VARCHAR,   -- Categorical quality based on average film rating: 'star', 'good', 'average', 'bad'
    is_active BOOLEAN,       -- Flag to indicate if the actor is currently active in the film industry, important for current status tracking
    current_year INT         -- Year to which the data pertains, useful for time-sensitive analyses and ensuring data relevancy
)
WITH
  (
    FORMAT = 'PARQUET',      -- Specifies the storage format of the table, chosen as PARQUET for its efficiency in large data scenarios
    partitioning =           -- Defines how the data is partitioned in the storage, optimizing performance for queries
    ARRAY['current_year']    -- Data is partitioned by the current_year field, beneficial for queries filtered by specific years
  )