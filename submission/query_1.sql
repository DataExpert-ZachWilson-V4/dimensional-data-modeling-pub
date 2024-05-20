-- Create the actors table in the jlcharbneau schema
-- Including the actor_id, actor (name), an array/struct of the films the actor has been in,
-- a quality_class field (for store analysis), is active, and the current year
CREATE OR REPLACE TABLE jlcharbneau.actors (
     -- 'actor': Stores the actor's name.
     actor VARCHAR,
     -- 'actor_id': Unique identifier for each actor.
     actor_id VARCHAR,
     -- 'films': Array of ROWs for multiple films associated with each actor. Each row contains film details.
     films ARRAY(ROW(
         -- 'film': Name of the film.
         film VARCHAR,
         -- 'votes': Number of votes the film received.
         votes INTEGER,
         -- 'rating': Rating of the film.
         rating DOUBLE,
         -- 'film_id': Unique identifier for each film.
         film_id VARCHAR,
         -- 'year': Release year of the film.
         year INTEGER
         )),
     -- 'quality_class': Categorical rating based on average rating in the most recent year.
     quality_class VARCHAR,
     -- 'is_active': Indicates if the actor is currently active.
     is_active BOOLEAN,
     -- 'current_year': Represents the year this row is relevant for the actor.
     current_year INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)