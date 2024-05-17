
CREATE OR REPLACE TABLE actors(
actor VARCHAR, -- 'actor': Stores the actor's name. Part of the actor_films dataset.
actor_ID VARCHAR, -- 'actor_id': Unique identifier for each actor, part of the primary key in actor_films dataset.
-- 'films': Array of ROWs for multiple films associated with each actor. Each row contains film details.
films ARRAY(
    ROW(
     film VARCHAR,  -- 'film': Name of the film, part of actor_films dataset.
     votes INTEGER, -- 'votes': Number of votes the film received, from actor_films dataset 
     rating DOUBLE, -- 'rating': Rating of the film, from actor_films dataset. 
     film_id VARCHAR, -- 'film_id': Unique identifier for each film, part of the primary key in actor_films dataset.
     year INTEGER --Release year of the film, part of actor_films dataset.
      )
  ),
--'quality clas': Categorical rating based on average rating in the most recent year. 
quality_class VARCHAR, 
--'is_active': Indicates if the actor is currently active, based on making films this year.
is_active BOOLEAN, 
--Represents the year this row is relevant to see the current year for actor. 
current_year INTEGER 
)
WITH
--partition the table on current year column and store the data in PARQUET FORMAT
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )

---test
