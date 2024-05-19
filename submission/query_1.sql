--Actors Table DDL (query_1)
CREATE TABLE sanniepatron.actors 
(
    actor VARCHAR,  --  Stores the actor's name, , stored as a variable-length string
    actor_id VARCHAR, -- Column for actor IDs, stored as a variable-length string
    films ARRAY(ROW( -- Column for storing information about films the actor has participated in.This is an array of rows, with each row representing a film.
	    year INTEGER, -- Year the film was released, stored as an integer
        film VARCHAR, -- Title of the film, stored as a variable-length string
        votes INTEGER, -- Number of votes the film has received, stored as an integer
        rating DOUBLE, -- Rating of the film, stored as a double (floating-point number)
        film_id VARCHAR)),  -- ID of the film, stored as a variable-length string
    quality_class VARCHAR,  -- Column to classify the quality of the actor, stored as a variable-length string
    is_active BOOLEAN, -- Column to indicate if the actor is currently active, stored as a boolean
    current_year INTEGER -- Column to store the current year, stored as an integer
)
WITH (
    format = 'PARQUET', -- Specify the format for storing the table's data as 'PARQUET'
    partitioning = ARRAY['current_year'] -- Partition the table's data based on the 'current_year' column
)