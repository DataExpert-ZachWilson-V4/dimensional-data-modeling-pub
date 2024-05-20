-- QUERY 1 ASSIGNMENT //

-- Actors Table DDL (query_1)
-- Write a DDL query to create an actors table with the following fields:
-- actor_id (bigint), actor (string), films (array of struct with fields film_id (bigint), film (string), votes (int), rating (double)), quality_class (string), is_active (boolean), current_year (int)
-- I have create my schema running CREATE SCHEMA vzucher in the dataexpert.io platform
-- here i am creating a table called actors in the vzucher schema

CREATE TABLE IF NOT EXISTS vzucher.actors (

    -- as Zach's always doing in the classes defining ids as bigint as a good practice
  actor_id VARCHAR, 
   -- actor name is a string so i am defining it as VARCHAR
  actor VARCHAR,

  -- this is interesting because trino's syntax to create array of struct can vary a lot from regular sql, 
  -- so here i am defining the films column as an array of struct with the fields film_id, film, votes and rating

  films ARRAY(ROW(film_id VARCHAR, film VARCHAR, votes INTEGER, rating DOUBLE)),

  -- quality_class is a bucket that can have the values of star, good, average or bad so i am defining it as VARCHAR
  quality_class VARCHAR,

  -- is active is a boolean field so i am defining it as BOOLEAN, 1s and 0s
  is_active BOOLEAN,

  -- current year is an year date like 1999 or 2021 so i am defining it as INTEGER
  current_year INTEGER
)
