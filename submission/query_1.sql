--Actors Table DDL (query_1)
--DESCRIBE bootcamp.actor_films : I used this query to find the datatype of each column in the master_table
create or replace table hariomnayani88482.actors(
      actor varchar,	
      actor_id varchar,	
      films ARRAY(ROW(
        film varchar,
        votes integer,
        rating double,
        film_id varchar
      )),
      quality_class varchar,
      is_active boolean,
      current_year integer      
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )
