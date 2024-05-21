-- HW1 query_1
/* hdamerla 
creating a table ased on the requirements.
Even though year is not mentioned in the query1, including it basedon the feedback from LLM*/


--creating table statement and columns based on the requirements provided 
CREATE TABLE actors ( 
    actor VARCHAR,
    actor_id VARCHAR,
    films ARRAY(ROW(  -- creation of films array
      film VARCHAR,
      votes INTEGER,
      rating DOUBLE,
      film_id VARCHAR,
      year INTEGER
      )),
    quality_class VARCHAR,
    is_active BOOLEAN,
    current_year INTEGER
)
WITH ( --format and partition specification
  format = 'PARQUET',
  partitioning = ARRAY['current_year']  
)
