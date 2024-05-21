CREATE
OR REPLACE TABLE grisreyesrios.actors (
    actor varchar       
  , actor_id varchar    
  , films ARRAY(        
    ROW(
        film varchar    
      , votes integer   
      , rating double   
      , film_id varchar 
    )
  )
  , quality_class varchar  
  , is_active Boolean      
  , current_year integer 
)
WITH
  (
      FORMAT = 'PARQUET'
    , partitioning = ARRAY['current_year']
  )
